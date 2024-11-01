// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::ops::{Bound, RangeBounds};
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::value_encoding::deserialize_datum;
use risingwave_pb::batch_plan::{scan_range, PbScanRange};
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::StateStore;

use crate::error::{BatchError, Result};
use crate::executor::{BoxedDataChunkStream, Executor};

pub type BoxedDataChunkListStream = BoxStream<'static, Result<Vec<DataChunk>>>;

/// Read at least `rows` rows.
#[try_stream(boxed, ok = Vec<DataChunk>, error = BatchError)]
pub async fn batch_read(mut stream: BoxedDataChunkStream, rows: usize) {
    let mut cnt = 0;
    let mut chunk_list = vec![];
    while let Some(build_chunk) = stream.next().await {
        let build_chunk = build_chunk?;
        cnt += build_chunk.cardinality();
        chunk_list.push(build_chunk);
        if cnt < rows {
            continue;
        } else {
            yield chunk_list;
            cnt = 0;
            chunk_list = vec![];
        }
    }
    if !chunk_list.is_empty() {
        yield chunk_list;
    }
}

pub struct BufferChunkExecutor {
    schema: Schema,
    chunk_list: Vec<DataChunk>,
}

impl Executor for BufferChunkExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "BufferChunkExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl BufferChunkExecutor {
    pub fn new(schema: Schema, chunk_list: Vec<DataChunk>) -> Self {
        Self { schema, chunk_list }
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self) {
        for chunk in self.chunk_list {
            yield chunk
        }
    }
}

pub struct DummyExecutor {
    pub schema: Schema,
}

impl Executor for DummyExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "dummy"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        DummyExecutor::do_nothing()
    }
}

impl DummyExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_nothing() {}
}

pub struct WrapStreamExecutor {
    schema: Schema,
    stream: BoxedDataChunkStream,
}

impl WrapStreamExecutor {
    pub fn new(schema: Schema, stream: BoxedDataChunkStream) -> Self {
        Self { schema, stream }
    }
}

impl Executor for WrapStreamExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        "WrapStreamExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.stream
    }
}

/// Range for batch scan.
pub struct ScanRange {
    /// The prefix of the primary key.
    pub pk_prefix: OwnedRow,

    /// The range bounds of the next column.
    pub next_col_bounds: (Bound<Datum>, Bound<Datum>),
}

impl ScanRange {
    /// Create a scan range from the prost representation.
    pub fn new(
        scan_range: PbScanRange,
        mut pk_types: impl Iterator<Item = DataType>,
    ) -> Result<Self> {
        let pk_prefix = OwnedRow::new(
            scan_range
                .eq_conds
                .iter()
                .map(|v| {
                    let ty = pk_types.next().unwrap();
                    deserialize_datum(v.as_slice(), &ty)
                })
                .try_collect()?,
        );
        if scan_range.lower_bound.is_none() && scan_range.upper_bound.is_none() {
            return Ok(Self {
                pk_prefix,
                ..Self::full()
            });
        }

        let bound_ty = pk_types.next().unwrap();
        let build_bound = |bound: &scan_range::Bound| -> Bound<Datum> {
            let datum = deserialize_datum(bound.value.as_slice(), &bound_ty).unwrap();
            if bound.inclusive {
                Bound::Included(datum)
            } else {
                Bound::Excluded(datum)
            }
        };

        let next_col_bounds: (Bound<Datum>, Bound<Datum>) = match (
            scan_range.lower_bound.as_ref(),
            scan_range.upper_bound.as_ref(),
        ) {
            (Some(lb), Some(ub)) => (build_bound(lb), build_bound(ub)),
            (None, Some(ub)) => (Bound::Unbounded, build_bound(ub)),
            (Some(lb), None) => (build_bound(lb), Bound::Unbounded),
            (None, None) => unreachable!(),
        };

        Ok(Self {
            pk_prefix,
            next_col_bounds,
        })
    }

    /// Create a scan range for full table scan.
    pub fn full() -> Self {
        Self {
            pk_prefix: OwnedRow::default(),
            next_col_bounds: (Bound::Unbounded, Bound::Unbounded),
        }
    }

    pub fn convert_to_range_bounds<S: StateStore>(
        self,
        table: Arc<StorageTable<S>>,
    ) -> impl RangeBounds<OwnedRow> {
        let ScanRange {
            pk_prefix,
            next_col_bounds,
        } = self;

        let order_type = table.pk_serializer().get_order_types()[pk_prefix.len()];
        let (start_bound, end_bound) = if order_type.is_ascending() {
            (next_col_bounds.0, next_col_bounds.1)
        } else {
            (next_col_bounds.1, next_col_bounds.0)
        };

        let start_bound_is_bounded = !matches!(start_bound, Bound::Unbounded);
        let end_bound_is_bounded = !matches!(end_bound, Bound::Unbounded);

        (
            match start_bound {
                Bound::Unbounded => {
                    if end_bound_is_bounded && order_type.nulls_are_first() {
                        // `NULL`s are at the start bound side, we should exclude them to meet SQL semantics.
                        Bound::Excluded(OwnedRow::new(vec![None]))
                    } else {
                        // Both start and end are unbounded, so we need to select all rows.
                        Bound::Unbounded
                    }
                }
                Bound::Included(x) => Bound::Included(OwnedRow::new(vec![x])),
                Bound::Excluded(x) => Bound::Excluded(OwnedRow::new(vec![x])),
            },
            match end_bound {
                Bound::Unbounded => {
                    if start_bound_is_bounded && order_type.nulls_are_last() {
                        // `NULL`s are at the end bound side, we should exclude them to meet SQL semantics.
                        Bound::Excluded(OwnedRow::new(vec![None]))
                    } else {
                        // Both start and end are unbounded, so we need to select all rows.
                        Bound::Unbounded
                    }
                }
                Bound::Included(x) => Bound::Included(OwnedRow::new(vec![x])),
                Bound::Excluded(x) => Bound::Excluded(OwnedRow::new(vec![x])),
            },
        )
    }
}
