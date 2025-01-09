// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{internal_table_name_to_parts, Field, Schema, StreamJobStatus};
use risingwave_common::types::{DataType, ScalarImpl};

use super::{ApplyResult, BoxedRule, FallibleRule};
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::expr::{Expr, ExprImpl, InputRef, Literal, TableFunctionType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalProject, LogicalScan, LogicalTableFunction, LogicalUnion, LogicalValues,
};
use crate::optimizer::PlanRef;
use crate::utils::GroupBy;
use crate::TableCatalog;

/// Transform a special `TableFunction` (with `FILE_SCAN` table function type) into a `LogicalFileScan`
pub struct TableFunctionToInternalBackfillProgressRule {}
impl FallibleRule for TableFunctionToInternalBackfillProgressRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function.function_type
            != TableFunctionType::InternalBackfillProgress
        {
            return ApplyResult::NotApplicable;
        }

        let fields = vec![
            Field::new("backfill_state_table_id", DataType::Int32),
            Field::new("row_count", DataType::Int64),
        ];

        let reader = plan.ctx().session_ctx().env().catalog_reader().read_guard();
        // TODO(kwannoel): Make sure it reads from source, snapshot backfill tables as well.
        let backfilling_tables = get_backfilling_tables(reader);

        // No backfill in progress, just return empty values.
        if backfilling_tables.is_empty() {
            let plan = LogicalValues::new(vec![], Schema::new(fields), plan.ctx().clone());
            return ApplyResult::Ok(plan.into());
        }

        let mut counts = Vec::with_capacity(backfilling_tables.len());
        for table in backfilling_tables {
            let table_id = table.id.table_id as i32;
            let Some(row_count_column_index) =
                table.columns.iter().position(|c| c.name() == "row_count")
            else {
                return ApplyResult::Err(
                    anyhow!("`row_count` column not found in snapshot backfill table").into(),
                );
            };
            let scan = LogicalScan::create(
                table.name.clone(),
                table,
                vec![],
                plan.ctx(),
                None,
                Default::default(),
            );
            let project = LogicalProject::new(
                scan.into(),
                vec![ExprImpl::InputRef(Box::new(InputRef {
                    index: row_count_column_index,
                    data_type: DataType::Int64,
                }))],
            );
            let select_exprs = vec![ExprImpl::Literal(Box::new(Literal::new(
                Some(ScalarImpl::Int32(table_id)),
                DataType::Int32,
            )))];
            let (count, _rewritten_select_exprs, _) =
                LogicalAgg::create(select_exprs, GroupBy::empty(), None, project.into())?;
            counts.push(count);
        }
        ApplyResult::Ok(LogicalUnion::new(true, counts).into())
    }
}

fn get_backfilling_tables(reader: CatalogReadGuard) -> Vec<Arc<TableCatalog>> {
    reader
        .iter_tables()
        .filter(|table| {
            let name = &table.name;
            match internal_table_name_to_parts(name) {
                None => false,
                Some((_job_name, _fragment_id, table_type, _table_id)) => {
                    let is_backfill = table_type == "snapshot_backfill";
                    let is_creating = table.stream_job_status == StreamJobStatus::Creating;
                    is_backfill && is_creating
                }
            }
        })
        .cloned()
        .collect_vec()
}

impl TableFunctionToInternalBackfillProgressRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToInternalBackfillProgressRule {})
    }
}
