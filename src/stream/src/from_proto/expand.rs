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

use risingwave_pb::stream_plan::ExpandNode;

use super::*;
use crate::executor::ExpandExecutor;

pub struct ExpandExecutorBuilder;

impl ExecutorBuilder for ExpandExecutorBuilder {
    type Node = ExpandNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let column_subsets = node
            .column_subsets
            .iter()
            .map(|subset| {
                subset
                    .column_indices
                    .iter()
                    .map(|idx| *idx as usize)
                    .collect_vec()
            })
            .collect_vec();
        Ok((params.info, ExpandExecutor::new(input, column_subsets)).into())
    }
}
