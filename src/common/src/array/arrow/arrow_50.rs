// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[allow(clippy::duplicate_mod)]
#[path = "./arrow_impl.rs"]
mod arrow_impl;
type ArrowIntervalType = i128;
pub use arrow_impl::{FromArrow, ToArrow};
pub use {
    arrow_50_array as arrow_array, arrow_50_buffer as arrow_buffer, arrow_50_cast as arrow_cast,
    arrow_50_schema as arrow_schema,
};
