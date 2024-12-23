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

mod confluent_resolver;
mod glue_resolver;
mod parser;
mod pulsar_resolver;

pub use confluent_resolver::ConfluentSchemaCache;
pub use glue_resolver::{GlueSchemaCache, GlueSchemaCacheImpl};
pub use parser::{AvroAccessBuilder, AvroParserConfig};
pub use pulsar_resolver::PulsarSchemaCache;
