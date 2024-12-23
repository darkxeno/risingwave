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

use std::collections::BTreeMap;

use risingwave_pb::catalog::PbSchemaRegistryNameStrategy;

use super::schema_registry::{
    get_subject_by_strategy, handle_sr_list, name_strategy_from_str, Client, Subject,
};
use super::{
    invalid_option_error, InvalidOptionError, SchemaFetchError, AWS_GLUE_SCHEMA_ARN_KEY,
    KEY_MESSAGE_NAME_KEY, MESSAGE_NAME_KEY, NAME_STRATEGY_KEY, SCHEMA_REGISTRY_KEY,
};
use crate::connector_common::AwsAuthProps;

pub enum SchemaLoader {
    Confluent {
        client: Client,
        name_strategy: PbSchemaRegistryNameStrategy,
        topic: String,
        key_record_name: Option<String>,
        val_record_name: Option<String>,
    },
    Glue {
        client: aws_sdk_glue::Client,
        schema_arn: String,
        mock: Option<() /* MockGlueSchemaCache */>,
    },
}

pub enum SchemaVersion {
    Confluent(i32),
    Glue(uuid::Uuid),
}

impl SchemaLoader {
    pub fn from_format_options(
        topic: &str,
        format_options: &BTreeMap<String, String>,
    ) -> Result<Self, InvalidOptionError> {
        if let Some(schema_arn) = format_options.get(AWS_GLUE_SCHEMA_ARN_KEY) {
            let aws_auth_props = serde_json::from_value::<AwsAuthProps>(
                serde_json::to_value(format_options).unwrap(),
            )
            .map_err(|_e| invalid_option_error!(""))?;
            let mock_config = format_options.get("aws.glue.mock_config").cloned();
            use futures::FutureExt as _;
            let client = aws_sdk_glue::Client::new(
                &aws_auth_props
                    .build_config()
                    .now_or_never()
                    .unwrap()
                    .unwrap(),
            );
            Ok(Self::Glue {
                client,
                schema_arn: schema_arn.clone(),
                mock: Some(()),
            })
        } else {
            let schema_location = format_options
                .get(SCHEMA_REGISTRY_KEY)
                .ok_or_else(|| invalid_option_error!("{SCHEMA_REGISTRY_KEY} required"))?;
            let client_config = format_options.into();
            let urls = handle_sr_list(schema_location)?;
            let client = Client::new(urls, &client_config)?;

            let name_strategy = format_options
                .get(NAME_STRATEGY_KEY)
                .map(|s| {
                    name_strategy_from_str(s)
                        .ok_or_else(|| invalid_option_error!("unrecognized strategy {s}"))
                })
                .transpose()?
                .unwrap_or_default();
            let key_record_name = format_options.get(KEY_MESSAGE_NAME_KEY).cloned();
            let val_record_name = format_options.get(MESSAGE_NAME_KEY).cloned();

            Ok(Self::Confluent {
                client,
                name_strategy,
                topic: topic.into(),
                key_record_name,
                val_record_name,
            })
        }
    }

    async fn load_schema<Out: LoadedSchema, const IS_KEY: bool>(
        &self,
    ) -> Result<(SchemaVersion, Out), SchemaFetchError> {
        match self {
            Self::Confluent {
                client,
                name_strategy,
                topic,
                key_record_name,
                val_record_name,
            } => {
                let record = match IS_KEY {
                    true => key_record_name,
                    false => val_record_name,
                }
                .as_deref();
                let subject = get_subject_by_strategy(name_strategy, topic, record, IS_KEY)?;
                let (primary_subject, dependency_subjects) =
                    client.get_subject_and_references(&subject).await?;
                let schema_id = primary_subject.schema.id;
                let out = Out::compile(primary_subject, dependency_subjects)?;
                Ok((SchemaVersion::Confluent(schema_id), out))
            }
            Self::Glue {
                client,
                schema_arn,
                mock,
            } => {
                use aws_sdk_glue::types::{SchemaId, SchemaVersionNumber};

                use super::schema_registry::ConfluentSchema;
                let res = client
                    .get_schema_version()
                    .schema_id(SchemaId::builder().schema_arn(schema_arn).build())
                    .schema_version_number(
                        SchemaVersionNumber::builder().latest_version(true).build(),
                    )
                    .send()
                    .await
                    .unwrap();
                let schema_version_id = res.schema_version_id().unwrap().parse().unwrap();
                let definition = res.schema_definition().unwrap();
                let primary = Subject {
                    version: 0,
                    name: "".to_owned(),
                    schema: ConfluentSchema {
                        id: 0,
                        content: definition.to_owned(),
                    },
                };
                let out = Out::compile(primary, vec![])?;
                Ok((SchemaVersion::Glue(schema_version_id), out))
            }
        }
    }

    pub async fn load_key_schema<Out: LoadedSchema>(
        &self,
    ) -> Result<(SchemaVersion, Out), SchemaFetchError> {
        self.load_schema::<Out, true>().await
    }

    pub async fn load_val_schema<Out: LoadedSchema>(
        &self,
    ) -> Result<(SchemaVersion, Out), SchemaFetchError> {
        self.load_schema::<Out, false>().await
    }
}

pub trait LoadedSchema: Sized {
    fn compile(primary: Subject, references: Vec<Subject>) -> Result<Self, SchemaFetchError>;
}
