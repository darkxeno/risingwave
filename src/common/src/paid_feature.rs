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

use risingwave_license::{License, LicenseKeyError, LicenseManager, Tier};
use risingwave_pb::telemetry::PbTelemetryEventStage;
use thiserror::Error;

use crate::telemetry::report::report_event_common;

/// Define all features that are available based on the tier of the license.
///
/// # Define a new feature
///
/// To add a new feature, add a new entry below following the same pattern as the existing ones.
///
/// Check the definition of [`Tier`] for all available tiers. Note that normally there's no need to
/// add a feature with the minimum tier of `Free`, as you can directly write the code without
/// gating it with a feature check.
///
/// # Check the availability of a feature
///
/// To check the availability of a feature during runtime, call the method
/// [`check_available`](Feature::check_available) on the feature. If the feature is not available,
/// an error of type [`FeatureNotAvailable`] will be returned and you should handle it properly,
/// generally by returning an error to the user.
///
/// # Feature availability in tests
///
/// In tests with `debug_assertions` enabled, a license key of the paid (maximum) tier is set by
/// default. As a result, all features are available in tests. To test the behavior when a feature
/// is not available, you can manually set a license key with a lower tier. Check the e2e test cases
/// under `error_ui` for examples.
macro_rules! for_all_features {
    ($macro:ident) => {
        $macro! {
            // name                 min tier    doc
            { TestPaid,             Paid,       "A dummy feature that's only available on paid tier for testing purposes." },
            { TimeTravel,           Paid,       "Query historical data within the retention period."},
            { GlueSchemaRegistry,   Paid,       "Use Schema Registry from AWS Glue rather than Confluent." },
            { SnowflakeSink,        Paid,       "Delivering data to SnowFlake." },
            { DynamoDbSink,         Paid,       "Delivering data to DynamoDb." },
            { OpenSearchSink,       Paid,       "Delivering data to OpenSearch." },
            { BigQuerySink,         Paid,       "Delivering data to BigQuery." },
            { ClickHouseSharedEngine,Paid,      "Delivering data to Shared tree on clickhouse cloud"},
            { SecretManagement,     Paid,       "Secret management." },
            { CdcTableSchemaMap,    Paid,       "Automatically map upstream schema to CDC Table."},
            { SqlServerSink,        Paid,       "Sink data from RisingWave to SQL Server." },
            { SqlServerCdcSource,   Paid,       "CDC source connector for Sql Server." },
        }
    };
}

macro_rules! def_feature {
    ($({ $name:ident, $min_tier:ident, $doc:literal },)*) => {
        /// A set of features that are available based on the tier of the license.
        ///
        /// To define a new feature, add a new entry in the macro [`for_all_features`].
        #[derive(Clone, Copy, Debug)]
        pub enum Feature {
            $(
                #[doc = concat!($doc, "\n\nAvailable for tier `", stringify!($min_tier), "` and above.")]
                $name,
            )*
        }

        impl Feature {
            /// Minimum tier required to use this feature.
            fn min_tier(self) -> Tier {
                match self {
                    $(
                        Self::$name => Tier::$min_tier,
                    )*
                }
            }

            fn get_feature_name(&self) -> &'static str {
                match &self {
                    $(
                        Self::$name => stringify!($name),
                    )*
                }
            }
        }
    };
}

for_all_features!(def_feature);

/// The error type for feature not available due to license.
#[derive(Debug, Error)]
pub enum FeatureNotAvailable {
    #[error(
        "feature {:?} is only available for tier {:?} and above, while the current tier is {:?}\n\n\
        Hint: You may want to set a license key with `ALTER SYSTEM SET license_key = '...';` command.",
        feature, feature.min_tier(), current_tier,
    )]
    InsufficientTier {
        feature: Feature,
        current_tier: Tier,
    },

    #[error("feature {feature:?} is not available due to license error")]
    LicenseError {
        feature: Feature,
        source: LicenseKeyError,
    },
}

impl Feature {
    /// Check whether the feature is available based on the current license.
    pub fn check_available(self) -> Result<(), FeatureNotAvailable> {
        let check_res = match LicenseManager::get().license() {
            Ok(license) => {
                if license.tier >= self.min_tier() {
                    Ok(())
                } else {
                    Err(FeatureNotAvailable::InsufficientTier {
                        feature: self,
                        current_tier: license.tier,
                    })
                }
            }
            Err(error) => {
                // If there's a license key error, we still try against the default license first
                // to see if the feature is available for free.
                if License::default().tier >= self.min_tier() {
                    Ok(())
                } else {
                    Err(FeatureNotAvailable::LicenseError {
                        feature: self,
                        source: error,
                    })
                }
            }
        };
        if let feature_name = Self::get_feature_name(&self)
            && !feature_name.eq_ignore_ascii_case("TestPaid")
        {
            let mut attr_builder = jsonbb::Builder::<Vec<u8>>::new();
            attr_builder.begin_object();
            attr_builder.add_string("success");
            attr_builder.add_value(jsonbb::ValueRef::Bool(check_res.is_ok()));
            attr_builder.end_object();
            let attr = attr_builder.finish();

            report_event_common(
                PbTelemetryEventStage::Unspecified,
                feature_name,
                0,
                None,
                None,
                Some(attr),
                "paywall".to_string(),
            );
        }

        check_res
    }
}
