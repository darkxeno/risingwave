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

use apache_avro::from_avro_datum;
use risingwave_connector_codec::decoder::avro::{
    avro_schema_to_column_descs, AvroAccess, AvroParseOptions, MapHandling, ResolvedAvroSchema,
};
use risingwave_connector_codec::decoder::Access;
use risingwave_connector_codec::AvroSchema;

use crate::utils::*;

/// Refer to `AvroAccessBuilder::parse_avro_value` for how Avro data looks like.
enum TestDataEncoding {
    /// Each data is a JSON encoded Avro value.
    ///
    /// Refer to: <https://avro.apache.org/docs/1.11.1/specification/#json-encoding>
    ///
    /// Specially, it handles `union` variants, and differentiates `bytes` from `string`.
    ///
    /// TODO: Not supported yet, because `apache_avro` doesn't support decoding JSON encoded avro..
    #[allow(dead_code)]
    Json,
    /// Each data is a binary encoded Avro value, converted to a hex string.
    ///
    /// Tool convert Avro JSON to hex string: <https://xxchan-vercel-playground.vercel.app/avro>
    HexBinary,
}

struct Config {
    /// TODO: For one test data, we can test all possible config options.
    map_handling: Option<MapHandling>,
    data_encoding: TestDataEncoding,
}

/// ## Arguments
/// - `avro_schema`: Avro schema in JSON format.
/// - `avro_data`: list of Avro data. Refer to [`TestDataEncoding`] for the format.
///
/// ## Why not directly test the uppermost layer `AvroParserConfig` and `AvroAccessBuilder`?
///
/// Because their interface are not clean enough, and have complex logic like schema registry.
/// We might need to separate logic to make them clenaer and then we can use it directly for testing.
///
/// ## If we reimplement a similar logic here, what are we testing?
///
/// Basically unit tests of `avro_schema_to_column_descs`, `convert_to_datum`, i.e., the type mapping.
///
/// It makes some sense, as the data parsing logic is generally quite simple (one-liner), and the most
/// complex and error-prone part is the type mapping.
///
/// ## Why test schema mapping and data mapping together?
///
/// Because the expected data type for data mapping comes from the schema mapping.
fn check(
    avro_schema: &str,
    avro_data: &[&str],
    config: Config,
    expected_risingwave_schema: expect_test::Expect,
    expected_risingwave_data: expect_test::Expect,
) {
    // manually implement some logic in AvroParserConfig::map_to_columns
    let avro_schema = AvroSchema::parse_str(avro_schema).expect("failed to parse Avro schema");
    let resolved_schema =
        ResolvedAvroSchema::create(avro_schema.into()).expect("failed to resolve Avro schema");

    let rw_schema =
        avro_schema_to_column_descs(&resolved_schema.resolved_schema, config.map_handling)
            .expect("failed to convert Avro schema to RisingWave schema")
            .iter()
            .map(ColumnDesc::from)
            .collect_vec();
    expected_risingwave_schema.assert_eq(&format!(
        "{:#?}",
        rw_schema.iter().map(ColumnDescTestDisplay).collect_vec()
    ));

    // manually implement some logic in AvroAccessBuilder, and some in PlainParser::parse_inner
    let mut data_str = vec![];
    for data in avro_data {
        let parser = AvroParseOptions::create(&resolved_schema.resolved_schema);

        match config.data_encoding {
            TestDataEncoding::Json => todo!(),
            TestDataEncoding::HexBinary => {
                let data = hex::decode(data).expect("failed to decode hex string");
                let avro_data =
                    from_avro_datum(&resolved_schema.original_schema, &mut data.as_slice(), None)
                        .expect("failed to parse Avro data");
                let access = AvroAccess::new(&avro_data, parser);

                for col in &rw_schema {
                    let rw_data = access
                        .access(&[&col.name], &col.data_type)
                        .expect("failed to access");
                    data_str.push(format!("{:?}", rw_data));
                }
            }
        }
    }
    expected_risingwave_data.assert_eq(&format!("{}", data_str.iter().format("\n")));
}

#[test]
fn test_simple() {
    check(
        r#"
{
  "name": "test_student",
  "type": "record",
  "fields": [
    {
      "name": "id",
      "type": "int",
      "default": 0
    },
    {
      "name": "sequence_id",
      "type": "long",
      "default": 0
    },
    {
      "name": "name",
      "type": ["null", "string"]
    },
    {
      "name": "score",
      "type": "float",
      "default": 0.0
    },
    {
      "name": "avg_score",
      "type": "double",
      "default": 0.0
    },
    {
      "name": "is_lasted",
      "type": "boolean",
      "default": false
    },
    {
      "name": "entrance_date",
      "type": "int",
      "logicalType": "date",
      "default": 0
    },
    {
      "name": "birthday",
      "type": "long",
      "logicalType": "timestamp-millis",
      "default": 0
    },
    {
      "name": "anniversary",
      "type": "long",
      "logicalType": "timestamp-micros",
      "default": 0
    },
    {
      "name": "passed",
      "type": {
        "name": "interval",
        "type": "fixed",
        "size": 12
      },
      "logicalType": "duration"
    },
    {
      "name": "bytes",
      "type": "bytes",
      "default": ""
    }
  ]
}
        "#,
        &[
            // {"id":32,"sequence_id":64,"name":{"string":"str_value"},"score":32.0,"avg_score":64.0,"is_lasted":true,"entrance_date":0,"birthday":0,"anniversary":0,"passed":"\u0001\u0000\u0000\u0000\u0001\u0000\u0000\u0000\u00E8\u0003\u0000\u0000","bytes":"\u0001\u0002\u0003\u0004\u0005"}
              "40800102127374725f76616c7565000000420000000000005040010000000100000001000000e80300000a0102030405"
            ],
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            [
                id(#1): Int32,
                sequence_id(#2): Int64,
                name(#3): Varchar,
                score(#4): Float32,
                avg_score(#5): Float64,
                is_lasted(#6): Boolean,
                entrance_date(#7): Date,
                birthday(#8): Timestamptz,
                anniversary(#9): Timestamptz,
                passed(#10): Interval,
                bytes(#11): Bytea,
            ]"#]],
        expect![[r#"
            Owned(Some(Int32(32)))
            Owned(Some(Int64(64)))
            Borrowed(Some(Utf8("str_value")))
            Owned(Some(Float32(OrderedFloat(32.0))))
            Owned(Some(Float64(OrderedFloat(64.0))))
            Owned(Some(Bool(true)))
            Owned(Some(Date(Date(1970-01-01))))
            Owned(Some(Timestamptz(Timestamptz(0))))
            Owned(Some(Timestamptz(Timestamptz(0))))
            Owned(Some(Interval(Interval { months: 1, days: 1, usecs: 1000000 })))
            Borrowed(Some(Bytea([1, 2, 3, 4, 5])))"#]],
    )
}

/// From `e2e_test/source_inline/kafka/avro/upsert_avro_json`
#[test]
fn test_1() {
    check(
        r#"
{
  "type": "record",
  "name": "OBJ_ATTRIBUTE_VALUE",
  "namespace": "CPLM",
  "fields": [
    {
      "name": "op_type",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "CLASS_ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ITEM_ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ATTR_ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ATTR_VALUE",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ORG_ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "UNIT_INFO",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "UPD_TIME",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "DEC_VAL",
      "type": [
        {
          "type": "bytes",
          "logicalType": "decimal",
          "precision": 10,
          "scale": 2
        },
        "null"
      ],
      "default": "\u00ff"
    },
    {
      "name": "REFERRED",
      "type": [
        "null",
        {
          "type": "record",
          "name": "REFERRED_TYPE",
          "fields": [
            {
              "name": "a",
              "type": "string"
            }
          ]
        }
      ],
      "default": null
    },
    {
      "name": "REF",
      "type": [
        "null",
        "REFERRED_TYPE"
      ],
      "default": null
    },
    {
      "name": "uuid",
      "type": [
        "null",
        {
          "type": "string",
          "logicalType": "uuid"
        }
      ],
      "default": null
    },
    {
      "name": "rate",
      "type": "double",
      "default": "NaN"
    }
  ],
  "connect.name": "CPLM.OBJ_ATTRIBUTE_VALUE"
}
"#,
        &[
            // {"op_type": {"string": "update"}, "ID": {"string": "id1"}, "CLASS_ID": {"string": "1"}, "ITEM_ID": {"string": "6768"}, "ATTR_ID": {"string": "6970"}, "ATTR_VALUE": {"string": "value9"}, "ORG_ID": {"string": "7172"}, "UNIT_INFO": {"string": "info9"}, "UPD_TIME": {"string": "2021-05-18T07:59:58.714Z"}, "DEC_VAL": {"bytes": "\u0002\u0054\u000b\u00e3\u00ff"}}
            "020c7570646174650206696431020231020836373638020836393730020c76616c756539020837313732020a696e666f390230323032312d30352d31385430373a35393a35382e3731345a000a02540be3ff000000000000000000f87f"
            ],
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            [
                op_type(#1): Varchar,
                ID(#2): Varchar,
                CLASS_ID(#3): Varchar,
                ITEM_ID(#4): Varchar,
                ATTR_ID(#5): Varchar,
                ATTR_VALUE(#6): Varchar,
                ORG_ID(#7): Varchar,
                UNIT_INFO(#8): Varchar,
                UPD_TIME(#9): Varchar,
                DEC_VAL(#10): Decimal,
                REFERRED(#11): Struct(StructType { field_names: ["a"], field_types: [Varchar] }),
                REF(#12): Struct(StructType { field_names: ["a"], field_types: [Varchar] }),
                uuid(#13): Varchar,
                rate(#14): Float64,
            ]"#]],
        expect![[r#"
            Borrowed(Some(Utf8("update")))
            Borrowed(Some(Utf8("id1")))
            Borrowed(Some(Utf8("1")))
            Borrowed(Some(Utf8("6768")))
            Borrowed(Some(Utf8("6970")))
            Borrowed(Some(Utf8("value9")))
            Borrowed(Some(Utf8("7172")))
            Borrowed(Some(Utf8("info9")))
            Borrowed(Some(Utf8("2021-05-18T07:59:58.714Z")))
            Owned(Some(Decimal(Normalized(99999999.99))))
            Owned(None)
            Owned(None)
            Owned(None)
            Owned(Some(Float64(OrderedFloat(NaN))))"#]],
    );
}
