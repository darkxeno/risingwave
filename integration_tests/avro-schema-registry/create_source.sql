create sink s from t
with (
    connector = 'kafka',
    properties.bootstrap.server = 'message_queue:29092',
    topic = 'public.t',
    primary_key = 'id'
) FORMAT UPSERT ENCODE AVRO (
    schema.registry = 'http://message_queue:8081'
);
