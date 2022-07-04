import sys
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.serialization import StringDeserializer

from rb_crawler.constant import BOOTSTRAP_SERVER

def consume_topic(topic, schema, group_id):

    deserializer = ProtobufDeserializer(
        schema, {"use.deprecated.format": True}
    )

    string_deserializer = StringDeserializer("utf_8")
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'key.deserializer': string_deserializer,
        'value.deserializer': deserializer,
        'group.id': group_id,
        'session.timeout.ms': 6000,
        'auto.offset.reset': 'earliest'
    }

    c = DeserializingConsumer(conf=config)
    c.subscribe([topic])

    messages = []
    try:
        while True:
            msg = c.poll(timeout=10.0)
            if msg is None:
                break
            if msg.error():
                print("Error")
            else:
                messages.append(msg.value())
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        c.close()

    return messages