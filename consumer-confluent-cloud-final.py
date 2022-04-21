from confluent_kafka import DeserializingConsumer
from confluent_kafka.avro import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import const

CONSUMER_GROUP_ID = const.consumer_group_id
TOPIC_NAME = const.TOPIC_NAME

sr_client = SchemaRegistryClient({
'url': const.schema_registry_url,
'basic.auth.user.info':(const.basic_auth_user_info)})

conf = {}
conf.update({
    'bootstrap.servers': const.bootstrap_servers,
    'sasl.username': const.sasl_username,
    'sasl.password': const.sasl_password,
    'sasl.mechanisms': const.sasl_mechanism,
    'security.protocol': const.security_protocol, 
    'key.deserializer': AvroDeserializer(sr_client),
    'value.deserializer': AvroDeserializer(sr_client),
    'auto.offset.reset': 'earliest',
    'group.id': const.consumer_group_id
})


consumer = DeserializingConsumer(conf)
consumer.subscribe([const.TOPIC_NAME])


while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            print("Listening")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            # key_object = msg.key()
            value_object = msg.value()

            print((value_object))

    except KeyboardInterrupt:
        break
    except SerializerError as e:
        # Report malformed record, discard results, continue polling
        print("Message deserialization failed {}".format(e))
        pass