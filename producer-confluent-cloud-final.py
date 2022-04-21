from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_avro.schema_registry import SchemaRegistry
from confluent_avro.schema_registry import HTTPBasicAuth
from confluent_kafka.avro import AvroProducer
import const
from confluent_kafka import avro
import json
# try:
#     # import some credentials, please refer to readme and get your own security info.
#     # from env import *
# except:
#     pass
# https://avro.apache.org/docs/current/gettingstartedpython.html
# https://avro.apache.org/docs/current/spec.html#schema_record

value_schema_str = str(const.value_schema_str)
key_schema_str = str(const.key_schema_str)

sr_client = SchemaRegistryClient({
'url': const.schema_registry_url,
'basic.auth.user.info':(const.basic_auth_user_info)})

# https://docs.confluent.io/platform/current/schema-registry/develop/api.html#put--config-(string-%20subject)
# BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE

# producer configurations
# for full list of configurations, see:
# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/#serializingproducer
conf = {}
conf.update({
    'bootstrap.servers': const.bootstrap_servers,
    'sasl.username': const.sasl_username,
    'sasl.password': const.sasl_password,
    'sasl.mechanisms': const.sasl_mechanism,  # fixed to sasl_ssl, plain
    'security.protocol': const.security_protocol, 
    'key.serializer': AvroSerializer(sr_client, schema_str=key_schema_str),
    'value.serializer': AvroSerializer(sr_client, schema_str=value_schema_str)
})


# mess=[{'name': 'value', "familyid": "700728482802", "arrivaldate": 200302},
# {'name': 'value', "familyid": "700728482801", "arrivaldate": 200301}]

# initialize producer instance
for dict1 in json.load(open("C:/Users/Dell/Kafka/confluent-kafka-python/Prod_Cons_Cloud/dev7/NE01_ODBX49_202109.sample1.json", 'r')):
  producer = SerializingProducer(conf)
  producer.produce(
      topic=const.TOPIC_NAME,
      key={'name': 'key'},
      value=dict1
  )
  producer.poll(0)
  producer.flush()

  print('message = {} posted successfully to confluent cloud'.format(dict1))