
from time import sleep
import json
import traceback
import uuid
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import const
import sys
import inspect


class ConfluentProducer:
    value_schema = avro.load(const.value_schema)
    key_schema = avro.load(const.key_schema)
    value = const.value
    key = const.key
    json_file = const.json_file



    def delivery_report(err, msg):
        if err:
            print("message delivery failed : {}".format(str(err)))
        else:
            print("message is delivered to topic name : {1} on the partition : {0}".format(msg.partition(), msg.topic()))
            print("message value = {}".format(msg.value()))


    def confluent_avro_publish(self):
        avroProducer = AvroProducer({'bootstrap.servers': const.bootstrap_servers,
            'schema.registry.url': const.schema_registry_url}, 
            default_key_schema=self.key_schema, 
            default_value_schema=self.value_schema)

        fObj = open(self.json_file, 'r')
        obj = json.load(fObj)
        print(obj)
        # for i in obj:
        #   value = i

        avroProducer.produce(topic=const.topic_name,
         value=obj,key=self.key, 
         key_schema=self.key_schema,
          value_schema=self.value_schema, on_delivery=ConfluentProducer.delivery_report)
        sleep(1)
        avroProducer.flush()




my_producer = ConfluentProducer()
my_producer.confluent_avro_publish()

