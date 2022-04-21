from ensurepip import bootstrap
from tokenize import group
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
# from confluent_kafka.cimpl import TopicPartition
import const



class ConlfuentConsumer:

    bootstrap_servers = const.bootstrap_servers
    schema_registry_url = const.schema_registry_url
    group_id = const.group_id
    topic_name = const.topic_name

    def confluent_avro_listener(self):
        cons = AvroConsumer(
            {'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id, 
            'schema.registry.url': self.schema_registry_url,
            "api.version.request": True})
        cons.subscribe([self.topic_name])
        running = True
        while running:
            msg = None
            try:
                msg = cons.poll(10)
                if msg:
                    if not msg.error():
                        print(msg.value())
                        print(msg.key())
                        # print(msg.partition())
                        # print(msg.offset())
                        cons.commit(msg)
                    elif msg.error().code() != KafkaError._PARTITION_EOF:
                        print(msg.error())
                        running = False
                else:
                    print("Listening")
            except SerializerError as e:
                print("Message deserialization failed for {}:{}".format(msg, e))
                running = False
        cons.commit()
        cons.close()

my_consumer = ConlfuentConsumer()
my_consumer.confluent_avro_listener()