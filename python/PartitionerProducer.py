from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer


class PartitionerProducer:

    __TOPIC = "partitioner-topic-test"
    __KEY = "partition_1"
    __VALUE = "Zapisane na 1 partycji..."

    @staticmethod
    def send_message_to_kafka_topic():
        # create producer
        producer = PartitionerProducer.create_producer()

        # prepare serializer (not in producer config!!!)
        string_serializer = StringSerializer('utf_8')

        # simulate partitioner object behavior
        partition_no = PartitionerProducer.get_partition_no_from_key(PartitionerProducer.__KEY)

        # prepare record to send and send action
        producer.poll(0)
        producer.produce(
            topic=PartitionerProducer.__TOPIC,
            key=string_serializer(PartitionerProducer.__KEY),
            value=string_serializer(PartitionerProducer.__VALUE),
            partition=partition_no
        )
        producer.flush()

    @staticmethod
    def create_producer() -> Producer:
        # this producer config map is not equivalent ProducerConfig.java :|
        return Producer(
            {
                'bootstrap.servers': 'localhost:9092',
                'client.id': 'our-producer',
            }
        )

    @staticmethod
    def get_partition_no_from_key(key):
        # it's equivalent OurPartitioner.java behavior
        return int(key.split("_")[-1])


def main():
    PartitionerProducer.send_message_to_kafka_topic()


if __name__ == '__main__':
    main()
