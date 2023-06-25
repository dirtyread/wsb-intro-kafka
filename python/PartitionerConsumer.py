from confluent_kafka import Consumer


class PartitionerConsumer:

    __TOPIC_TO_SUBSCRIBE = "partitioner-topic-test"

    @staticmethod
    def subscribe_our_messages():
        # consumer
        consumer = PartitionerConsumer.create_consumer()

        # subscribe
        consumer.subscribe([PartitionerConsumer.__TOPIC_TO_SUBSCRIBE])

        # messages records
        while True:
            message = consumer.poll(10.0)

            if message is None:
                break

            print(message.value().decode('utf-8'))

        consumer.close()

    @staticmethod
    def create_consumer() -> Consumer:
        return Consumer(
            {
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'group-id-conf-1-python',
                'auto.offset.reset': 'earliest'
            }
        )


def main():
    PartitionerConsumer.subscribe_our_messages()


if __name__ == '__main__':
    main()
