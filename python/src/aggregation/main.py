import os
import logging
import bisect
import signal
import sys

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_tops_per_client = {}
        self.eof_per_client = {}
        self.messages_received = 0

    def _process_data(self, client_id, fruit, amount):
        # logging.info("Processing data message")
        client_list = self.fruit_tops_per_client.get(client_id, [])
        for i in range(len(client_list)):
            if client_list[i].fruit == fruit:
                client_list[i] = client_list[i] + fruit_item.FruitItem(
                    fruit, amount
                )
                self.fruit_tops_per_client[client_id] = client_list
                return
        bisect.insort(client_list, fruit_item.FruitItem(fruit, amount))
        self.fruit_tops_per_client[client_id] = client_list

    def _process_eof(self, client_id):
        self.eof_per_client[client_id] = self.eof_per_client.get(client_id, 0) + 1
        if self.eof_per_client[client_id] == SUM_AMOUNT:
            logging.info("Received EOF")
            client_fruits = self.fruit_tops_per_client.pop(client_id, [])
            logging.info(f"Sending top {TOP_SIZE} fruits for client {client_id}: {client_fruits}")
            client_fruits.sort(key=lambda x: x.amount, reverse=True)
            fruit_chunk = list(client_fruits[:TOP_SIZE])
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )
            logging.info(f"Sending top {TOP_SIZE} fruits for client {client_id}: {fruit_top}")
            self.output_queue.send(message_protocol.internal.serialize([client_id] + fruit_top))

    def process_messsage(self, message, ack, nack):
        # logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        self.messages_received += 1
        if len(fields) == 3:
            [client_id, fruit, amount] = fields
            if self.messages_received % 20 == 0:
                logging.info(f"Received data message from client {client_id}")
            self._process_data(client_id, fruit, amount)
        elif len(fields) == 2 and fields[1] == "EOF":
            [client_id, _] = fields
            logging.info(f"Received EOF message from client {client_id}")
            self._process_eof(client_id)
        else:
            logging.error(f"Invalid message received: {fields}")
        ack()

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            self.stop()
        finally:
            self.stop()

    def stop(self):
        try:
            self.input_exchange.close()
            self.output_queue.close()
        except Exception as e:
            logging.error(f"Error closing queues: {e}")
            sys.exit(1)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: aggregation_filter.stop())
    try:
        aggregation_filter.start()
    except KeyboardInterrupt:
        aggregation_filter.stop()
    return 0


if __name__ == "__main__":
    main()
