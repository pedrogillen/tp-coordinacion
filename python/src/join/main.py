import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.tops_per_clients_received = {}
        self.fruit_tops_per_client = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        deserialized_message = message_protocol.internal.deserialize(message)
        client_id = deserialized_message[0]
        fruit_top = deserialized_message[1:]
        self._update_fruit_tops(client_id, fruit_top)
        self.tops_per_clients_received[client_id] = self.tops_per_clients_received.get(client_id, 0) + 1
        if self.tops_per_clients_received[client_id] == AGGREGATION_AMOUNT:
            logging.info(f"Received all tops for client {client_id}")
            fruit_top = self.fruit_tops_per_client.pop(client_id, [])
            fruit_top.sort(key=lambda x: x[1], reverse=True)
            fruit_chunk = list(fruit_top[:TOP_SIZE])
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item[0], fruit_item[1]),
                    fruit_chunk,
                )
            )
            logging.info(f"Sending top {TOP_SIZE} fruits for client {client_id}: {fruit_top}")
            self.output_queue.send(message_protocol.internal.serialize([client_id] + fruit_top))
        ack()

    def _update_fruit_tops(self, client_id, fruit_top):
        logging.info(f"Updating fruit tops for client {client_id}: {fruit_top}")
        actual_top = self.fruit_tops_per_client.get(client_id, [])
        actual_top_dict = {fruit: amount for fruit, amount in actual_top}
        for fruit, amount in fruit_top:
            current_amount = actual_top_dict.get(fruit, 0)
            actual_top_dict[fruit] = current_amount if current_amount > amount else amount
        updated_top = list(actual_top_dict.items())
        updated_top.sort(key=lambda x: x[1], reverse=True)
        self.fruit_tops_per_client[client_id] = updated_top[:TOP_SIZE]

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
