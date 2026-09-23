import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_exchange_producer = self._create_control_exchange()
        self.control_thread = threading.Thread(target=self._start_control_listener_thread, daemon=True)
        self.control_thread.start()
        self.fruit_count_lock = threading.Lock()
        self.amount_by_fruit_and_client = {}

    def _create_control_exchange(self):
        logging.info(f"Creating control exchange for client")
        return middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}"]
        )

    def _start_control_listener_thread(self):
        data_output_exchanges = []
        control_exchange_consumer = self._create_control_exchange()
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            data_output_exchanges.append(data_output_exchange)
        def receive_control_message(message, ack, nack):
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 2 and fields[1] == "EOF":
                [client_id, _] = fields
                logging.info(f"Received EOF message from client {client_id}")
                self._send_processed_data(client_id, data_output_exchanges)
            else:
                logging.error(f"Invalid control message received: {fields}")
            ack()
        
        control_exchange_consumer.start_consuming(receive_control_message)


    def _process_data(self, client_id, fruit, amount):
        with self.fruit_count_lock:
            self.amount_by_fruit_and_client[client_id] = self.amount_by_fruit_and_client.get(client_id, {})
            self.amount_by_fruit_and_client[client_id][fruit] = self.amount_by_fruit_and_client.get(client_id, {}).get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        self.control_exchange_producer.send(message_protocol.internal.serialize([client_id, "EOF"]))

    def _send_processed_data(self, client_id, data_output_exchanges):
        with self.fruit_count_lock:
            client_fruits = self.amount_by_fruit_and_client.pop(client_id, {})

        for final_fruit_item in client_fruits.values():
            for data_output_exchange in data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id, "EOF"]))
        logging.info(f"Finished broadcasting data messages for client {client_id}")

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            [client_id, fruit, amount] = fields
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
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
