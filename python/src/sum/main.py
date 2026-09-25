import os
import logging
import threading
import hashlib

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
        self.messages_received = 0
        self.messages_received_lock = threading.Lock()
        self.messages_received_per_client = {}
        self.eof_lock = threading.Lock()
        self.eof_received_per_client = set()

    def _create_control_exchange(self):
        logging.info(f"Creating control exchange for client")
        return middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}"]
        )

    def _start_control_listener_thread(self):
        data_output_exchanges = []
        control_exchange_consumer = self._create_control_exchange()
        control_exchange_producer = self._create_control_exchange()
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            data_output_exchanges.append(data_output_exchange)
        def receive_control_message(message, ack, nack):
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3 and fields[1] == "EOF":
                [client_id, _, expected_messages] = fields
                logging.info(f"Received EOF message from client {client_id}")
                self._manage_eof(client_id, expected_messages, control_exchange_producer)
            elif len(fields) == 3 and fields[1] == "MESSAGE_RECEIVED":
                [client_id, _, peer_id] = fields
                logging.info(f"Received MESSAGE_RECEIVED message from client {client_id}")
                message_received_count = 1 if peer_id != ID else 0
                if self._update_received_count(client_id, message_received_count):
                    self._send_processed_data(client_id, data_output_exchanges)
            elif len(fields) == 4 and fields[1] == "MESSAGES_SENT":
                [client_id, _, messages_sent, peer_id] = fields
                message_received_count = messages_sent if peer_id != ID else 0
                logging.info(f"Received {messages_sent} from peer {peer_id} message for {client_id}")
                if self._update_received_count(client_id, message_received_count):
                    self._send_processed_data(client_id, data_output_exchanges)
            else:
                logging.error(f"Invalid control message received: {fields}")
            ack()

        
        control_exchange_consumer.start_consuming(receive_control_message)

    def _update_received_count(self, client_id, messages):
        send_data_flag = False
        with self.messages_received_lock:
            self.messages_received_per_client[client_id] = self.messages_received_per_client.get(client_id, {"expected": None, "internal": 0,"received": 0})
            self.messages_received_per_client[client_id]["received"] += messages
            total_messages = self.messages_received_per_client[client_id]["received"] + self.messages_received_per_client[client_id]["internal"]
            if total_messages == self.messages_received_per_client[client_id]["expected"]:
                logging.info(f"Client {client_id} has sent all messages, sending processed data")
                send_data_flag = True
        return send_data_flag

    def _manage_eof(self, client_id, expected_messages, control_exchange_producer):
        with self.eof_lock:
            self.eof_received_per_client.add(client_id)
            with self.messages_received_lock:
                # aseguro que este en el diccionario, sino lo agrego con el valor de expected_messages
                self.messages_received_per_client[client_id] = self.messages_received_per_client.get(client_id, {"expected": expected_messages, "internal": 0,"received": 0})
                # si no estaba en el diccionario, lo agrego con el valor de expected_messages, sino lo actualizo
                self.messages_received_per_client[client_id]["expected"] = expected_messages
                # mensajes recibidos hasta ahora en este nodo
                received_messages = self.messages_received_per_client[client_id]["internal"]
        control_exchange_producer.send(message_protocol.internal.serialize([client_id, "MESSAGES_SENT", received_messages, ID]))

    def _process_data(self, client_id, fruit, amount):
        with self.fruit_count_lock:
            client_dict = self.amount_by_fruit_and_client.setdefault(client_id, {})
            current_item = client_dict.get(fruit, fruit_item.FruitItem(fruit, 0))
            client_dict[fruit] = current_item + fruit_item.FruitItem(fruit, int(amount))

        needs_notification = False
        # Mismo orden que en _manage_eof: eof_lock -> messages_received_lock
        with self.eof_lock:
            with self.messages_received_lock:
                client_info = self.messages_received_per_client.setdefault(
                    client_id, {"expected": None, "internal": 0, "received": 0}
                )
                client_info["internal"] += 1
                self.messages_received_per_client[client_id] = client_info
                # Solo notifica si el EOF ya había sido procesado ANTES de este incremento
                if client_id in self.eof_received_per_client:
                    needs_notification = True

        # El envío por red se hace FUERA de ambos locks
        if needs_notification:
            logging.info(f"Received data message for client {client_id} after EOF, sending MESSAGE_RECEIVED")
            self.control_exchange_producer.send(
                message_protocol.internal.serialize([client_id, "MESSAGE_RECEIVED", ID])
            )

    def _process_eof(self, client_id, total_messages):
        logging.info(f"Broadcasting data messages")
        self.control_exchange_producer.send(message_protocol.internal.serialize([client_id, "EOF", total_messages]))

    def _send_processed_data(self, client_id, data_output_exchanges):
        with self.eof_lock:
            self.eof_received_per_client.discard(client_id)
        with self.fruit_count_lock:
            client_fruits = self.amount_by_fruit_and_client.pop(client_id, {})
        with self.messages_received_lock:
            self.messages_received_per_client.pop(client_id, 0)

        for final_fruit_item in client_fruits.values():
            digest_hex = hashlib.md5(
                final_fruit_item.fruit.encode("utf-8")
            ).hexdigest()
            aggregator_idx = int(digest_hex, 16) % AGGREGATION_AMOUNT

            target_exchange = data_output_exchanges[aggregator_idx]
            logging.info(f"Sending data message for client {client_id} to exchange {target_exchange.exchange_name}: {final_fruit_item.fruit} - {final_fruit_item.amount}")

            target_exchange.send(message_protocol.internal.serialize([client_id, final_fruit_item.fruit, final_fruit_item.amount]))

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id, "EOF"]))
        logging.info(f"Finished broadcasting data messages for client {client_id}")

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self.messages_received += 1
        if len(fields) == 3 and fields[1] == "EOF":
            [client_id, _, total_messages] = fields
            logging.info(f"Received EOF message from client {client_id}")
            self._process_eof(client_id, total_messages)
        elif len(fields) == 3:
            [client_id, fruit, amount] = fields
            if self.messages_received % 20 == 0:
                logging.info(f"Received data message from client {client_id}")
            self._process_data(client_id, fruit, amount)
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
