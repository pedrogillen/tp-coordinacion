from abc import abstractmethod
import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

def create_callback_function(on_message_callback):
    def callback(ch, method, properties, body):
        ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
        on_message_callback(body, ack, nack)
    return callback

class RabbitMQMiddleware:
    PIKA_DISCONNECT_ERRORS = (
        pika.exceptions.AMQPConnectionError,
        pika.exceptions.ChannelClosedByBroker,
        pika.exceptions.ConnectionClosedByBroker,
        pika.exceptions.StreamLostError,
    )

    def __init__(self, host):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.consuming = False

    def start_consuming(self, on_message_callback):
        if self.consuming:
            return
        try:
            self.consuming = True
            callback = create_callback_function(on_message_callback)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except self.PIKA_DISCONNECT_ERRORS as e:
            raise MessageMiddlewareDisconnectedError("Error starting consumption") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Error consuming message") from e
        finally:
            self.consuming = False

    def stop_consuming(self):
        if not self.consuming:
            return
        try:
            self.channel.stop_consuming()
            self.consuming = False
        except self.PIKA_DISCONNECT_ERRORS as e:
            raise MessageMiddlewareDisconnectedError("Error stopping consumption") from e

    def close(self):
        try:
            self.stop_consuming()
            self.channel.close()
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError("Error closing connection") from e

    @abstractmethod
    def send(self, message):
        pass

class MessageMiddlewareQueueRabbitMQ(RabbitMQMiddleware, MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        super().__init__(host)
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name)

    def send(self, message):
        try:
            self.channel.basic_publish(body=message, exchange='', routing_key=self.queue_name)
        except self.PIKA_DISCONNECT_ERRORS as e:
            raise MessageMiddlewareDisconnectedError("Connection lost") from e
        except Exception as e:
            raise MessageMiddlewareMessageError("Error sending message") from e

class MessageMiddlewareExchangeRabbitMQ(RabbitMQMiddleware, MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.queue_name = None
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=routing_key)

    def send(self, message):
        for routing_key in self.routing_keys:
            try:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
            except self.PIKA_DISCONNECT_ERRORS as e:
                raise MessageMiddlewareDisconnectedError("Connection lost") from e
            except Exception as e:
                raise MessageMiddlewareMessageError("Error sending message") from e