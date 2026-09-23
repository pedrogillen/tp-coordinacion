from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        self.id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.id, "EOF"])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] != self.id:
            return None
        return fields[1:]
