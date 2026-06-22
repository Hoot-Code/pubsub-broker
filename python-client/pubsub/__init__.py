"""pubsub — Python client library for pubsub-broker.

Pure Python, no external dependencies (stdlib only). Supports Python 3.9+.

Quickstart::

    from pubsub import Client

    with Client("127.0.0.1:9000") as c:
        prod = c.new_producer("orders")
        prod.publish("key-1", b'{"amount": 99}')

        cons = c.new_consumer("my-group", "orders")
        for msg in cons.fetch(partition=0, offset=0):
            print(msg.offset, msg.payload)
            cons.commit(msg.partition, msg.offset)
"""

from .client import Client
from .consumer import Consumer
from .producer import Producer
from .types import BrokerError, Message

__all__ = ["Client", "Producer", "Consumer", "Message", "BrokerError"]
__version__ = "0.1.0"
