#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pika
from time import sleep
from multiprocessing import Queue, Process
from multiprocessing.queues import Empty as EmptyQueue
from os import environ as env

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class MQClient(object):

    EXCHANGE = env.get('EXCHANGE', '')
    EXCHANGE_TYPE = env.get('EXCHANGE_TYPE', 'topic')
    QUEUE = env.get('TASK_QUEUE', 'task_queue')
    MQ_HOST = env.get('RABBIT_URL', 'localhost')
    MQ_SLEEP_INTERVAL = 30

    def __init__(self):
        self._connection = None
        self._channel = None
        self._results = Queue()
        self._message_callback = None
        self._p = None

    def connect(self):
        """
        Connects to amqp server
        :return:
        """
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self.__class__.MQ_HOST
        ))
        LOGGER.info('Connected to amqp broker')
        self._channel = self._connection.channel()
        self._declare_queues(self.__class__.QUEUE)

    def kill_process(self):
        if self._p:
            self._p.close()

    def _on_message(self, ch, method, properties, body):

        LOGGER.info('Received message # %s from %s: %s',
                    method.delivery_tag, properties.app_id, body)
        self._p = Process(target=self._message_callback, args=(body, self._results))
        self._p.daemon = True
        self._p.start()

        result = self.await_response()

        LOGGER.info('Process completed with value %s', result)

        self._channel.basic_ack(delivery_tag=method.delivery_tag)
        self._channel.basic_publish(exchange='',
                                    routing_key='notify_queue',
                                    body=result,
                                    properties=pika.BasicProperties())

    def await_response(self):
        """
        Waits for the concurrent process to terminate while keeping the amqp
        connection alive.
        """

        # TODO: set a sufficiently large timeout to kill rougue jobs (48hrs?)

        LOGGER.info('Entering keep-alive loop')
        while True:
            interval = self.__class__.MQ_SLEEP_INTERVAL
            LOGGER.info('Snoozing channel connection for %ss...', interval)
            self._connection.sleep(interval)

            try:
                result = self._results.get(False)
            except EmptyQueue:
                result = None

            if result:
                return result

    def _declare_queues(self, queue):
        """
        Ensure that a durable queue exists from which messages may be consumed
        :param str queue: The name of the queue
        """
        self._channel.queue_declare(queue=queue, durable=True)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(consumer_callback=self._on_message,
                                    queue=self.__class__.QUEUE)

        self._channel.queue_declare(queue='notify_queue',
                                    durable=False)

    def set_on_message_callback(self, callback):
        """
        Registers the callback function to be invoked when messages are consumed
        from the queue.

        The callable provided accepts a single argument :param bytes body:

        :param callable callback: Callback function to apply to message body
        :return:
        """
        self._message_callback = callback

    def start_consuming(self):
        """
        Begin consuming messages from queue.
        :return:
        """
        self._channel.start_consuming()

    def stop_consuming(self):
        """
        Gracefully stop consuming messages from queue
        :return:
        """
        self._channel.stop_consuming()

    def close(self):
        self._connection.close()

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    def my_callback(body, q):
        LOGGER.info('Callback applied to %s', body)
        for _ in range(10):
            sleep(5)
            logging.info('callback is working...')
        q.put('I am done')
        logging.info('callback is DONE!')
        return

    client = MQClient()
    client.connect()
    client.set_on_message_callback(my_callback)
    try:
        client.start_consuming()
    except KeyboardInterrupt:
        client.stop_consuming()
    client.close()
