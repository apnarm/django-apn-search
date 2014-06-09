#!/usr/bin/env python

import logging
import os
import time

from requests.exceptions import HTTPError, ConnectionError, Timeout

try:
    import cPickle as pickle
except ImportError:
    import pickle

from django.conf import settings
from django.db import connections, DatabaseError

from mq.daemon import ConsumerDaemon

from apn_search.update import update_object


class MessageHandler(object):

    RetryExceptions = (HTTPError, ConnectionError, Timeout, DatabaseError)

    def on_error(self, error):
        if isinstance(error, DatabaseError):
            for connection in connections.all():
                try:
                    connection.close()
                except Exception:
                    connection.connection = None

    def process_message(self, message_body, message_id, queue):
        """Process a queued message and update the search index."""

        # Unpack the message.
        try:
            message_body = pickle.loads(message_body)
            identifier = message_body['identifier']
            remove = message_body.get('remove')
        except Exception, error:
            # There was a major problem with this message. Accept the message
            # since it's not likely to be a service availability problem.
            logging.error('Invalid message: %s' % error)
            queue.ack(message_id)
            return

        def update_search_index():
            update_object(identifier, remove=remove, exception_handling=False)

        try:

            retry = False

            try:
                update_search_index()
            except self.RetryExceptions as error:
                retry = True
                logging.warning('Problem while processing %r: %s' % (identifier, error))
                self.on_error(error)
                time.sleep(1)
                update_search_index()

            queue.ack(message_id)

        except Exception as error:

            if retry:

                # There was still an error after retrying. This is likely to be
                # a service availability issue, so log a critical error message
                # and DON'T accept the message. It will try again next time
                # this script runs / the daemon is restarted.
                logging.critical('Could not process %r: %s' % (identifier, error))
                logging.error('Not accepting message for %r' % identifier)

            else:

                # All hell has broken loose and it's probably a code problem.
                # It's probably not a service availability issue, so only log
                # an error level message, and accept the message so it doesn't
                # clog up the queue.
                logging.error('Unhandled error while processing %r: %s' % (identifier, error))
                queue.ack(message_id)


def start_daemon(message_queue, queue_name=settings.APN_SEARCH_QUEUE, handler_class=MessageHandler):
    logging.info('Starting search update consumer daemon using %s.' % message_queue)
    consumer = ConsumerDaemon(
        message_queue=message_queue,
        queue_name=queue_name,
        message_handler=handler_class().process_message,
        pid_file_name=os.path.join(
            settings.DAEMON_PID_PATH,
            'consume_search_updates.pid'
        ),
    )
    consumer.start()


def start_cron(message_queue, queue_name=settings.APN_SEARCH_QUEUE, handler_class=MessageHandler):
    """Consume and process all search updates and then quit."""
    logging.info('Starting search update script.')
    message_handler = handler_class().process_message
    with message_queue.open(queue_name) as queue:
        for message_body, message_id in queue:
            message_handler(message_body, message_id, queue)
