import os


class EnvironmentVariable(object):
    AMQP_URL = os.environ.get('AMQP_URL')
