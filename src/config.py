#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configuration

Loads from ENV vars or defaults to sane values for development purposes.
"""

from os import environ as env

RMQ_HOST = env.get('RMQ_HOST', '127.0.0.1')
RMQ_PORT = env.get('RMQ_PORT', '5672')

#
# https://mongodb.github.io/node-mongodb-native/driver-articles/mongoclient.html#the-url-connection-format
#
MONGO_HOST = env.get('MONGO_HOST', '127.0.0.1')
MONGO_PORT = env.get('MONGO_PORT', '27017')
MONGO_NAME = env.get('MONGO_NAME', 'app')
MONGO_USERNAME = env.get('MONGO_USERNAME', '')
MONGO_PASSWORD = env.get('MONGO_PASSWORD', '')

WORKER_NUM_PROCESSES = int(env.get('FO_WORKER_NUM_PROCESSES', '3'))
