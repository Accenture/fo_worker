#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configuration

Loads from ENV vars or defaults to sane values for development purposes.
"""

from os import environ as env

AMQP_URI = env.get('AMQP_URI','localhost')

MONGO_CON = env.get('MONGO_CON','mongodb://localhost:27017')
