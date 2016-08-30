#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configuration

Loads from ENV vars or defaults to sane values for development purposes.
"""

from os import environ as env

RABBIT_URL = env.get('RABBIT_URL','localhost')

MONGO_CON = env.get('MONGO_CON','mongodb://localhost:27017')
