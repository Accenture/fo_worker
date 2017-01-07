#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient
import config as env
from distutils.util import strtobool

class MongoConnection():
    """
    Created on Sat Dec 31 14:00:51 2016 IST

    @author: omkar.marathe
    """

    db= MongoClient(host=env.MONGO_HOST,port=env.MONGO_PORT)[env.MONGO_NAME]
    if strtobool(env.IS_AUTH_MONGO):
        current_db.authenticate(env.MONGO_USERNAME, env.MONGO_PASSWORD, mechanism='SCRAM-SHA-1')
