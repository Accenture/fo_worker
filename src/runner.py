#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pymongo import MongoClient
import gridfs
from os import environ as env
from bson.objectid import ObjectId
import pandas as pd
import json
import datetime as dt
from brandExitConversion import brandExitMung
from preoptimizerR4 import preoptimize
from optimizerR4 import optimize

#
# ENV VARS
#

MONGO_HOST = env.get('MONGO_CONN', 'mongodb://localhost:27017')
MONGO_DB = env.get('MONGO_DB', 'app')

#
# LOGGING
#

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOG_FILE = None
LOGGER = logging.getLogger(__name__)


def run(body):

    db = MongoClient(MONGO_HOST)[MONGO_DB]
    fs = gridfs.GridFS(db)

    def fetch_artifact(artifact_id):
        file = fs.get(ObjectId(artifact_id))
        file = pd.read_csv(file, header=0)
        return file

    msg = json.loads(body.decode('utf-8'))
    # Find job to check status of job
    job = db.jobs.find_one({'_id': ObjectId(msg['_id'])})
    try:
        job_id = job['_id']
    except TypeError:
        print('Job Not Found')
        return False

    # current_user = job['userId']
    # job_status = job['status']
    worker_start_time = dt.datetime.today()
    db.jobs.update_one(
        {'_id': job['_id']},
        {
            "$set": {
                "status": "running",
                'worker_start_time': worker_start_time
            }
        }
    )

    fixture_artifact = fetch_artifact(msg["artifacts"]["spaceArtifactId"])
    transaction_artifact = fetch_artifact(msg["artifacts"]["salesArtifactId"])
    transaction_artifact = transaction_artifact.drop(
        transaction_artifact.index[[0]]).set_index("Store")
    fixture_artifact = fixture_artifact.drop(
        fixture_artifact.index[[0]]).set_index("Store")
    stores = fixture_artifact.index.values.astype(int)
    categories = fixture_artifact.columns[2:].values
    print("There are " + str(len(stores)) + " and " + str(
        len(categories)) + " Categories")
    print(msg['optimizationType'])
    try:
        future_space = fetch_artifact(
            msg["artifacts"]["futureSpaceId"]).set_index("Store")
        print("Future Space was Uploaded")
    except:
        future_space = None
        print("Future Space was not Uploaded")
    try:
        brand_exit_artifact = fetch_artifact(
            msg["artifacts"]["brandExitArtifactId"])
        print("Brand Exit was Uploaded")
        brand_exit_artifact = brandExitMung(brand_exit_artifact, stores,
                                            categories)
        print("Brand Exit Munged")
    except:
        print("Brand Exit was not Uploaded")
        brand_exit_artifact = None
    msg["optimizationType"] = 'traditional'
    if str(msg["optimizationType"]) == 'traditional':
        pre_opt = preoptimize(Stores=stores,
                              Categories=categories,
                              spaceData=fixture_artifact,
                              data=transaction_artifact,
                              mAdjustment=float(msg["metricAdjustment"]),
                              salesPenThreshold=float(
                                  msg["salesPenetrationThreshold"]),
                              optimizedMetrics=msg["optimizedMetrics"],
                              increment=msg["increment"],
                              brandExitArtifact=brand_exit_artifact,
                              newSpace=future_space)
        optimizationStatus = optimize(job_id,
                                      pre_opt,
                                      msg["tierCounts"],
                                      msg["spaceBounds"],
                                      msg["increment"],
                                      fixture_artifact,
                                      brand_exit_artifact)
    if msg["optimizationType"] == 'enhanced':
        print("Ken hasn't finished development for that yet")
        # set status to done
    db.jobs.update_one(
        {'_id': job['_id']},
        {
            "$set": {
                "status": "done"
            }
        }
    )

    res = dict(
        job_id=msg['_id'],
        user_id=msg['userId'],
        outcome='Success'
    )

    db.client.close()

    return json.dumps(res)


if __name__ == '__main__':
    LOGGER.debug('hello from {}'.format(__name__))
