#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mongoConnection import MongoConnection
import datetime as dt
from bson.objectid import ObjectId
import socket

class JobDAO():
    """
    Created on Sat Dec 31 14:15:51 2016 ISt
    A class for MongoDB operations on
    Jobs collection
    @author: omkar.marathe
    """

    def __init__(self):
        pass

    def findJob(self,objectId):
        # Find job to check status of job
        return MongoConnection.db.jobs.find_one({'_id': ObjectId(objectId)})

    def updateJobStatusTime(self,jobId,status,upsert):
        MongoConnection.db.jobs.update_one(
            {'_id': jobId },
            {
                "$set": {
                "status": status,
                'worker_start_time': dt.datetime.utcnow()
                }
            }
        ,upsert)

    def updateJobOutputIdsInvalidVal(self,jobId,status,idList,objValue,invalidsList,upsert):
        MongoConnection.db.jobs.update_one(
                {'_id': jobId},
                {
                    "$set": {
                        'optimization_end_time': dt.datetime.utcnow() ,
                        "status": status,
                        "objectiveValue": objValue,
                        "artifactResults": {
                            'long_table': idList[0],
                            'wide_table': idList[1],
                            'summary_report': idList[2],
                            'analytic_data': idList[3],
                            'master_data': idList[4]
                        },
                        "outputErrors": {
                            'invalidValues': invalidsList[0],
                            'invalidTierCounts': invalidsList[1],
                            'invalidBrandExit': invalidsList[2],
                            'invalidSalesPenetration': invalidsList[3],
                            'invalidBalanceBack': invalidsList[4]
                        }
                    }
                }
            ,upsert)

    def updateJobOutputIds(self,jobId,status,idList,upsert):
            MongoConnection.db.jobs.update_one(
            {'_id': jobId},
            {
                "$set": {
                    'optimization_end_time':dt.datetime.utcnow() ,
                    "status": status ,
                    "artifactResults": {
                        'long_table': idList[0],
                        'wide_table': idList[1],
                        'summary_report': idList[2],
                        'analytic_data': idList[3]
                    }
                }
            }
        ,upsert)

    def updateJobObjValue(self,jobId,status,objValue,upsert):
            MongoConnection.db.jobs.update_one(
                {'_id': jobId},
                {
                    "$set": {
                        'optimization_end_time': dt.datetime.utcnow(),
                        "status": status,
                        "objectiveValue": objValue 
                    }
                }
            ,upsert)
 
    def logging_db(self,id):
        MongoConnection.db.jobs.update_one(
            {'_id': ObjectId(id)},
            {
                "$set": {
                    "logging": {
                        "server": socket.gethostname(),
                        "pid": getpid()
                    }
                }
            }
        )
        print('Updated job with logging info.')

    #use for divideByZero as well by passing status
    def reconcile_db(self, id,status):
        MongoConnection.db.jobs.update_one(
            {'_id': ObjectId(id)},
            {
                "$set": {
                    "status": "failed",
                    "logging": {
                        "server": socket.gethostname(),
                        "pid": getpid()
                    }
                }
            }
        )
        print('RECONCILE DB')

    def updateEnd(self, id):
        MongoConnection.db.jobs.update_one(
            {'_id': ObjectId(id)},
            {
                "$set": {
                    'optimization_end_time': dt.datetime.utcnow(),
                    "logging": {
                        "server": socket.gethostname(),
                        "pid": getpid()
                    }
                }
            }
        )
        print('update failed time')

if __name__ == "__main__" :
    MongoConnection.db.countries.insert({"name" : "India"})
    country = MongoConnection.db.countries.find_one()
    jobDAO = JobDAO()
    jobDAO.updateJobStatusTime(1,"running",True) 
    jobDAO.updateJobOutputIdsInvalidVal(1, "complete",[1234567,987654,123,456,9999],56,[1,2,3,4,5],True)
    jobDAO.updateJobObjValue(1,"complete",78,False)
    print(MongoConnection.db.jobs.find_one())
