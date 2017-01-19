#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mongoConnection import MongoConnection
import datetime as dt
from bson.objectid import ObjectId
import socket
import logging
import gridfs

"""
A class for MongoDB operations on
Jobs collection
"""
class JobDAO():
    def __init__(self):
        pass

    """
     Finds a job to check status of job
    """
    def find_job(self,objectId):       
        return MongoConnection.db.jobs.find_one({'_id': ObjectId(objectId)})

    def update_job_status_time(self,jobId,status,upsert):
        MongoConnection.db.jobs.update_one(
            {'_id': jobId },
            {
                "$set": {
                "status": status,
                'worker_start_time': dt.datetime.utcnow()
                }
            }
        ,upsert)

    def update_job_outputIds_invalid_val(self,jobId,status,idList,objValue,invalidsList,upsert):
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

    def update_job_problem_file(self, job_id,lp_filename):        
        fs =  gridfs.GridFS(MongoConnection.db)
        file_id = fs.put(open(lp_filename,'rb'))
        
        MongoConnection.db.jobs.update_one(
            {'_id':job_id},
            {
                "$set":{
                    'lp_problem_name':lp_filename,
                    'lp_problem_id':file_id
                     }
             }
            )
    def update_job_outputIds(self,jobId,status,idList,upsert):
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

    def update_jobobj_value(self,jobId,status,objValue,upsert):
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
        logging.info('Updated job with logging info.')

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
        logging.info('RECONCILE DB')

    def update_end(self, id):
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
        logging.info('updated failed time')

if __name__ == "__main__" :
    #This is a test
    MongoConnection.db.countries.insert({"name" : "India"})
    country = MongoConnection.db.countries.find_one()
    jobDAO = JobDAO()
    jobDAO.update_job_status_time(1,"running",True) 
    jobDAO.update_job_outputIds_invalid_val(1, "complete",[1234567,987654,123,456,9999],56,[1,2,3,4,5],True)
    jobDAO.update_jobobj_value(1,"complete",78,False)
    print(MongoConnection.db.jobs.find_one())
