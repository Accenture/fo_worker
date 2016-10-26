#!/usr/bin/env python
# -*- coding: utf-8 -*-


import datetime as dt
from pulp import *
import pandas as pd
# from FixtureOptimization.CurveFitting import curveFittingBS
from FixtureOptimization.ksMerging import ksMerge
from FixtureOptimization.preoptimizerEnh import preoptimizeEnh
from FixtureOptimization.optimizerR5 import optimize
from FixtureOptimization.optimizer2 import optimize2
from FixtureOptimization.outputFunctions import createLong, createWide, createDrillDownSummary, createTieredSummary, \
    outputValidation
from FixtureOptimization.SingleStoreOptimization import optimizeSingleStore
import logging

LOG_FORMAT = ('%(levelname) -7s %(asctime) -25s Filename: %(filename) -20s'
              ' MFL: %(module)s:%(funcName)s:%(lineno) -20s %(message)s')
DATE_FORMAT = '%m/%d/%Y %I:%M:%S %p'
LOG_FILE = 'mylogs.log'
LOGGER = logging.getLogger(__name__)
logging.basicConfig(filename=LOG_FILE,
                    level=logging.INFO,
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT)


def run(msg):
    def create_output_artifact_from_dataframe(dataframe, *args, **kwargs):
        """
        Returns: file to csv inside detached_worker_output directory

        """
        return dataframe.to_csv('output.csv', index=False)

    def fetchTransactions(artifact_id):
        '''
        Fetch the artifact from the json and read it in as a csv
        :param artifact_id: The id of the artifact
        :return: return the file as a csv
        '''
        # file = fs.get(ObjectId(artifact_id))
        file = pd.read_csv(artifact_id, header=None)
        return file

    def fetchSpace(artifact_id):
        '''
        Fetch the artifact from the json and read it in as a csv
        :param artifact_id: The id of the artifact
        :return: return the file as a csv
        '''
        # file = fs.get(ObjectId(artifact_id))
        file = pd.read_csv(artifact_id, header=0, dtype={'Store': object}, skiprows=[1])
        return file

    def fetchExit(artifact_id):
        '''
        Fetch the artifact from the json and read it in as a csv
        :param artifact_id: The id of the artifact
        :return: return the file as a csv
        '''
        # file = fs.get(ObjectId(artifact_id))
        file = pd.read_csv(artifact_id, header=0, skiprows=[1])
        return file

    print(LOGGER.info('beginning of ' + msg['meta']['name']))

    try:
        futureSpace = fetchSpace(msg["artifacts"]["futureSpaceId"])
        print("Future Space was Uploaded")
    except:
        futureSpace = None
        print("Future Space was not Uploaded")
    try:
        brandExitArtifact = fetchExit(msg["artifacts"]["brandExitArtifactId"])
        print("Brand Exit was Uploaded")
    except:
        print("Brand Exit was not Uploaded")
        brandExitArtifact = None

    dataMerged = ksMerge(msg['jobType'], fetchTransactions(msg["artifacts"]["salesArtifactId"]),
                         fetchSpace(msg["artifacts"]["spaceArtifactId"]),
                         brandExitArtifact, futureSpace)
    print('finished data merging')
    print(msg['optimizationType'])
    preOpt = preoptimizeEnh(optimizationType=msg['optimizationType'], dataMunged=dataMerged[1],
                            mAdjustment=float(msg["metricAdjustment"]),
                            salesPenThreshold=float(msg["salesPenetrationThreshold"]),
                            optimizedMetrics=msg["optimizedMetrics"], increment=msg["increment"])
    print('finished preoptimize')

    if msg['optimizationType'] == 'traditional':
        print('finished preoptimize')
        print('going to the optimization')
        optimRes = optimize(jobName=msg['meta']['name'], Stores=msg['salesStores'], Categories=msg['salesCategories'],
                            tierCounts=msg['tierCounts'], spaceBound=msg['spaceBounds'], increment=msg['increment'],
                            dataMunged=preOpt)
        cfbsArtifact = [None, None]
    else:
        print('curveFittingBS will not work in debugger')
        cfbsArtifact = curveFittingBS(dataMerged[0], msg['spaceBounds'], msg['increment'],
                                      msg['storeCategoryBounds'],
                                      float(msg["salesPenetrationThreshold"]), msg['jobType'],
                                      msg['optimizationType'])
        print('finished curve fitting')
        cfbsOptimal = optimizeSingleStore(cfbsArtifact[0].set_index(['Store', 'Category']), msg['increment'],
                                          msg['optimizedMetrics'])
        # preOpt = optimizeSingleStore(cfbsArtifact[0],msg['increment'],msg['optimizerMetrics'])
        print(msg['optimizationType'])
        if msg['jobType'] == 'tiered':
            optimRes = optimize2(methodology=msg['optimizationType'], jobName=msg['meta']['name'],
                                 Stores=msg['salesStores'], Categories=msg['salesCategories'],
                                 tierCounts=msg['tierCounts'],
                                 increment=msg['increment'], weights=msg['optimizedMetrics'], cfbsOutput=cfbsOptimal[1],
                                 preOpt=preOpt, salesPen=msg['salesPenetrationThreshold'])
            # optimRes = optimize3(jobName=msg['meta']['name'], Stores=msg['salesStores'],
            #                     Categories=msg['salesCategories'],
            #                     tierCounts=msg['tierCounts'], spaceBound=msg['spaceBounds'], increment=msg['increment'],
            #                     dataMunged=optimRes)
        else:
            try:
                ddRes = drillDownOptim()
            except:
                print("We aren't ready for Drill Down")
        print('New optimization completed')
    if msg['optimizationType'] == 'drillDown':
        cfbsOptimal = optimizeSingleStore(cfbsArtifact[0], msg['increment'], msg['optimizedMetrics'])

    # Call functions to create output information
    print('Out of the optimization')
    longOutput = createLong(msg['jobType'], msg['optimizationType'], optimRes[1])
    print('Created Long Output')
    wideID = str(create_output_artifact_from_dataframe(createWide(longOutput, msg['jobType'], msg['optimizationType'])))
    print('Created Wide Output')

    if cfbsArtifact[1] is not None:
        longID = str(create_output_artifact_from_dataframe(longOutput))
        analyticsID = str(create_output_artifact_from_dataframe(cfbsArtifact[1]))
        print('Created analytics ID')
    else:
        longID = str(create_output_artifact_from_dataframe(
            longOutput[['Store', 'Category', 'Climate', 'VSG', 'Sales Penetration', 'Result Space', 'Current Space',
                        'Optimal Space']]))
        analyticsID = None
        print('Set analytics ID to None')

    statusID = optimRes[0]
    print('Set the Status')

    if msg['jobType'] == "tiered":
        summaryID = str(create_output_artifact_from_dataframe(createTieredSummary(longOutput)))
    else:  # since type == "Drill Down"
        summaryID = str(create_output_artifact_from_dataframe(createDrillDownSummary(longOutput)))

    LOGGER.info('end of ' + msg['meta']['name'])

    print("Job complete")


if __name__ == '__main__':
    from job_context import job_context

    payload = job_context()
    run(payload)
