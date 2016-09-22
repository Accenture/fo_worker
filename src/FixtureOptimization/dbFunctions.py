import pandas as pd
import pymongo as pm
import gridfs
import config

db = pm.MongoClient(config.MONGO_CON)['app']
fs = gridfs.GridFS(db)

def fetchArtifact(artifact_id):
    file = fs.get(ObjectId(artifact_id))
    file = pd.read_csv(file, header=None)
    return file

def create_output_artifact_from_dataframe(dataframe, *args, **kwargs):
    return fs.put(dataframe.to_csv().encode(), **kwargs)