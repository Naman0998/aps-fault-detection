import pymongo
import pandas as pd
import json
client = pymongo.MongoClient("mongodb+srv://Cluster0:0001@cluster0.p4rrzqh.mongodb.net/test")

Data_File_Path = "/config/workspace/aps_failure_training_set1.csv"
DATABASE_NAME = "aps"
COLLECTION_NAME = "sensor"

if __name__ == "__main__":

    df = pd.read_csv(Data_File_Path)
    print(f"Rows and Columns:{df.shape}")

    #Convert Data Frame JSON so that we can dump these record in Mongo DB

    df.reset_index(drop=True,inplace=True)
    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

#insert converted data to Mongo db
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)


