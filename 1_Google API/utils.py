##### Functions for google_bq_api.py ##### 
import boto3
import json
from sqlalchemy import create_engine
import pymongo


### Connect Mysql local database ###
def connect_DB(database='fircdp-dev'):

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('db_config')
    response = table.get_item(Key={'cdb_id': database})
    setting = response['Item']
    user = setting['user']
    passwd = setting['passwd']
    host = setting['host']
    db_name = setting['db_name']
    
    engine = create_engine(f'mysql+pymysql://{user}:{passwd}@{host}:3306/{db_name}?charset=utf8mb4', echo=False)
    con = engine.connect()

    return engine, con

### Connect local MongoDB ###
def connect_pymongo(database='fircdp-dev'):
    
    myclient = pymongo.MongoClient("mongodb+srv://root:XXXXXXX@cluster0.u6qj2.mongodb.net")
    mydb = myclient[database]

    return myclient, mydb

### Get customer id from aws s3 ###
def get_cf_id(cdb_id):
    
    client = boto3.client('s3')
    result = client.get_object(Bucket='clickforce', Key='click-force.json')
    setting = result["Body"].read().decode()
    setting = json.loads(setting)
    cf_id = setting[cdb_id]
    cf_id = cf_id.replace("-", "_")
    
    return cf_id
    


