from google.cloud import bigquery as bq
import os
from datetime import datetime,timedelta
import boto3
import json
import pandas as pd
from lib.utils import connect_DB,get_cf_id
import numpy as np



### Set AWSDEFAULT_REGION ###
os.environ['AWS_DEFAULT_REGION'] = 'ap-northeast-1'


### Value function for ga_etl_task_to_cdp fuction ###
def values(df):
    v1 = df[0]
    v2 = df[1]
    v3 = df[2]
    v4 = df[3]
    if pd.isnull(v1) == False:
        return v1
    elif pd.isnull(v2) == False:
        return v2
    elif pd.isnull(v3) == False:
        return v3
    elif pd.isnull(v4) == False:
        return v4


### Run etl task: from GA BigQuery to MySQL ###
def ga_etl_task_to_cdp(cdb_id):
    
    ## Get ga_id from aws s3 ##
    client = boto3.client("s3")
    result = client.get_object(Bucket='rema-ga-list', Key='ga.json')
    setting = result["Body"].read().decode()
    setting = json.loads(setting)
    ga_id = setting[cdb_id]

    ## Set GA BigQuery API to get GA data ##
    result = client.get_object(Bucket='remaga', Key=f'{cdb_id}.json')
    credential = result["Body"].read().decode()
    with open("/tmp/"+cdb_id+".json", 'w') as out_file:
        for line in credential:
            out_file.write(line)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ="/tmp/"+cdb_id+".json"
    
    cf_id = get_cf_id(cdb_id)
    engine,con  = connect_DB("clickforce")
    path = f'''SELECT date(max(event_timestamp)) FROM {cf_id}.behavior_laurel;'''
    cursor = con.execute(path)
    max_date = cursor.fetchone()[0]
    client = bq.Client()
    
    ## Try if GA data exits ##
    time = str(max_date+timedelta(days=1)).replace("-","")
    query = f"""
    SELECT * FROM `analytics_{ga_id}.events_{time}` limit 1  
    """
    try:
        query_job = client.query(query) 
        row = query_job.result().to_dataframe()
        print(row)
    except:
        return("skip")

    
    ## Data transform ##
    query = f"""
    SELECT event_name,event_timestamp,event_params,device.category,user_pseudo_id cid ,
    traffic_source.name campaign,traffic_source.medium medium,traffic_source.source,
    geo.country country, geo.region region
    FROM `analytics_{ga_id}.events_{time}`  
    """
    query_job = client.query(query) 

    rows = query_job.result().to_dataframe()
    rows["event_timestamp"]=[str(datetime.fromtimestamp(int(x/1000000))) for x in rows["event_timestamp"]]
    
    col = ["ga_session_id","ga_session_number","register_page","account_email","name",'phone',"method","account","product_name","product_price","menu_name","social",
           "special","banner","product_share","label","keywords","item","tel","add1","add2","add3","gender","birthday",  
           "jobtype","jobtitle","marital","chidren","refer_url","url","refer_path","current_page"]

    c = rows.explode(["event_params"])
    c = c[c["event_params"].apply(lambda i: i["key"]).isin(col)]
    c[['key', 'value']] = c['event_params'].apply(pd.Series)
    c[['v1', 'v2', 'v3', 'v4']] = c['value'].apply(pd.Series)
    c['values'] = c[['v1', 'v2', 'v3', 'v4']].apply(values, axis=1)
    c.drop(columns=['value', 'v1', 'v2', 'v3', 'v4', 'event_params'], inplace=True)

    d = c[['key', 'values']].reset_index().set_index(['index', 'key']).squeeze().unstack()
    w = [i for i in col if i not in d.columns]
    for x in w:
        d[x] = np.nan
    d = d[col]

    c = c.drop(['key', 'values'], axis=1).merge(d, left_index=True, right_index=True)
    c = c.drop_duplicates()

    ## Data to lacal mysql lacal database ## 
    engine,con  = connect_DB("clickforce")
    c.to_sql(name = "behavior_laurel",
             chunksize = 10000,
             schema = cf_id,
             con = con,
             index = False,
             if_exists = "append")
    con.close()
    engine.dispose()

    return("success")
  


