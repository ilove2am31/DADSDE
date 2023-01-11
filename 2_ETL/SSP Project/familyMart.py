##### SSP(Smart Sale Promotion) data ETL for familymart #####
import numpy as np
import pandas as pd
import os
from google.cloud import bigquery as bq
from datetime import datetime
from datetime import date, timedelta
from pymongo import MongoClient
import boto3


### Connect local mongo database ###
def connect_DB(db,name):
    client = MongoClient(host = "ec2-XXX.ap-northeast-1.compute.amazonaws.com",
                         port = 27017, 
                         username = "username", 
                         password = "password",
                         authSource = "admin") 
    db = client[db]
    db_col = db[name]
    return db_col, client

### Customer relation data transformation function ###
def relation(df):
    ssp_df = pd.DataFrame(columns = df.columns)
    for i in df["group_id"].unique():
        temp = df[df["group_id"] == i]
        if temp["relation"].isin(['家庭']).any():
            temp["relation"] = "家庭"
        elif temp["relation"].isin(['情侶']).any():
            temp["relation"] = "情侶"
        elif temp["relation"].isin(['朋友']).any():
            temp["relation"] = "朋友"
        else:
            temp["relation"] = temp["relation"].mode()[0]
        ssp_df = pd.concat([ssp_df, temp])
    return ssp_df

### Google BQ event_params.value data transform ###
def value(df):
    if pd.notnull(df[0]):
        return df[0]
    elif pd.notnull(df[1]):
        return df[1]
    elif pd.notnull(df[2]):
        return df[2]
    elif pd.notnull(df[3]):
        return df[3]
    else:
        return np.nan



#%%
### Extract and transform data from google bigquery database ###
def familymart_ssp_trans(path_gtm):   
    ## GA GTM data ##
    # GA GTM data from bigquery database #
    client = bq.Client()
    df = client.query(path_gtm).result().to_dataframe()   
    
    # Date & time format transform #
    df["event_timestamp"] = pd.to_datetime(df["event_date"] + " " + df["event_timestamp"])
    df["event_date"] = pd.to_datetime(df["event_date"])
    
    # Flatten nested data & filter data from raw data #
    data = pd.DataFrame()
    for i in df.index:
        for j in range(0, len(df["event_params"].iloc[i])):
            df_temp = pd.concat([pd.DataFrame(df["event_params"].iloc[i][j]["key"], index=[i], columns=["params"]),
                                 pd.DataFrame(df["event_params"].iloc[i][j]["value"], index=[i])], axis=1)
            data = pd.concat([data, df_temp])
    params = ["ga_session_id", "item_name", "page_url", "id", "brand_name"]
    data = data[data["params"].isin(params)]
    data["value"] = data.drop(["params"], axis=1).apply(value, axis=1)
    pivot_data = data[["params", "value"]].pivot(columns='params', values="value")
    if len(pivot_data) == 0:
        pivot_data = pd.DataFrame(columns = ['ga_session_id', 'item_name', 'page_url', 'id', "brand_name"])
    else:
        for i in params:
            if i not in pivot_data.columns:
                pivot_data[i] = ""
    df = df.join(pivot_data).drop(["event_params"], axis=1).rename(columns={"id": "group_id"}).dropna(subset=["item_name"])
    
    # Dwell Time #
    df.sort_values(["event_timestamp", "ga_session_id"], inplace=True)
    df["til_time"] = df.groupby(['event_date', 'ga_session_id'])['event_timestamp'].shift(-1)
    df["stay_time"] = df["til_time"] - df["event_timestamp"]
    df["stay_time"] = df["stay_time"].apply(lambda i: pd.Timedelta("30 sec") if (i/ np.timedelta64(30,'s')>1) else i).astype(str)
    
    ## SSP data ##
    # Achieve ssp data from local mongodb #
    mycol, con = connect_DB("family_mart", "detect_record")
    today_before = (date.today() - timedelta(days = 5)).strftime('%Y-%m-%d')
    today_after = (date.today() + timedelta(days = 5)).strftime('%Y-%m-%d')
    start_time = datetime.strptime(today_before + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(today_after + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = { "visittime": {"$gte": start_time, "$lte": end_time} }
    fields = {"relation", "group_id"}
    cursor = mycol.find(query, fields)
    ssp = pd.DataFrame(list(cursor)).drop(["_id"], axis=1)
    con.close()
    
    # Machine data transformation #
    ssp_df = relation(ssp).drop_duplicates()
    
    # Final integrate #
    df = pd.merge(df, ssp_df, how="left", on=["group_id"])
    df["group_id"] = df["group_id"].replace("", np.nan)
    df = df[['event_date', 'event_timestamp', 'event_name', 'item_name', 'stay_time', 'group_id', 'relation', 'brand_name']]
    
    return df


#%%
### Scheduling data transform ###
def run_familymart_ssp_trans():
    
    ## ssp ##
    # update ssp relation data #
    mycol, con = connect_DB("family_mart", "detect_record")
    query = { "relation_group": {"$eq": None} }
    fields = {"relation", "group_id"}
    cursor = mycol.find(query, fields)
    ssp_dfs = pd.DataFrame(list(cursor))
    if ssp_dfs.empty == False:
        ssp_dfs.drop(columns=["_id"], inplace=True)
        ssp_dfs = relation(ssp_dfs)   
        ssp_dfs.drop_duplicates(inplace=True)
        ssp_dfs.columns = ["group_id", "relation_group"]

        for i, j in zip(ssp_dfs["group_id"], ssp_dfs["relation_group"]):
            mycol.update_many({"group_id": i}, {"$set": {"relation_group": j}})
    con.close()
    
    ## GA ##
    # API for google bigquery & local mongodb #
    client = boto3.client('s3')
    result = client.get_object(Bucket='remaga', Key='familymart_BQ.json')
    credential = result["Body"].read().decode()
    with open("/tmp/familymart_BQ.json", 'w') as out_file:
        for line in credential:
            out_file.write(line)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/tmp/familymart_BQ.json"
    
    # Check whether new data exit #
    mycol, con = connect_DB("family_mart", "GA_record")
    today = mycol.find_one(sort=[("event_date", -1)])["event_date"]
    today = (today+timedelta(days=1)).strftime('%Y%m%d')
    
    query = f"""
    SELECT * FROM `analytics_337678570.events_{today}` limit 1  
    """
    client = bq.Client()
    try:
        client.query(query).result()
    except:
        return("skip")
    
    # data transform & load data to local mongodb #
    path_gtm = f""" SELECT event_date, FORMAT_TIME('%T', TIME(TIMESTAMP_MICROS(event_timestamp))) as event_timestamp,
                    event_name, event_bundle_sequence_id, event_params
                    FROM analytics_337678570.events_{today}
                    Where event_name Like 'select%%'; """   
    
    df = familymart_ssp_trans(path_gtm)
    dict_df = df.to_dict("records")
    mycol, con = connect_DB("family_mart", "GA_record")
    if len(dict_df) != 0:
        mycol.insert_many(dict_df)
    con.close()
    
    return "success"


### Run code from aws lambda function ###
def lambda_handler(event, context):
    ans = run_familymart_ssp_trans()
    
    return ans

    
