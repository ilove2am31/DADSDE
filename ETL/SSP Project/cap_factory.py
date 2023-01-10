##### SSP(Smart Sale Promotion) data ETL for CAP factory #####
import numpy as np
import pandas as pd
import os
from google.cloud import bigquery as bq
from datetime import datetime
from datetime import date,timedelta
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

### Machine location data transformation function1 ###
def machine(df):
    if "locationA1" in df:
        return "A1"
    elif "locationI" in df:
        return "I"
    else:
        return np.nan 

### Machine location data transformation function2 ###    
def machine_ssp(df):
    if df == "17d5dffb":
        return "A1"
    elif df == "b97bc201":
        return "I"
    else:
        return np.nan

### Front page data transformation function ###
def hands(df):
    hand = df[0]
    event_name = df[1]
    if hand == "":
        return event_name
    elif pd.notnull(hand):
        return hand
    else:
        return event_name

### Buttom menu data transformation function ###
def buttom(df):
    event_name = df[0]
    item_name = df[1]
    web = df[2]
    if event_name == "點擊下方項目":
        if pd.isnull(item_name):
            if web.split("/")[-1] == "HomePage":
                return "首頁"
            elif web.split("/")[-1] == "MapArchive":
                return "地圖"
            elif web.split("/")[-1] == "Intro":
                return "歷史"
            elif web.split("/")[-1] == "CoursePage":
                return "課程"
            elif web.split("/")[-1] == "ShopPage":
                return "購物"
        else:
            return item_name
    else:
        return item_name

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
            try:
                temp["relation"] = temp["relation"].mode()[0]
            except:
                temp["relation"] = ""
        ssp_df = pd.concat([ssp_df, temp])
    return ssp_df



#%%
### Extract and transform data from google bigquery database ###
def bottle_ssp_trans(path_gtm, db, collection):   
    ## GA GTM data ##
    # GA GTM data from bigquery database #
    client = bq.Client()
    df = client.query(path_gtm).result().to_dataframe()
    
    # Date & time format transform #
    df["event_timestamp"] = pd.to_datetime(df["event_date"] + " " + df["event_timestamp"])
    df["event_date"] = pd.to_datetime(df["event_date"])
    
    # Flatten nested data & filter data from raw data #
    rows = df.copy()
    data = pd.DataFrame()
    for i in rows.index:
        event_params = list(rows["event_params"][i])
        df_temp = pd.DataFrame()
        for j in event_params:
            b = pd.DataFrame(j).dropna()
            df_temp = pd.concat([df_temp, b])
        df_temp = df_temp.set_index("key").T
        df_temp.index=[i]
        data = pd.concat([data, df_temp])
    
    params = ["ga_session_id", "項目名稱", "目前網站", "id", "課程名稱"]
    data = data[[i for i in data.columns if i in params]]
    for i in params:
        if i not in data.columns:
            data[i] = ''
    if len(data) == 0:
        data = pd.DataFrame(columns = ['目前網站', 'ga_session_id', '項目名稱', 'id', '課程名稱'])
    df = pd.concat([df.drop(["event_params"], axis=1), data], axis=1).rename(columns={"id": "group_id"})
    
    # Machine data transformation # 
    df["機台"] = df["目前網站"].apply(machine)
    df.dropna(subset=["機台"], inplace=True) 
    
    # Front page data transformation #
    if len(df) > 0:
        if '課程名稱' in df.columns:
            df["項目名稱"] = df[["課程名稱", "項目名稱"]].apply(hands, axis=1)
            df.drop(["課程名稱"], axis=1, inplace=True)
    
    # Buttom menu data transformation #
    if len(df) > 0:
        df["項目名稱"] = df[["event_name", "項目名稱", "目前網站"]].apply(buttom, axis=1)
    
    # Dwell Time #
    df.sort_values(["event_timestamp", "ga_session_id"], inplace=True)
    df["til_time"] = df.groupby(['event_date', 'ga_session_id'])['event_timestamp'].shift(-1)
    df["time_delta"] = df["til_time"] - df["event_timestamp"]
    df["time_delta"] = df["time_delta"].apply(lambda i: pd.Timedelta("1 min") if (i/ np.timedelta64(1,'m')>1) else i)
    
    
    ## SSP data ##
    # Achieve ssp data from local mongodb #
    db, con = connect_DB(db, collection)
    today_before = (date.today() - timedelta(days = 5)).strftime('%Y-%m-%d')
    today_after = (date.today() + timedelta(days = 5)).strftime('%Y-%m-%d')
    start_time = datetime.strptime(today_before + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(today_after + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = { "visittime": {"$gte": start_time, "$lte": end_time} }
    fields = {"relation", "group_id", "pcid"}
    cursor = db.find(query, fields)
    ssp = pd.DataFrame(list(cursor))
    con.close()
    
    # Machine data transformation #
    ssp["機台"] = ssp["pcid"].str.split("_").str[-1].apply(machine_ssp)
    ssp.drop(["_id", "pcid"], axis=1, inplace=True)
    
    # Relation data transformation #    
    ssp_df = relation(ssp)   
    ssp_df.drop_duplicates(keep="first", inplace=True)
    
    # Final integrate #
    df = pd.merge(df, ssp_df, how="left", on=["group_id", "機台"])
    df = df[['event_date', 'event_timestamp', 'event_name', '項目名稱', 'time_delta', 'group_id', 'relation', '機台']]
    df.columns = ['event_date', 'event_timestamp', 'event_name', 'item_name', 'stay_time', 'group_id', 'relation', 'location']
    df["stay_time"] = df["stay_time"].astype(str)
    
    return df



#%%
### Scheduling data transform ###
def run_bottle_ssp_trans(db="bottle_factory", collection="detect_record"):
    
    ## ssp ##
    # update ssp relation data #
    db_col, con = connect_DB("bottle_factory", "detect_record")
    nowadays = db_col.find_one(sort=[("visittime", -1)])["visittime"]
    start_date = datetime.strptime(str(nowadays)[:10] + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(str(nowadays)[:10] + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = { "visittime": {"$gte": start_date,"$lte": end_date} }
    fields = {"relation", "group_id"}
    cursor = db_col.find(query, fields)
    ssp_dfs = pd.DataFrame(list(cursor)).drop(columns=["_id"])
    ssp_dfs = relation(ssp_dfs)   
    ssp_dfs.drop_duplicates(inplace=True)
    ssp_dfs.columns = ["group_id", "relation_group"]

    for i, j in zip(ssp_dfs["group_id"], ssp_dfs["relation_group"]):
        db_col.update_many({"group_id": i}, {"$set": {"relation_group": j}})
    con.close()
    
    ## GA ##
    # API for google bigquery & local mongodb #
    client = boto3.client('s3')
    result = client.get_object(Bucket='remaga', Key='cap-factory.json')
    credential = result["Body"].read().decode()
    with open("/tmp/cap-factory.json", 'w') as out_file:
        for line in credential:
            out_file.write(line)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] ="/tmp/cap-factory.json"
    
    # Check whether new data exit #
    db_col, con = connect_DB("bottle_factory", "GA_record")
    today=db_col.find_one(sort=[("event_date", -1)])["event_date"]
    today=(today+timedelta(days=1)).strftime('%Y%m%d')
    
    query = f"""
    SELECT * FROM `analytics_315551813.events_{today}` limit 1  
    """
    client = bq.Client()
    try:
        client.query(query).result()
    except:
        return("skip")

    # data transform & load data to local mongodb #
    path_gtm = f""" SELECT event_date, FORMAT_TIME('%T', TIME(TIMESTAMP_MICROS(event_timestamp))) as event_timestamp,
    event_name, event_bundle_sequence_id, event_params
    FROM analytics_315551813.events_{today}
    Where event_name In ("點擊下方項目", "點擊歷史事件圈圈", "點擊歷史事件圈圈", "點擊課程更多資訊", "點擊A1地圖項目", "點擊地圖項目", "點擊購物更多資訊"); """   
    
    df = bottle_ssp_trans(path_gtm, db, collection)
    dict_df = df.to_dict("records")
    db_col, con = connect_DB(db, "GA_record")
    if len(dict_df) != 0:
        db_col.insert_many(dict_df) 
    con.close()
    
    return "success"


### Run code from aws lambda function ###
def lambda_handler(event, context):
    ans = run_bottle_ssp_trans(db="bottle_factory", collection="detect_record")
    
    return ans

