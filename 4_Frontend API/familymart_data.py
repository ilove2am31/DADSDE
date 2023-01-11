from fastapi import Request, APIRouter, Header, HTTPException, Depends
from typing import Union
from lib.utils import connect_DB
from bson.objectid import ObjectId
from routers.auth.auth import verify_token
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

### Set APIRouter ###
router = APIRouter(prefix="/api/familymart/data")

### Age data transform to range function ###
def age_range(age):
    age = age
    if 0 < age <= 25:
        return "25歲以下"
    elif 25 < age <= 35:
        return "26-35歲"
    elif 35 < age <= 45:
        return "36-45歲"
    else:
        return "46歲以上"



#%%
##### API for frontend #####
### 機台前人、群數、停留時間 ###
@router.get("/info",dependencies=[Depends(verify_token)])
async def info_api(request: Request, first_date:str, last_date:str):
    # Mongodb connect and extract data #
    mycol, con =  connect_DB("family_mart", "detect_record")
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = {"visittime": {"$gte": start_date, "$lte": end_date}, "pcid": {"$eq": "114.32.43.223_fff79019"}}
    fields = {"staytime", "group_id"}
    cursor = mycol.find(query, fields)
    ssp = pd.DataFrame(list(cursor))
    
    start_date1 = start_date - (end_date - start_date) + timedelta(days=-1)
    query1 = {"visittime": {"$gte": start_date1, "$lte": start_date}}
    cursor = mycol.find(query1, fields)
    ssp_before = pd.DataFrame(list(cursor))
    con.close()
    
    mycol, con =  connect_DB("family_mart", "GA_record")
    query2 = {"event_timestamp": {"$gte": start_date, "$lte": end_date},
              "event_name": {"$eq": "select_homepage"}}
    fields2 = {"item_name"}
    cursor = mycol.find(query2, fields2)
    df = pd.DataFrame(list(cursor)).rename(columns={"stay_time": "staytime"})
    
    query3 = {"event_timestamp": {"$gte": start_date1, "$lte": start_date},
              "event_name": {"$eq": "select_homepage"}}
    cursor = mycol.find(query3, fields2)
    df_before = pd.DataFrame(list(cursor)).rename(columns={"stay_time": "staytime"})
    con.close()
    
    # Data cleaning & transform #
    for i in [ssp, ssp_before]:
        if i.empty == False:
            i["staytime"] = i["staytime"].str.split(" ").str[0].astype(float)
    try:
        grow_view = round((len(ssp) - len(ssp_before))*100 / len(ssp_before), 0)
        grow_group = round((ssp["group_id"].nunique() - ssp_before["group_id"].nunique())*100 / ssp_before["group_id"].nunique(), 0)
        grow_stay = round((ssp["staytime"].mean() - ssp_before["staytime"].mean())*100 / ssp_before["staytime"].mean(), 0)
        grow_front = round((len(df) - len(df_before))*100 / len(df_before), 0)
    except:
        grow_view = None
        grow_group = None
        grow_stay = None
        grow_front = None
    
    # Transform dataframe tpye to json type #
    out_dict = {"機台閱覽人數": {"值": len(ssp), "成長率": grow_view}, 
                "機台閱覽群群": {"值": ssp["group_id"].nunique(), "成長率": grow_group},
                "平均機台停留時間": {"值": round(ssp["staytime"].mean(), 2), "成長率": grow_stay},
                "機台點擊首頁人數": {"值": len(df), "成長率": grow_front}}
    
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

##### 每日機台 人數/停留時間 #####
@router.get("/timeline",dependencies=[Depends(verify_token)])
async def itimeline_api(request: Request, first_date:str, last_date:str):
    mycol, con =  connect_DB("family_mart", "detect_record")
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = {"visittime": {"$gte": start_date, "$lte": end_date}, "pcid": {"$eq": "114.32.43.223_ff79019"}}
    fields = {"visittime", "staytime", "gender"}
    cursor = mycol.find(query, fields)
    ssp = pd.DataFrame(list(cursor))
    con.close()
    
    ssp["staytime"] = ssp["staytime"].str.split(" ").str[0].astype(float)
    ssp["visittime"] = ssp["visittime"].dt.date
    
    ssp_gb = ssp.groupby(["visittime", "gender"])["staytime"].agg(["count", "mean"]).reset_index()
    ssp_gb_all = ssp.groupby(["visittime"])["staytime"].agg(["count", "mean"]).reset_index()
    ssp_gb_all["gender"] = "全"
    ssp_gb = pd.concat([ssp_gb, ssp_gb_all]).rename(columns={"visittime":"日期", "count":"人數", "mean":"停留時間"})
    ssp_gb["停留時間"] = ssp_gb["停留時間"].round(2)
    
    out_dict = ssp_gb.groupby(["日期", "gender"])["人數", "停留時間"].apply(lambda i: i.to_dict("list")).reset_index(level='gender', name="list")
    out_dict = out_dict.groupby(["日期"])["gender", "list"].apply(lambda i: dict(zip(i["gender"], i["list"]))).to_dict()
    
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

##### 顧客屬性占比 #####
@router.get("/relation",dependencies=[Depends(verify_token)])
async def relation_api(request: Request, first_date:str, last_date:str):
    mycol, con =  connect_DB("family_mart", "detect_record")
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = {"visittime": {"$gte": start_date, "$lte": end_date}, "pcid": {"$eq": "114.32.43.223_fff79019"}}
    fields = {"relation_group", "group_id"}
    cursor = mycol.find(query, fields)
    ssp = pd.DataFrame(list(cursor)).drop(["_id"], axis=1).drop_duplicates()
    con.close()
    
    ssp = ssp[ssp["relation_group"].isin(["male", "female", "friend", "couple", "family"])]
    ssp_df_gb = (ssp["relation_group"].value_counts(normalize=True)*100).round(2).reset_index()
    ssp_df_gb.columns = ["relation_group", "屬性占比"]
    for i in ["male", "female", "friend", "couple", "family"]:
        if i not in ssp_df_gb["relation_group"].unique():
            temp = pd.DataFrame([[i, 0]], columns=["relation_group", "屬性占比"])
            ssp_df_gb = pd.concat([ssp_df_gb, temp])
    
    out_dict = ssp_df_gb.to_dict("list")
    
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response
    
##### 訪客年齡層性別占比 #####
@router.get("/age_gender",dependencies=[Depends(verify_token)])
async def age_gender_api(request: Request, first_date:str, last_date:str):
    mycol, con =  connect_DB("family_mart", "detect_record")
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = {"visittime": {"$gte": start_date, "$lte": end_date}, "pcid": {"$eq": "114.32.43.223_fff79019"}}
    fields = {"gender", "age"}
    cursor = mycol.find(query, fields)
    ssp = pd.DataFrame(list(cursor)).drop(["_id"], axis=1)
    con.close()
    
    ssp["age_range"] = ssp["age"].str.replace("歲", "").astype(int).apply(age_range)
    
    ssp_gb = ssp.groupby(["gender", "age_range"]).count().reset_index().rename(columns={'age': 'counts'})
    temp = pd.DataFrame()
    for i in ["男", "女"]:
        for j in ["25歲以下", "26-35歲", "36-45歲", "46歲以上"]:
           temp1 =  pd.DataFrame([[i,j]], columns=["gender", "age_range"])
           temp = pd.concat([temp, temp1])
    ssp_gb = temp.merge(ssp_gb, on=["gender", "age_range"], how="left").fillna(0)
    
    out_dict = ssp_gb.to_dict("list")
    
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

##### 場域點擊次數及停留時間 #####
@router.get("/scatter",dependencies=[Depends(verify_token)])
async def scatter_api(request: Request, first_date:str, last_date:str):
    mycol, con =  connect_DB("family_mart", "GA_record")
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = {"event_date": {"$gte": start_date, "$lte": end_date},
             "event_name": {"$in": ["select_brand", "select_activity", "select_product", "select_related_product"]}}
    fields = {"event_name", "item_name", "stay_time"}
    cursor = mycol.find(query, fields)
    df = pd.DataFrame(list(cursor)).drop(["_id"], axis=1)
    con.close()
    
    maps = {'select_brand': "品牌", "select_product": "產品", "select_activity": "活動", "select_related_product": "產品"}
    df["event_name"] = df["event_name"].map(maps)
    df["stay_time"] = pd.to_timedelta(df["stay_time"]).apply(lambda i: i.total_seconds()/60).round(2).fillna(0.5)
    
    df_gb = df.groupby(["event_name", "item_name"])["stay_time"].agg(["count", "sum"]).reset_index(level="item_name")
    df_gb.columns = ["name", "click", "time"]
    df_gb["time"] = df_gb["time"].round(2)
    
    out_dict = df_gb.groupby(df_gb.index).apply(lambda i: i.to_dict("list")).to_dict()
    
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

### 訪客對場域點擊次數/停留時間 ###
@router.get("/visiters",dependencies=[Depends(verify_token)])
async def visiters_api(request: Request, first_date:str, last_date:str):
    mycol, con =  connect_DB("family_mart", "GA_record")
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = {"event_date": {"$gte": start_date, "$lte": end_date},
             "event_name": {"$in": ["select_brand", "select_activity", "select_product", "select_related_product"]}}
    fields = {"event_name", "item_name", "stay_time", "relation"}
    cursor = mycol.find(query, fields)
    df = pd.DataFrame(list(cursor))
    con.close()
    
    maps = {'select_brand': "品牌", "select_product": "產品", "select_activity": "活動", "select_related_product": "產品"}
    df["event_name"] = df["event_name"].map(maps)
    df_gb = df.groupby(["event_name", "relation", "item_name"])["stay_time"].agg(["count"]).reset_index()

    top_c = df_gb.groupby(["event_name", "item_name"])["count"].sum().sort_values(ascending=False).reset_index().groupby(["event_name"]).head(5)
    but_c = df_gb.groupby(["event_name", "item_name"])["count"].sum().sort_values(ascending=False).reset_index().groupby(["event_name"]).tail(5)
    top_c.insert(0, "TB", "top")
    but_c.insert(0, "TB", "buttom")
    c_all = pd.concat([top_c, but_c])
    
    df_gb_top = df_gb[df_gb["item_name"].isin(top_c["item_name"])].rename(columns={"count": "value"})
    df_gb_but = df_gb[df_gb["item_name"].isin(but_c["item_name"])].rename(columns={"count": "value"})
    
    df_gb_top.insert(0, "TB", "top")
    df_gb_but.insert(0, "TB", "buttom")
    df_gb_all = pd.concat([df_gb_top, df_gb_but])
    temp = pd.DataFrame()
    for i, j, k in zip(c_all["TB"], c_all["event_name"], c_all["item_name"]):
        for x in ["male", "female", "friend", "couple", "family"]:
            temp1 = pd.DataFrame([[i, j, k, x]], columns = ["TB", "event_name", "item_name", "relation"])
            temp = pd.concat([temp, temp1])
    try:
        df_gb_all = pd.merge(temp, df_gb_all , on=["TB", "event_name", "item_name", "relation"], how="left").fillna(0)
        df_gb_all["item_name"] = df_gb_all["item_name"].replace(0, None)
        
        out_dict = df_gb_all.groupby(["TB", "event_name", "item_name"])["relation", "value"].apply(lambda i: i.to_dict("list")).reset_index(name="list", level="item_name")
        out_dict = out_dict.groupby(["TB", "event_name"])["item_name", "list"].apply(lambda i: dict(zip(i["item_name"], i["list"]))).reset_index(name="list")
        out_dict = out_dict.groupby(["TB"])["event_name", "list"].apply(lambda i: dict(zip(i["event_name"], i["list"]))).to_dict()
    except:
        out_dict = {"top": {"品牌": None, "活動": None, "產品": None},
                    "buttom": {"品牌": None, "活動": None, "產品": None}}   
    
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

##### Top10推薦產品點擊 #####
@router.get("/recommend",dependencies=[Depends(verify_token)])
async def recommend_api(request: Request, first_date:str, last_date:str):
    mycol, con =  connect_DB("family_mart", "GA_record")
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = {"event_date": {"$gte": start_date, "$lte": end_date},
             "event_name": {"$eq": "select_related_product"}}
    fields = {"item_name"}
    cursor = mycol.find(query, fields)
    df = pd.DataFrame(list(cursor))
    con.close()
    
    if df.empty == True:
        df = pd.DataFrame(columns = ["_id", "item_name"])
        
    df_gb = df.groupby(["item_name"])["item_name"].count().reset_index(name="count").sort_values(by=["count"],ascending=False).head(10)
    df_gb = df_gb[df_gb["item_name"] != "茶2"]
    
    out_dict = df_gb.to_dict("list")
    
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

##### 篩選項目次數 #####
@router.get("/filter",dependencies=[Depends(verify_token)])
async def filter_api(request: Request, first_date:str, last_date:str):
    mycol, con =  connect_DB("family_mart", "GA_record")
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = {"event_date": {"$gte": start_date, "$lte": end_date},
             "event_name": {"$in": ["select_filter_activity", "select_filter_product"]}}
    fields = {"event_name", "item_name"}
    cursor = mycol.find(query, fields)
    df = pd.DataFrame(list(cursor))
    con.close()
    
    if df.empty == True:
        df = pd.DataFrame(columns = ["_id", "event_name",  "item_name"])
    df.drop(["_id"], axis=1, inplace=True)
    df["event_name"] = df["event_name"].map({"select_filter_activity": "活動", "select_filter_product": "產品"})
    
    temp = df["item_name"].str.split(",", expand=True)
    df = pd.concat([df.drop(["item_name"], axis=1), temp], axis=1)
    df = pd.melt(df, id_vars=['event_name'], value_name="filter_name").drop(["variable"], axis=1).dropna()
    
    df_gb = df.groupby(["event_name", "filter_name"])["filter_name"].count().reset_index(name="count").sort_values(by="count", ascending=False)
    df_gb = df_gb.groupby(["event_name"]).head(10).sort_values(by=["event_name", "count"], ascending=False)
    temp = pd.DataFrame(["活動", "產品"], columns=["event_name"])
    df_gb = temp.merge(df_gb, on=["event_name"], how="left").fillna("QAQ").replace("QAQ", None)
    
    out_dict = df_gb.groupby(["event_name"])["filter_name", "count"].apply(lambda i: i.to_dict("list")).to_dict()
    
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response


