from fastapi import Request, APIRouter, Depends
from lib.utils import connect_DB
from routers.auth.auth import verify_token
import numpy as np
import pandas as pd
from datetime import datetime

### Set APIRouter ###
router = APIRouter(prefix="/api/data")

### Machine data transform function ###
def machine_ssp(df):
    if df == "17d5dffb":
        return "A1"
    elif df == "b97bc201":
        return "I"
    else:
        return np.nan

### Age data transform to range function ###
def age_range(age):
    age = age
    if 0 < age <= 10:
        return "0~10"
    elif 10 < age <= 20:
        return "11~20"
    elif 20 < age <= 25:
        return "21~25"
    elif 25 < age <= 30:
        return "26~30"
    elif 30 < age <= 40:
        return "31~40"
    elif 40 < age <= 50:
        return "41~50"
    elif 50 < age <= 60:
        return "51~60"
    else:
        return "60up"

### Click and dwell time data transform function ###
def click_stay(df):
    df_c = pd.DataFrame(df.groupby(["location", "event_name", "item_name"])["event_name"].count()).rename(columns={"event_name":"點擊次數"}).reset_index()
    df_s = df.groupby(["location", "event_name", "item_name"])["stay_time"].apply(lambda i: i.sum()).reset_index().rename(columns={"stay_time":"停留時間"})
    all_gb = pd.merge(df_c, df_s, how="inner").sort_values(["點擊次數"], ascending=False)
    return all_gb

### Click data transform function ###
def clicktop(df):
    df_gb = pd.DataFrame(df.groupby(["location", "event_name", "item_name"])["event_name"].count()).rename(columns={"event_name":"點擊次數"}).reset_index().sort_values(["點擊次數"], ascending=False)
    df_gb = df_gb.groupby(['location', 'event_name']).head(10)
    return df_gb

### Click and dwell time data transform function for GA data ###
def click_stay_g_a(df):
    df_c_g = pd.DataFrame(df.groupby(["location", "event_name", "item_name", "relation"])["event_name"].count()).rename(columns={"event_name":"點擊次數"}).reset_index()
    df_s_g = df.groupby(["location", "event_name", "item_name", "relation"])["stay_time"].apply(lambda i: i.sum()).reset_index().rename(columns={"stay_time":"停留時間"})
    df_g = pd.merge(df_c_g, df_s_g, how="inner").sort_values(["點擊次數"], ascending=False)
    return df_g

### Rename relation function ###
def rename(col):
    if col == "male":
        return "男"
    elif col == "female":
        return "女"
    elif col == "friend":
        return "朋友"
    elif col == "couple":
        return "情侶"
    elif col == "family":
        return "家庭"
    else:
        return col


#%%
##### API for frontend #####
### 機台前人群數字 ###
@router.get("/info",dependencies=[Depends(verify_token)])
async def data_info_api(request: Request, first_date:str, last_date:str):
    # Mongodb 資料庫連結 #
    db, con = connect_DB('bottle_factory', 'detect_record')
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = { "visittime": {"$gte": start_date,"$lte": end_date} }
    fields = {"relation_group", "age", "group_id", "visittime", "pcid", "staytime"}
    cursor = db.find(query, fields)
    ssp = pd.DataFrame(list(cursor))
    con.close()
    # 機台處理 #
    ssp["location"] = ssp["pcid"].str.split("_").str[-1].apply(machine_ssp)
    ssp.drop(["pcid"], axis=1, inplace=True)
    # 機台停留時間處理 #
    ssp["staytime"] = ssp["staytime"].str.split(" ").str[0].astype(float)
    ssp = ssp.groupby(["group_id", "relation_group", "location"], dropna=False)["staytime"].sum().reset_index()
    # 機台閱覽人數 & 平均停留時間 #
    ssp_df_gb_v = ssp.groupby(["location"])["group_id"].nunique().reset_index().rename(columns={"group_id": "機台閱覽群數"})
    ssp_df_gb_s = ssp.groupby(["location"])["staytime"].mean().round(2).reset_index().rename(columns={"staytime": "機台停留時間"})
    ssp_df_gb = pd.merge(ssp_df_gb_v, ssp_df_gb_s, how="inner")
    
    # Dataframe資料格式轉為json格式 #
    out_dict = ssp_df_gb.groupby('location')[['location', '機台閱覽群數','機台停留時間']].apply(lambda i: i.set_index('location').to_dict('list')).to_dict()
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

### 每日機台 人群數/停留時間 ###
@router.get("/timeline",dependencies=[Depends(verify_token)])
async def time_line_api(request: Request, first_date:str, last_date:str):
    db, con = connect_DB('bottle_factory', 'detect_record')
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = { "visittime": {"$gte": start_date,"$lte": end_date} }
    fields = {"relation_group", "age", "group_id", "visittime", "pcid", "staytime"}
    cursor = db.find(query, fields)
    ssp = pd.DataFrame(list(cursor))
    ssp["visittime"] = pd.to_datetime(ssp["visittime"])
    ssp["date"] = ssp["visittime"].dt.date
    con.close()
    # 機台處理 #
    ssp["location"] = ssp["pcid"].str.split("_").str[-1].apply(machine_ssp)
    ssp.drop(["pcid"], axis=1, inplace=True)      
    # 機台停留時間處理 #
    ssp["staytime"] = ssp["staytime"].str.split(" ").str[0].astype(float)
    ssp = ssp.groupby(["date", "group_id", "relation_group", "location"], dropna=False)["staytime"].sum().reset_index()
    ## 每日機台 機台閱覽人數&平均停留時間 ##
    ssp_df_gb_v = ssp.groupby(["location", "date"])["group_id"].nunique().reset_index().rename(columns={"group_id": "機台閱覽群數"})
    ssp_df_gb_s = ssp.groupby(["location", "date"])["staytime"].mean().round(2).reset_index().rename(columns={"staytime": "機台停留時間"})
    ssp_df_gb = pd.merge(ssp_df_gb_v, ssp_df_gb_s, how="inner")
    
    out_dict = ssp_df_gb.groupby('location')[['location', "date", '機台閱覽群數','機台停留時間']].apply(lambda i: i.set_index('location').to_dict(orient='list')).to_dict()
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response
    
### 顧客屬性占比 ###
@router.get("/relation",dependencies=[Depends(verify_token)])
async def relation_api(request: Request, first_date:str, last_date:str):
    db, con = connect_DB('bottle_factory', 'detect_record')
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = { "visittime": {"$gte": start_date,"$lte": end_date} }
    fields = {"relation_group", "age", "group_id", "visittime", "pcid", "staytime"}
    cursor = db.find(query, fields)
    ssp = pd.DataFrame(list(cursor))
    con.close()
    # 機台處理 #
    ssp["location"] = ssp["pcid"].str.split("_").str[-1].apply(machine_ssp)
    ssp.drop(["pcid", "age", "visittime", "staytime", "_id"], axis=1, inplace=True)
    # Relation處理 #      
    ssp.drop_duplicates(keep="first", inplace=True)
    ssp = ssp[ssp["relation_group"] != ""]
    ssp["relation_group"] = ssp["relation_group"].apply(rename)
    ## 顧客屬性占比 ##
    ssp_df_gb = pd.DataFrame((ssp.groupby(["location"])["relation_group"].value_counts(normalize=True)*100).round(2)).rename(columns={"relation_group": "屬性占比"}).reset_index()
    ssp_df_gb.rename(columns={"relation_group": "relation"}, inplace=True)
    
    out_dict = ssp_df_gb.groupby('location')[['location', "relation", '屬性占比']].apply(lambda i: i.set_index('location').to_dict(orient='list')).to_dict()
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

### 訪客年齡層占比 ###
@router.get("/age",dependencies=[Depends(verify_token)])
async def age_api(request: Request, first_date:str, last_date:str):
    db, con = connect_DB('bottle_factory', 'detect_record')
    start_date = datetime.strptime(first_date + " 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(last_date + " 23:59:59", '%Y-%m-%d %H:%M:%S')
    query = { "visittime": {"$gte": start_date,"$lte": end_date} }
    fields = {"relation", "age", "group_id", "visittime", "pcid", "staytime"}
    cursor = db.find(query, fields)
    ssp = pd.DataFrame(list(cursor))
    con.close()
    # 機台處理 #
    ssp["location"] = ssp["pcid"].str.split("_").str[-1].apply(machine_ssp)
    ssp.drop(["pcid", "visittime", "staytime", "_id", "group_id", "relation"], axis=1, inplace=True)
    # Age處理 #
    ssp["age"] = ssp["age"].str.replace("歲", "").astype(int)
    ssp["age_range"] = ssp["age"].apply(age_range)
    ## 訪客年齡層占比 ##
    ssp_df_gb = pd.DataFrame(ssp.groupby(["location"])["age_range"].value_counts()).rename(columns={"age_range": "年齡層群數"}).reset_index()
    
    out_dict = ssp_df_gb.groupby('location')[['location', "age_range", '年齡層群數']].apply(lambda i: i.set_index('location').to_dict(orient='list')).to_dict()
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

### 場域點擊次數/停留時間 ###
@router.get("/click_stay",dependencies=[Depends(verify_token)])
async def click_stay_api(request: Request, first_date:str, last_date:str):
    db, con = connect_DB('bottle_factory', 'GA_record')
    start_date = datetime.strptime(first_date, '%Y-%m-%d')
    end_date = datetime.strptime(last_date, '%Y-%m-%d')
    query = { "event_date": {"$gte": start_date, "$lte": end_date} }
    fields = {'event_name', 'item_name', 'stay_time', 'location'}
    cursor = db.find(query, fields)
    df = pd.DataFrame(list(cursor))
    df["stay_time"] = pd.to_timedelta(df["stay_time"])
    con.close()

    ## 點擊數&總停留時間 ##
    df = df.replace({'event_name' : {'點擊下方項目':"選單", "點擊地圖項目":"地圖", "點擊課程更多資訊":"課程", "點擊購物更多資訊":"購物"}})
    df_select = df[(df["event_name"] == "選單") | (df["event_name"] == "地圖") | (df["event_name"] == "課程") | (df["event_name"] == "購物")]
    df_select_gb = click_stay(df_select)
    df_select_gb["停留時間"] = df_select_gb["停留時間"].apply(lambda i: i.total_seconds()/60).round(2)
    
    out_dict = df_select_gb.groupby(['location', 'event_name'])['item_name', '點擊次數', '停留時間'].apply(lambda i :i.to_dict('list')).reset_index(level="event_name").rename(columns={0: "list"})
    out_dict = out_dict.groupby(['location'])['event_name', 'list'].apply(lambda i: dict(zip(i["event_name"], i["list"]))).to_dict()
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

### 訪客對場域之點擊次數/停留時間 ###
@router.get("/click_stay_re",dependencies=[Depends(verify_token)])
async def click_stay_re_api(request: Request, first_date:str, last_date:str):
    db, con = connect_DB('bottle_factory', 'GA_record')
    start_date = datetime.strptime(first_date, '%Y-%m-%d')
    end_date = datetime.strptime(last_date, '%Y-%m-%d')
    query = { "event_date": {"$gte": start_date, "$lte": end_date} }
    fields = {'event_name', 'item_name', 'stay_time', 'relation', 'location'}
    cursor = db.find(query, fields)
    df = pd.DataFrame(list(cursor))
    df["stay_time"] = pd.to_timedelta(df["stay_time"])
    con.close()
    
    df["relation"] = df["relation"].apply(rename)

    ## 點擊次數/停留時間 vs 男女比/年齡層 ##
    df = df.replace({'event_name' : {'點擊下方項目':"選單", "點擊地圖項目":"地圖", "點擊課程更多資訊":"課程", "點擊購物更多資訊":"購物"}})
    df_select = df[(df["event_name"] == "選單") | (df["event_name"] == "地圖") | (df["event_name"] == "課程") | (df["event_name"] == "購物")]
    df_select_gb = click_stay_g_a(df_select)
    df_select_gb["停留時間"] = df_select_gb["停留時間"].apply(lambda i: i.total_seconds()/60).round(2) 
    # 補齊空值 #
    location = pd.DataFrame(df_select_gb["location"].unique(), columns=["location"])
    relation = pd.DataFrame(["男", "女", "朋友", "情侶", "家庭"], columns=["relation"])
    df_select_gb_all = pd.DataFrame(columns=["location", "relation", "item_name", "event_name"])
    for i in (df_select_gb["event_name"].unique()):
        item = pd.DataFrame(df_select_gb[df_select_gb["event_name"] == i]["item_name"].unique(), columns=["item_name"])
        for j in (location, relation, item) :
            j["key"] = 1
        globals()['df_%s' % i] = location.merge(relation, on=['key']).merge(item, on=['key']).drop("key", 1)
        globals()['df_%s' % i]["event_name"] = i
        df_select_gb_all = pd.concat([df_select_gb_all, globals()['df_%s' % i]])
        del globals()['df_%s' % i]
    df_select_gb = pd.merge(df_select_gb_all, df_select_gb, on=["location", "relation", "item_name", "event_name"], how="left").fillna(0)
    
    out_dict = df_select_gb.groupby(['location', 'event_name', 'relation'])['item_name', '點擊次數', '停留時間'].apply(lambda i :i.to_dict('list')).reset_index(level="relation").rename(columns={0: "list"})
    out_dict = out_dict.groupby(['location', 'event_name'])['relation', 'list'].apply(lambda i: dict(zip(i["relation"], i["list"]))).reset_index(level="event_name").rename(columns={0: "list"})
    out_dict = out_dict.groupby(['location'])['event_name', 'list'].apply(lambda i: dict(zip(i["event_name"], i["list"]))).to_dict()
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response

### 場域Top10瀏覽項目 ###
@router.get("/top10click",dependencies=[Depends(verify_token)])
async def top10click_api(request: Request, first_date:str, last_date:str):
    db, con = connect_DB('bottle_factory', 'GA_record')
    start_date = datetime.strptime(first_date, '%Y-%m-%d')
    end_date = datetime.strptime(last_date, '%Y-%m-%d')
    query = { "event_date": {"$gte": start_date, "$lte": end_date} }
    fields = {'event_name', 'item_name', 'location'}
    cursor = db.find(query, fields)
    df = pd.DataFrame(list(cursor))
    con.close()
    
    ## 場域Top10瀏覽項目 ##
    df = df.replace({'event_name' : {'點擊下方項目':"選單", "點擊地圖項目":"地圖", "點擊課程更多資訊":"課程", "點擊購物更多資訊":"購物"}})
    df_top10 = df[(df["event_name"] == "選單") | (df["event_name"] == "地圖") | (df["event_name"] == "課程") | (df["event_name"] == "購物")]
    df_top10_gb = clicktop(df_top10)
    
    out_dict = df_top10_gb.groupby(['location', 'event_name'])['item_name', '點擊次數'].apply(lambda i :i.to_dict('list')).reset_index(level="event_name").rename(columns={0: "list"})
    out_dict = out_dict.groupby(['location'])['event_name', 'list'].apply(lambda i: dict(zip(i["event_name"], i["list"]))).to_dict()
    response={
                "Method": "GET",
                "State": "Success",
                "Data": out_dict,
                "message": "GET Success"
            }
    return response
    

