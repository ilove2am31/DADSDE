##### CDP labels report data etl #####
##### Only present "marketing channel interactive ratings" & "products purchase simultaneously" #####
import pandas as pd
import numpy as np
from datetime import date
from lib.utils import connect_DB, get_member_by_Clabel
from lib.src import get_label_by_Clabel


### Get this month function ###
def get_year_month():
    # year_month: contains date, format: '%Y-%m', example: '2022-06', type: string
    return str(date.today())[:7]


### Products purchase simultaneously function ###
def prod_purchase(pur_df, members_df, mem, dates):
    
    if len(pur_df) != 0:
        # purchase data
        pur_df = pur_df.drop(["order_id"], axis=1)
        pur_df = pur_df[["D" not in i for i in pur_df["product_id"]]]
        pur_df.dropna(subset=["member_id"], inplace=True)
        # purchase data with members in label group #
        members_df = members_df[members_df["member_id"].isin(pur_df["member_id"])]
        pur_df = pur_df.merge(members_df, on=["member_id"], how="outer").drop_duplicates()
        pur_df = pur_df.groupby(['labelId', 'label', 'date', 'online', 'product_id'])["member_id"].nunique().reset_index(name="count")
        pur_df = pur_df.sort_values(['label','count'], ascending=False)
        out = mem.merge(pur_df, on=['labelId', 'label'], how="outer")
        out["date"] = dates
    else:
        out = mem
        out["date"] = dates
        for i in ['online', 'product_id']:
            out[i] = np.nan
    
    return out


### Marketing channel interactive ratings function ###
def channel_rating(ema_df, sms_df, fb_df, members_df, mem):
    ema_df["channel"] = "email"
    sms_df["channel"] = "sms"
    fb_df["channel"] = "fb"
    df = pd.concat([ema_df, sms_df, fb_df])
    df = members_df.merge(df, on=["member_id"])
    df_gb = df.groupby(["labelId", "label", "channel", "response"])["member_id"].count().reset_index(name="count")
    df_gb["percentage"] = 100 * df_gb['count'] / df_gb.groupby(["labelId", "label", "channel"])['count'].transform('sum')
    temps = pd.DataFrame()
    for i in mem["label"].unique():
        for j in ["email", "sms", "fb"]:
            for k in ["高", "中", "低", "無"]:
                temp1 = pd.DataFrame([[i, j, k]], columns=["label", "channel", "response"])
                temps = pd.concat([temps, temp1])
    df_gb = temps.merge(df_gb, on=["label", "channel", "response"], how="outer").drop(["labelId", "count"], axis=1)
    df_gb = df_gb.merge(mem[["labelId", "label"]].drop_duplicates(), on=["label"]).sort_values(["label", "channel", "response"]).fillna(0).rename(columns={"response": "reaction"})
    df_gb = df_gb[['labelId', 'label', 'channel', 'reaction', 'percentage']]
    
    return df_gb



#%%
### Extract & transform & load data ###
def export_report(cdb_id):
    # Connect mysql local databas # 
    year_month = get_year_month()
    engine, con = connect_DB(cdb_id)
    
    # Label group members chosed by cusomer #
    c_mem = get_label_by_Clabel(cdb_id)
    members_df = pd.DataFrame()
    for i, j in zip(c_mem["labelId"], c_mem["label"]):
        C_member = pd.DataFrame(get_member_by_Clabel(i, cdb_id), columns=["member_id"])
        C_member["labelId"] = i
        C_member["label"] = j
        members_df = pd.concat([members_df, C_member])
    members_df["member_id"] = members_df["member_id"].astype(str)
    
    # Products purchase simultaneously #
    path = f''' SELECT Left(date, 7) as date, online, order_id, product_id, member_id FROM Info.info_purchase_record
                Where Left(date, 7) = "{year_month}" And add_on = 0; '''
    cursor = con.execute(path)
    pur_df = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())

    mem = c_mem.copy()
    pur_prod = prod_purchase(pur_df, members_df, mem, dates=year_month)
    
    path = f'DELETE FROM Cluster_report.pur_prod_month WHERE date = "{year_month}";'
    con.execute(path)
    
    pur_prod.to_sql("pur_prod_month", con = con, schema = "Cluster_report", if_exists = "append", index = False)
        
    # Marketing channel interactive ratings #
    path = f''' Select * From Email_performance.Email_user Where member_id In {tuple(list(members_df["member_id"])+[""])}; '''
    cursor = con.execute(path)
    ema_df = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    path = f''' Select * From SMS_performance.SMS_user Where member_id In {tuple(list(members_df["member_id"])+[""])}; '''
    cursor = con.execute(path)
    sms_df = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    path = f''' Select * From Facebook.facebook_user Where member_id In {tuple(list(members_df["member_id"])+[""])}; '''
    cursor = con.execute(path)
    fb_df = pd.DataFrame(cursor.fetchall(), columns=cursor.keys())
    
    mem = c_mem.copy()
    channel_df = channel_rating(ema_df, sms_df, fb_df, members_df, mem)
    channel_df_m = channel_df.copy()
    channel_df_m.insert(2, "month", year_month)
    
    path = f'DELETE FROM Cluster_report.response_month WHERE month = "{year_month}";'
    con.execute(path)
    
    channel_df_m.to_sql("response_month", con = con, schema = "Cluster_report", if_exists = "append", index = False)
    
    con.close()
    engine.dispose()
    
    return "success"


