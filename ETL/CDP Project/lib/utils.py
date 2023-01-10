import boto3
import json
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
import hashlib,base64
from cryptography.fernet import Fernet
import time
import pymongo
import numpy as np
import uuid

def schedule_process(expression):
    #process schedule into the eventbridge format.
    for filter in ('cron', '(', ')', ' ?'):
        expression = expression.replace(filter, '')
    expression = expression.split(' ')
    def to_int(x):
        return int(x)
    expression = list(map(to_int, expression))
    '''
    expression: [minute, hour, day, month, year]
    '''

    return datetime(year=expression[-1], month=expression[-2], day=expression[-3], hour=expression[-4], minute=expression[-5])+timedelta(hours=8) # UTC to CST


def connect_DB(database='fircdp-dev'):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('db_config')
    response = table.get_item(
    Key={
        'cdb_id':database
    }
    )
    setting = response['Item']
    user = setting['user']
    passwd = setting['passwd']
    host = setting['host']
    db_name = setting['db_name']
    
    engine = create_engine(f'mysql+pymysql://{user}:{passwd}@{host}:3306/{db_name}?charset=utf8mb4', echo=False)
    con = engine.connect()

    return engine, con

def connect_pymongo(database='fircdp-dev'):
    myclient = pymongo.MongoClient("mongodb+srv://root:rd0227853858@cluster0.u6qj2.mongodb.net")
    mydb = myclient[database]

    return myclient, mydb

def http_response(code, message):
    return {
        "statusCode": code,
        "message": message
    }

def activity(cdb_id):
    engine,con=connect_DB(cdb_id)
    path=f'''SELECT activity_id FROM Activity.Activity;  '''
    cursor = con.execute(path) 
    data = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())               
    acitivity = list(data["activity_id"])+["",""]

    path=f'''SELECT 
        k.activity_id,k.activity_name,
        CASE
            WHEN k.start_date<j.start_date or isnull(j.start_date)  THEN k.start_date 
            ELSE j.start_date
        END AS start_date,
        CASE
            WHEN k.end_date>j.end_date or isnull(j.end_date)  THEN k.end_date 
            ELSE j.end_date
        END AS end_date,
        k.time
        
    FROM
        (SELECT 
            script_name activity_id,
                DATE(MAX(ScheduleExpression)) `end_date`,
                DATE(MIN(ScheduleExpression)) `start_date`
        FROM
            (SELECT 
            Name, script_name, ScheduleExpression
            FROM Email_schedule.schedule_tracking 
            
            UNION 
            
            SELECT 
            Name, script_name, ScheduleExpression
            FROM SMS_schedule.schedule_tracking

            UNION 
            
            SELECT 
            Name, script_name, start_time
            FROM Script_schedule.origin_schedule_tracking where end_time is not null and script_name in {tuple(acitivity)}

            UNION 
            SELECT 
            Name, script_name, end_time
            FROM Script_schedule.origin_schedule_tracking where end_time is not null and script_name in {tuple(acitivity)}) i
        GROUP BY script_name) j
            RIGHT JOIN
        Activity.Activity k ON k.activity_id = j.activity_id;'''
    cursor = con.execute(path) 
                    
    data = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    data.to_sql(name="Activity",chunksize=10000, schema = 'Activity',index = False,if_exists = 'replace' ,con=con)
    con.close()
    engine.dispose()




def fernet_key():
    password = "rdretailingdata".encode('utf-8')
    key = hashlib.md5(password).hexdigest().encode('utf-8')
    key = base64.urlsafe_b64encode(key)
    fernet = Fernet(key)
    return fernet
    
def get_cf_id(cdb_id):
    client = boto3.client('s3')
    result = client.get_object(Bucket='clickforce', Key='click-force.json')
    setting = result["Body"].read().decode()
    setting = json.loads(setting)
    cf_id = setting[cdb_id]
    cf_id=cf_id.replace("-","_")
    return(cf_id)
    
def get_ma_id():
    client = boto3.client('s3')
    result = client.get_object(Bucket='rema-lambda-function', Key='config/ma_cdb_id.json')
    setting = result["Body"].read().decode()
    return(eval(setting))

def email_out_script(df,cdb_id,types="vice",chunk=20000):
    df=df.copy()
    df["template"]=df["ruleID"]
    df["configuretion_set"]=cdb_id
    
    engine,con=connect_DB(cdb_id)
    if types =="vice":
        path='''SELECT Name,sender FROM Email_schedule.schedule_vice_tracking  as a union (SELECT Name,email_sender FROM SMS_schedule.schedule_vice_tracking as b);'''
    elif types =="trigger":
        path='''SELECT Name,email_sender sender FROM Script_schedule.schedule_tracking;'''
    cursor=con.execute(path)
    trans=dict(cursor.fetchall())
    con.close()
    engine.dispose()
    df["sender"]=[trans[x] for x in df["ruleID"]]
    for n in range(len(df)//chunk+1):
        temp=df.iloc[chunk*n:chunk*(n+1)]
        if len(temp):
            out=temp.reset_index(drop=True).to_csv(None).encode()
            client=boto3.client("s3")
            client.put_object(Body=out,Bucket='rema-email-send',Key="wait-send/"+cdb_id+"_"+types+"_"+uuid.uuid4().hex[:8]+"_"+str(n+1))
            time.sleep(1)

def email_immediate(df,cdb_id,types="trigger",chunk=20000):
    df=df.copy()
    df["template"]=df["ruleID"]
    df["configuretion_set"]=cdb_id
    
    engine,con=connect_DB(cdb_id)

    if types =="trigger" or types=="trigger-system":
        path='''SELECT Name,email_sender sender FROM Script_schedule.schedule_tracking;'''
        cursor=con.execute(path)
        trans=dict(cursor.fetchall())
    con.close()
    engine.dispose()

    df["sender"]=[trans[x] for x in df["ruleID"]]
    for n in range(len(df)//chunk+1):
        temp=df.iloc[chunk*n:chunk*(n+1)]
        if len(temp):
            out=temp.reset_index(drop=True).to_csv(None).encode()
            client=boto3.client("s3")
            client.put_object(Body=out,Bucket='rema-email-send',Key="send-immediate/"+cdb_id+"_"+types+"_"+uuid.uuid4().hex[:8]+"_"+str(n+1))
            time.sleep(1)

def sms_immediate(df,cdb_id,types="trigger",chunk=20000):
    df=df.copy()
    df["template"]=df["ruleID"]
    
    engine,con=connect_DB(cdb_id)

    if types =="trigger" or types=="trigger-system":
        path='''SELECT Name,sms_sender sender FROM Script_schedule.schedule_tracking;'''
        cursor=con.execute(path)
        trans=dict(cursor.fetchall())
    con.close()
    engine.dispose()

    df["sender"]=[trans[x] for x in df["ruleID"]]
    for n in range(len(df)//chunk+1):
        temp=df.iloc[chunk*n:chunk*(n+1)]
        if len(temp):
            out=temp.reset_index(drop=True).to_csv(None).encode()
            client=boto3.client("s3")
            client.put_object(Body=out,Bucket='rema-sms-send',Key="send-immediate/"+cdb_id+"_"+types+"_"+uuid.uuid4().hex[:8]+"_"+str(n+1))
            time.sleep(1)
            
def sms_out_script(df,cdb_id,types="vice",chunk=20000):
    df=df.copy()
    engine,con=connect_DB(cdb_id)
    if types =="vice":
        path='''SELECT Name,sender FROM SMS_schedule.schedule_vice_tracking as a union (SELECT Name,sms_sender FROM Email_schedule.schedule_vice_tracking as b);'''
        cursor=con.execute(path)
        trans=dict(cursor.fetchall())
        df["sender"]=[trans[x] for x in df["ruleID"]]
        df['template']=df["ruleID"]
    elif types =="trigger":
        path='''SELECT Name,sms_sender sender FROM Script_schedule.schedule_tracking;'''
        cursor=con.execute(path)
        trans=dict(cursor.fetchall())
        df["sender"]=[trans[x] for x in df["ruleID"]]
        df['template']=df["ruleID"]
    elif types =="type_type":

        df["sender"]=["None"]*len(df)
        df['template']=df["ruleID"]
    con.close()
    engine.dispose()
    
    
    for n in range(len(df)//chunk+1):
        temp=df.iloc[chunk*n:chunk*(n+1)]
        if len(temp):
            out=temp.reset_index(drop=True).to_csv(None).encode()
            client=boto3.client("s3")
            client.put_object(Body=out,Bucket='rema-sms-send',Key="wait-send/"+cdb_id+"_"+types+"_"+uuid.uuid4().hex[:8]+"_"+str(n+1))
            time.sleep(1)
    





def get_test_label(label_id,cdb_id):
    engine,con=connect_DB(cdb_id)
    path=f'''SELECT Email,phone  FROM ReMA.T_test_label where labelId="{label_id}";'''
    cursor = con.execute(path)
    result = cursor.fetchone()
    engine.dispose()
    con.close()
    return {"email":eval(result[0]),"phone":eval(result[1])}

def get_member_by_SS_label(cdb_id,labels):
    engine,con=connect_DB(cdb_id)
    query=f'''SELECT labelId,list,include FROM ReMA.S_customized_label where labelId in {labels}''' 
    cursor = con.execute(query) 
    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    df["list"]=[json.loads(x)['content'] for x in df["list"] ]
    con.close()
    engine.dispose() 
    
    return df

def get_member_by_Clabel(label_id, cdb_id):
    client, db=connect_pymongo(cdb_id)
    out=db["label"].find_one({"label_id":label_id})
    client.close()
    try:
        out=out["content"]
        member_ids = get_member_content(cdb_id,out)
    except:
        member_ids=[]
    return member_ids

def get_member_by_Ulabel(label_id, cdb_id):
    client, db=connect_pymongo(cdb_id)
    out=db["upload_label"].find({"labelId":label_id})
    try:
        df=pd.DataFrame(out)
        member_ids=df['members'].values[0]
    except:
        member_ids=[]
    client.close()    
    return member_ids
#
def get_member_by_Mlabel_input(cdb_id,data,types):
    s=set()
    for x,y in data.values:

        if (x.startswith("C_")) or (x.startswith("X")):
            temp=get_member_by_Clabel(x, cdb_id)
        elif x.startswith("ALL"):
            engine,con=connect_DB(cdb_id)
            path='''SELECT member_id FROM Info.info_member_info;'''
            cursor=con.execute(path)
            df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
            engine.dispose()
            con.close()  
            temp=list(df["member_id"])

        elif x.startswith("T_"):
            temp=get_test_label(x,cdb_id)[types]
        elif x.startswith("SS_"):
            y=tuple([x]+["",""])
            data=get_member_by_SS_label(cdb_id,y)
            if data["include"][0]==1:
                temp=eval(data["list"][0])

            else:
                engine,con=connect_DB(cdb_id)
                path = f'''
                SELECT member_id FROM Info.info_member_info'''        
                cursor = con.execute(path)
                engine.dispose()
                con.close()  
                df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                mem=set(df.member_id)
                temp=list(mem-set(data["list"][0]))  
        elif x.startswith("U_"):
            client, db=connect_pymongo(cdb_id)
            a=db["upload_label"].find({"labelId":x})
            df=pd.DataFrame(a)
            client.close()
            temp=df['members'].values[0]
            temp=list(map(int, temp))
        else:
            temp=[]
        temp=list(map(str, temp))
        s=set(map(str, s))
        if y :
            s=set(list(s)+temp)
        else:
            s=s-set(temp)
    return(s)
def get_member_by_Mlabel(label_id, cdb_id,types="email"):

    engine,con=connect_DB(cdb_id)
    path=f'''select label,include from ReMA.M_custom_label where  label_id ="{label_id}"'''
    cursor=con.execute(path)
    data=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    con.close()
    engine.dispose()
    s=get_member_by_Mlabel_input(cdb_id,data,types)
    return list(s)

def delete_Mlabel(label_id, cdb_id):

    engine,con=connect_DB(cdb_id)
    path=f'''select label from ReMA.M_custom_label where  label_id ="{label_id}" and label like "SS_%%" '''
    con.execute(path)
    cursor = con.execute(path)
    df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())

    label=tuple(list(df["label"])+["",""])
    path=f'''delete FROM ReMA.S_customized_label where labelId in {label};'''
    con.execute(path) 

    path=f'''delete from ReMA.M_custom_label where  label_id ="{label_id}"'''
    con.execute(path)
   
    con.close()
    engine.dispose()

    return http_response(200,"successfully delete the label")



def get_member_content(cdb_id,out):

    s=set()
    for x in out:
        tag=x["tag"]
        value=x["value"]
        web=x.get("web")
        more_option=x.get("more_option")
        if x["type"]=="基本屬性":
            temp=basic(cdb_id,tag,value)
        elif x["type"]=="價值分群":
            temp=value_label(cdb_id,tag,value)
        elif x["type"]=="渠道再行銷":
            temp=remarketing_label(cdb_id,web,tag,value,more_option)

        elif x["type"]=="AI顧客分群":
            temp=ai_label(cdb_id,tag,value)
        elif x["type"]=="消費行為":
            temp=purchase_behavior(cdb_id,web,tag,value,more_option)
        elif x["type"]=="網站行為":
            temp=online_behavior(cdb_id,web,tag,value,more_option)
        elif x["type"]=="CF站外瀏覽":
            temp=cf_behavior(cdb_id,tag,value,more_option)
        else:
            temp=[]
            f=x["type"].split("_")[0]
            fun=find(cdb_id,f)
            temp=fun(cdb_id,web,tag,value,more_option)
            
        
        if x["mode"] in ["初","或"]:
            s=set(list(s)+temp)
        elif x["mode"] in ["且"]:
            s=s-(s-set(temp))
    return(list(s))
    
def email_members(cdb_id):
    query=f'''SELECT a.member_id  FROM Info.info_member_status as a inner join (SELECT member_id from ReMA.Q_Email_quota where daily< (SELECT daily from ReMA.Q_Email_quota where member_id="limit") and 
    weekly< (SELECT weekly from ReMA.Q_Email_quota where member_id="limit")  and 
    monthly< (SELECT monthly from ReMA.Q_Email_quota where member_id="limit")  and
    yearly< (SELECT yearly from ReMA.Q_Email_quota where member_id="limit") ) as b on a.member_id=b.member_id  and a.email='normal'
    '''
    engine,con = connect_DB(cdb_id)
    cursor = con.execute(query)
    df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    out=df['member_id'].to_list()
    engine.dispose()
    con.close()
    
    return out

def sms_members(cdb_id):
    query=f'''SELECT a.member_id  FROM Info.info_member_status as a inner join (SELECT member_id from ReMA.Q_SMS_quota where daily< (SELECT daily from ReMA.Q_Email_quota where member_id="limit") and 
    weekly< (SELECT weekly from ReMA.Q_Email_quota where member_id="limit")  and 
    monthly< (SELECT monthly from ReMA.Q_Email_quota where member_id="limit")  and
    yearly< (SELECT yearly from ReMA.Q_Email_quota where member_id="limit") ) as b on a.member_id=b.member_id  and a.sms='normal'
    '''
    engine,con = connect_DB(cdb_id)
    cursor = con.execute(query)
    df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    out=df['member_id'].to_list()
    engine.dispose()
    con.close()
    
    return out
    
# class Remarketing_label_DB:
#     def __init__(self,cdb_id):
#         self.cdb_id = cdb_id
      
#     def export_member_id(self,label_id):
#         query=f''' select * from ReMA.R_marketing_label where labelId="{label_id}" ''' 
#         cursor = self.con.execute(query) 
#         df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
#         if df['filter'].any():
#             filter=df['filter'].values[0]
#             audience_type=df['audience_type'].values[0]
#             audience_action=df['audience_action'].values[0]
#             if filter=='time':
#                 start_time=str(df['start_time'].values[0])[:10]
#                 end_time=str(df['end_time'].values[0])[:10]
#                 if start_time=='all' and end_time=='all':
#                     if audience_type=='Email':
#                         if audience_action=='open':
#                             query=f''' SELECT a.member_id from Info.info_member_info as a 
#                             inner join (select  distinct member_id from (SELECT * FROM Email_performance.Email_view union all 
#                             SELECT * FROM Email_performance.Email_vice_view ) as t ) as b on a.member_id=b.member_id;  ''' 
#                             cursor = self.con.execute(query) 
#                         else:
#                             query=f''' SELECT a.member_id from Info.info_member_info as a 
#                             inner join (select  distinct member_id from (SELECT * FROM Email_performance.Email_{audience_action} union all 
#                             SELECT * FROM Email_performance.Email_vice_{audience_action} ) as t ) as b on a.member_id=b.member_id  ;  ''' 
#                             cursor = self.con.execute(query) 
#                     elif audience_type=='SMS':
#                         query=f''' SELECT a.member_id from Info.info_member_info as a 
#                         inner join (select  distinct member_id from (SELECT * FROM SMS_performance.SMS_{audience_action} union all 
#                         SELECT * FROM SMS_performance.SMS_vice_{audience_action} ) as t ) as b on a.member_id=b.member_id ;  '''                         
#                         cursor = self.con.execute(query) 
#                     elif audience_type=='Facebook':
#                         if audience_action=='buy':
#                             query=f''' SELECT a.member_id from Info.info_member_info as a inner join (select member_id from Facebook.facebook_{audience_action} ) as b on a.member_id=b.member_id ; ''' 
#                             cursor = self.con.execute(query) 
#                         else:
#                             query=f'''SELECT a.member_id from Info.info_member_info as a  inner join (select member_id from Facebook.facebook_{audience_action}) as b on a.member_id=b.member_id ; ''' 
#                             cursor = self.con.execute(query) 
#                     elif audience_type=='Line':
#                         if audience_action=='buy':
#                             query=f'''SELECT a.member_id from Info.info_member_info as a  inner join (select member_id from Line.line_{audience_action} ) as b on a.member_id=b.member_id ; ''' 
#                             cursor = self.con.execute(query) 
#                         else:
#                             query=f'''SELECT a.member_id from Info.info_member_info as a inner join (select member_id from Line.line_{audience_action}) as b on a.member_id=b.member_id ; ''' 
#                             cursor = self.con.execute(query) 
#                 else:
#                     if audience_type=='Email':
#                         if audience_action=='open':
#                             query=f''' SELECT i.member_id from Info.info_member_info as i inner join (select distinct member_id 
#                             from (select * from Email_performance.Email_view  where open_time >= "{start_time}" and open_time  <= "{end_time}" 
#                             union all select * from Email_performance.Email_vice_view  where open_time >= "{start_time}" and open_time  <= "{end_time}") as t ) as j on i.member_id=j.member_id ; ''' 
#                             cursor = self.con.execute(query) 
#                         elif audience_action=='buy':
#                             query=f''' SELECT i.member_id from Info.info_member_info as i  inner join (select distinct member_id 
#                             from (select * from Email_performance.Email_{audience_action}  where purchasetime >= "{start_time}" and purchasetime  <= "{end_time}" 
#                             union all select * from Email_performance.Email_vice_{audience_action}  where purchasetime >= "{start_time}" and purchasetime  <= "{end_time}") as t ) as j on i.member_id=j.member_id ; ''' 
#                             cursor = self.con.execute(query) 
#                         elif audience_action=='click':
#                             query=f''' SELECT i.member_id from Info.info_member_info as i inner join (select distinct member_id 
#                             from (select * from Email_performance.Email_{audience_action}  where  {audience_action}_time >= "{start_time}" and  {audience_action}_time  <= "{end_time}" 
#                             union all select * from Email_performance.Email_vice_{audience_action}  where {audience_action}_time >= "{start_time}" and {audience_action}_time  <= "{end_time}") as t ) as j on i.member_id=j.member_id ; ''' 
#                             cursor = self.con.execute(query) 
#                     elif audience_type=='SMS':
#                         if audience_action=='buy':
#                             query=f''' SELECT i.member_id from Info.info_member_info as i inner join (select distinct member_id 
#                             from (select * from SMS_performance.SMS_{audience_action}  where purchasetime >= "{start_time}" and purchasetime  <= "{end_time}" 
#                             union all select * from SMS_performance.SMS_vice_{audience_action}  where purchasetime >= "{start_time}" and purchasetime  <= "{end_time}") as t ) as j on i.member_id=j.member_id ; 
#                             '''                         
#                             cursor = self.con.execute(query) 
#                         else:
#                             query=f''' SELECT i.member_id from Info.info_member_info as i inner join (select distinct member_id 
#                             from (select * from SMS_performance.SMS_{audience_action}  where  {audience_action}_time >= "{start_time}" and  {audience_action}_time  <= "{end_time}" 
#                             union all select * from SMS_performance.SMS_vice_{audience_action}  where {audience_action}_time >= "{start_time}" and {audience_action}_time  <= "{end_time}") as t ) as b on i.member_id=j.member_id  ; ''' 
#                             cursor = self.con.execute(query) 
#                     elif audience_type=='Facebook':
#                         if audience_action=='buy':
#                             query=f''' SELECT a.member_id from Info.info_member_info as a inner join (select member_id from Facebook.facebook_{audience_action} where purchasetime between "{start_time}" and "{end_time}") as b on a.member_id=b.member_id ; ''' 
#                             cursor = self.con.execute(query) 
#                         else:
#                             query=f''' SELECT a.member_id from Info.info_member_info as a inner join (select member_id from Facebook.facebook_{audience_action} where {audience_action}_time between "{start_time}" and "{end_time}") as b on a.member_id=b.member_id ; ''' 
#                             cursor = self.con.execute(query) 
#                     elif audience_type=='Line':
#                         if audience_action=='buy':
#                             query=f''' SELECT a.member_id from Info.info_member_info as a inner join (select member_id from Line.line_{audience_action} where purchasetime between "{start_time}" and "{end_time}") as b on a.member_id=b.member_id  ; ''' 
#                             cursor = self.con.execute(query) 
#                         else:
#                             query=f''' SELECT a.member_id from Info.info_member_info as a inner join (select member_id from Line.line_{audience_action} where {audience_action}_time between "{start_time}" and "{end_time}") as b on a.member_id=b.member_id  ; ''' 
#                             cursor = self.con.execute(query) 

#             elif filter=='rule':
#                 rule_id=df['ruleID'].values[0]
#                 if audience_type=='Email':
#                     if audience_action=='open':
#                         query=f''' SELECT i.member_id from Info.info_member_info as  i inner join (SELECT distinct member_id FROM 
#                         (select * from Email_performance.Email_view 
#                         where ruleID ="{rule_id}" union all select c.ruleID,c.member_id,c.open_time,c.track,c.SectionID from 
#                         (select * from Email_performance.Email_vice_view) as c 
#                         inner join (select a.Name from ( select * from Email_schedule.schedule_vice_tracking) as  a  
#                         inner join  (select * from Email_schedule.schedule_tracking b where Name="{rule_id}")as b  
#                         on a.script_id=b.script_id) as t ) as x )as j on i.member_id=j.member_id  ; ''' 
#                         cursor = self.con.execute(query) 
#                     elif audience_action=='buy':
#                         query=f''' SELECT i.member_id  from Info.info_member_info as  i inner join (SELECT distinct member_id FROM (select * from Email_performance.Email_buy 
#                         where ruleID ="{rule_id}" union all select c.ruleID,c.member_id,c.purchasetime,c.transaction_id from 
#                         (select * from Email_performance.Email_vice_buy) as c 
#                         inner join (select a.Name from ( select * from Email_schedule.schedule_vice_tracking) as  a  
#                         inner join  (select * from Email_schedule.schedule_tracking b where Name="{rule_id}")as b  
#                         on a.script_id=b.script_id) as t ) as x  )as j on i.member_id=j.member_id  ; ''' 
#                         cursor = self.con.execute(query) 
#                     elif audience_action=='click':
#                         query=f''' SELECT i.member_id  from Info.info_member_info as  i inner join (SELECT distinct member_id FROM (select * from Email_performance.Email_click 
#                         where ruleID ="{rule_id}" union all select c.ruleID,c.member_id,c.click_time,c.track,c.SectionID from 
#                         (select * from Email_performance.Email_vice_click) as c 
#                         inner join (select a.Name from ( select * from Email_schedule.schedule_vice_tracking) as  a  
#                         inner join  (select * from Email_schedule.schedule_tracking b where Name="{rule_id}")as b  
#                         on a.script_id=b.script_id) as t ) as x)as j on i.member_id=j.member_id ; ''' 
#                         cursor = self.con.execute(query) 
#                 elif audience_type=='SMS':
#                     if audience_action=='buy':
#                         query=f''' SELECT i.member_id  from Info.info_member_info as  i inner join
#                         (SELECT distinct member_id  FROM (select * from SMS_performance.SMS_buy 
#                         where ruleID ="{rule_id}" union all select c.ruleID,c.member_id,c.purchasetime,c.transaction_id,c.track from 
#                         (select * from SMS_performance.SMS_vice_buy) as c 
#                         inner join (select a.Name from ( select * from SMS_schedule.schedule_vice_tracking) as  a  
#                         inner join  (select * from SMS_schedule.schedule_tracking b where Name="{rule_id}")as b  
#                         on a.script_id=b.script_id) as t ) as x ) as j on i.member_id=j.member_id  ; ''' 
#                         cursor = self.con.execute(query) 
#                     else:
#                         query=f''' SELECT i.member_id  from Info.info_member_info as  i inner join 
#                         (SELECT distinct member_id  FROM (select * from SMS_performance.SMS_click 
#                         where ruleID ="{rule_id}" union all select c.ruleID,c.member_id,c.click_time,c.track,c.SectionID from 
#                         (select * from SMS_performance.SMS_vice_click) as c 
#                         inner join (select a.Name from ( select * from SMS_schedule.schedule_vice_tracking) as  a  
#                         inner join  (select * from SMS_schedule.schedule_tracking b where Name="{rule_id}")as b  
#                         on a.script_id=b.script_id) as t ) as x )  as i on i.member_id=j.member_id ; ''' 
#                         cursor = self.con.execute(query) 
#                 elif audience_type=='Facebook':
#                     if audience_action=='buy':
#                         query=f''' SELECT a.member_id  from Info.info_member_info as  a inner join (select member_id from Facebook.facebook_{audience_action} where ruleID ="{rule_id}") as b on a.member_id=b.member_id ; ''' 
#                         cursor = self.con.execute(query) 
#                     else:
#                         query=f''' SELECT a.member_id  from Info.info_member_info as  a inner join (select member_id from Facebook.facebook_{audience_action} where ruleID ="{rule_id}") as b on a.member_id=b.member_id ; ''' 
#                         cursor = self.con.execute(query) 
#                 elif audience_type=='Line':
#                     if audience_action=='buy':
#                         query=f''' SELECT a.member_id  from Info.info_member_info as  a inner join (select member_id from Line.line_{audience_action} where ruleID ="{rule_id}") as b on a.member_id=b.member_id ; ''' 
#                         cursor = self.con.execute(query) 
#                     else:
#                         query=f''' SELECT a.member_id  from Info.info_member_info as  a inner join (select member_id from Line.line_{audience_action} where ruleID ="{rule_id}") as b on a.member_id=b.member_id ; ''' 
#                         cursor = self.con.execute(query) 
                    
                        
                        
#             elif filter=='smart':
#                 if audience_type=='Email':
#                     if audience_action!='all':
#                         query=f''' select a.member_id from Info.info_member_info as  a inner join (select member_id FROM Email_performance.Email_user  where `{audience_action}`="高") as b on a.member_id=b.member_id ; ''' 
#                     else:
#                         query=f''' select a.member_id  from Info.info_member_info as  a inner join (select member_id FROM Email_performance.Email_user ) as b on a.member_id=b.member_id  ; ''' 
#                     cursor = self.con.execute(query) 
#                 elif audience_type=='SMS':
#                     if audience_action!='all':
#                         query=f''' select a.member_id from Info.info_member_info as  a inner join (select member_id FROM SMS_performance.SMS_user  where `{audience_action}`="高") as b on a.member_id=b.member_id ; ''' 
#                     else:
#                         query=f''' select a.member_id from Info.info_member_info as  a inner join (select member_id FROM SMS_performance.SMS_user ) as b on a.member_id=b.member_id  ; ''' 
#                     cursor = self.con.execute(query) 

#                 elif audience_type=='Facebook':
#                     if audience_action!='all':
#                         query=f''' select a.member_id from Info.info_member_info as  a inner join (select member_id FROM Facebook.facebook_user  where `{audience_action}`="高") as b on a.member_id=b.member_id ; ''' 
#                     else:
#                         query=f''' select a.member_id from Info.info_member_info as  a inner join (select member_id FROM Facebook.facebook_user ) as b on a.member_id=b.member_id  ; ''' 
#                     cursor = self.con.execute(query) 
#                 elif audience_type=='Line':
#                     if audience_action!='all':
#                         query=f''' select a.member_id from Info.info_member_info as  a inner join (select member_id FROM Line.line_user  where `{audience_action}`="高") as b on a.member_id=b.member_id ; ''' 
#                     else:
#                         query=f''' select a.member_id from Info.info_member_info as  a inner join (select member_id FROM Line.line_user ) as b on a.member_id=b.member_id ; ''' 

#                     cursor = self.con.execute(query) 
                    
#             data = cursor.fetchall()
#             data = [d[0] for d in data]
#         else:
#             data=[]

#         return data

##################### label function

# 基本資料
def basic(cdb_id,tag,value):

    
    cf_id=get_cf_id(cdb_id)
    engine,con=connect_DB("clickforce")
    path=f'''SELECT name,id FROM {cf_id}.name where type ="會員";'''
    cursor=con.execute(path)
    trans=dict(cursor.fetchall())
    con.close()
    engine.dispose()


    engine,con=connect_DB(cdb_id)
    if tag=="性別":
        path=f'''select member_id from Info.info_member_info where gender in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="年齡級距":
        path=f'''select member_id from Info.info_member_info where age between {value["low"]} and {value["high"]}  '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="生日":
        path=f'''select member_id from Info.info_member_info where birth_month in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    
    elif tag=="城市":
        path=f'''select member_id from Info.info_member_info where city in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="線下門市名稱":
        path=f'''select member_id from Info.info_member_info where store_source in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="會員等級":
        path=f'''select member_id from Info.info_member_info where level in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="註冊管道":
        path=f'''select member_id from Info.info_member_info where source in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="註冊日期":
        path=f'''select member_id from Info.info_member_info where join_date between "{value["low"]}" and "{value["high"]}"'''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="會員品牌歸屬": 

        value=[trans[x] for x in value]
        path=f'''select member_id from Info.info_member_info where type in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())

    else:
        df=pd.DataFrame(columns=["member_id"])
    engine.dispose()
    con.close()
    return(list(df["member_id"].apply(str))) 

    
# 再行銷分群
def remarketing_label(cdb_id,web,tag,value,more_option=None):
    engine,con=connect_DB(cdb_id)

    start_time=None
    end_time=None
    ruleID=None

    if more_option:
        ruleID=more_option.get("event")
        time_range=more_option.get("time_range")
        if time_range:
            day=time_range.get("days")
            end_time=time_range.get("end_time")
            start_time=time_range.get("start_time")
            if day:
                end_time=datetime.now()+timedelta(hours=8)
                start_time=datetime.now()+timedelta(hours=8)-timedelta(days=day)
      
            
    
    
    



        
    if (not start_time) and (not end_time) and (not ruleID):
        if web=="Email":

            if tag=='可寄送名單':
                query=f''' SELECT member_id FROM Info.info_member_status where email="normal"; ''' 
                cursor = con.execute(query) 
                df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ str(x) if "@" not in str(x) else  0 for x in df.member_id ]
                df.member_id=[ 0 if str(x).startswith("09") else x for x in df.member_id]
                df= df[df.member_id!=0]

            elif tag=='互動評級':
                query=f''' select member_id FROM Email_performance.Email_user WHERE response in {tuple(value+["",""])}; '''  
                cursor = con.execute(query) 
                df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]


            elif tag=='有寄出過':
                query=f''' select distinct member_id 
                from (select member_id from Email_schedule.schedule_recipient_record 
                      union 
                      select member_id from Email_schedule.schedule_vice_recipient_record
                      union 
                      select member_id from Script_schedule.email_recipient_record
                      
                      ) i; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[  0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]

            elif tag=='有開信過':
                query=f''' select distinct member_id from (select member_id from Email_performance.Email_view 
                            union 
                            select member_id from Email_performance.Email_vice_view
                            union 
                            select member_id from Script_performance.email_view) i; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]
                
            elif tag=='有點擊過':
                query=f'''select distinct member_id from (select member_id from Email_performance.Email_click  
                        union 
                        select member_id from Email_performance.Email_vice_click
                        union 
                        select member_id from Script_performance.email_click) i; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]

            elif tag=='有轉換過':
                query=f''' select distinct member_id 
                from (select member_id from Email_performance.Email_buy 
                      union 
                      select member_id from Email_performance.Email_vice_buy
                      union 
                      select member_id from Script_performance.email_buy ) i; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[0 if str(x).startswith("09") else x for x in df.member_id]
                df= df[df.member_id!=0]



        elif web== "SMS":
            if tag=='可寄送名單':
                query=f''' SELECT member_id FROM Info.info_member_status where sms="normal"; ''' 
                cursor = con.execute(query) 
                df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ str(x) if "@" not in str(x) else  0 for x in df.member_id ]
                df.member_id=[ 0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]

            elif tag=='有開信過':
                
                return([])
            elif tag=='互動評級':
                query=f''' select member_id FROM SMS_performance.SMS_user WHERE response in {tuple(value+["",""])}; '''  
                cursor = con.execute(query) 
                df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]
              
            elif tag=='有寄出過':
                query=f''' select distinct member_id 
                from (select member_id from SMS_schedule.schedule_recipient_record 
                      union 
                      select member_id from SMS_schedule.schedule_vice_recipient_record
                      union
                      SELECT member_id FROM Script_performance.sms_record) i; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[ 0 if str(x).startswith("09") else x for x in df.member_id]
                df= df[df.member_id!=0]


            elif tag=='有點擊過':
                query=f'''select distinct member_id 
                from (select member_id from SMS_performance.SMS_click  
                      union
                      select member_id from SMS_performance.SMS_vice_click
                      union
                      select member_id from  Script_performance.sms_click ) i; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[ 0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]

            elif tag=='有轉換過':
                query=f''' select distinct member_id 
                from (select member_id from SMS_performance.SMS_buy  
                      union 
                      select member_id from SMS_performance.SMS_vice_buy 
                      union
                      select member_id from  Script_performance.sms_buy ) i; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]
    

    elif start_time and end_time:
        
        if web=="Email":
            
            if tag=='有寄出過':
                query=f''' select distinct member_id 
                from (select member_id from Email_schedule.schedule_recipient_record  where send_time >= "{start_time}" and send_time  <= "{end_time}" 
                        union
                        select member_id from Email_schedule.schedule_vice_recipient_record  where send_time >= "{start_time}" and send_time  <= "{end_time}"
                        union
                        select member_id from Script_performance.email_record  where send_time >= "{start_time}" and send_time  <= "{end_time}"
                        ) t  ; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[ 0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]


            elif tag=='有開信過':
                query=f''' select distinct member_id 
                from (select member_id from Email_performance.Email_view  where open_time >= "{start_time}" and open_time  <= "{end_time}" 
                    union 
                      select member_id from Email_performance.Email_vice_view  where open_time >= "{start_time}" and open_time  <= "{end_time}"
                    union
                      SELECT member_id FROM Script_performance.email_view where  open_time >= "{start_time}" and  open_time  <= "{end_time}"
                    ) t  ; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[ 0 if str(x).startswith("09") else x for x in df.member_id]
                df= df[df.member_id!=0]
            
            elif tag=='有寄出過':
                
                return([])

            elif tag=='有點擊過':
                query=f''' select distinct member_id 
                from (select member_id from Email_performance.Email_click  where click_time >= "{start_time}" and click_time  <= "{end_time}" 
                    union
                    select member_id from Email_performance.Email_vice_click  where click_time >= "{start_time}" and click_time  <= "{end_time}"
                    union
                    SELECT member_id FROM Script_performance.email_click  where  click_time >= "{start_time}" and  click_time  <= "{end_time}") t; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]

            elif tag=='有轉換過':
                query=f''' select distinct member_id 
                from (select member_id from Email_performance.Email_buy  where  purchasetime >= "{start_time}" and   purchasetime  <= "{end_time}" 
                     union 
                     select member_id from Email_performance.Email_vice_buy  where  purchasetime >= "{start_time}" and  purchasetime  <= "{end_time}"
                     union
                     SELECT member_id FROM Script_performance.email_buy where  purchasetime >= "{start_time}" and  purchasetime  <= "{end_time}"

                ) t; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]


        elif web=="SMS":
           
            if tag=='有寄出過':
                query=f''' select distinct member_id 
                from (select member_id from SMS_schedule.schedule_recipient_record  where send_time >= "{start_time}" and send_time  <= "{end_time}" 
                        union
                        select member_id from SMS_schedule.schedule_vice_recipient_record  where send_time >= "{start_time}" and send_time  <= "{end_time}"
                        union
                        select member_id from Script_performance.sms_record  where send_time >= "{start_time}" and send_time  <= "{end_time}") t  ; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[ 0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]

            elif tag=='有開信過':
                return([])

            if tag=='有點擊過':
                query=f'''select distinct member_id 
                from (select member_id from SMS_performance.SMS_click  where click_time >= "{start_time}" and click_time  <= "{end_time}" 
                       union 
                       select member_id from SMS_performance.SMS_vice_click  where click_time >= "{start_time}" and click_time  <= "{end_time}"
                       union 
                       SELECT member_id FROM Script_performance.sms_click  where  purchasetime >= "{start_time}" and  purchasetime  <= "{end_time}") t; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[ 0 if str(x).startswith("09")  else x for x in df.member_id]
                df= df[df.member_id!=0]

            elif tag=='有轉換過':
                query=f''' select distinct member_id 
                from 
                (select member_id from SMS_performance.SMS_buy  where  purchasetime >= "{start_time}" and   purchasetime  <= "{end_time}" 
                 union 
                 select member_id from SMS_performance.SMS_vice_buy  where  purchasetime >= "{start_time}" and  purchasetime  <= "{end_time}"
                 union
                 SELECT member_id FROM Script_performance.sms_buy  where  purchasetime >= "{start_time}" and  purchasetime  <= "{end_time}") t; ''' 
                cursor = con.execute(query) 
                df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                df.member_id=[0 if str(x).startswith("09") else x for x in df.member_id]
                df= df[df.member_id!=0]
                
    
        
    elif ruleID:
        
        if web=="Email":
            if ruleID.startswith("ve_"):
                if tag=='有寄出過':
                    query=f''' SELECT distinct member_id FROM Script_performance.email_record where ruleID like "{ruleID+"%%"} '''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[ 0 if x.startswith("09")  else x for x in df.member_id]
                    df= df[df.member_id!=0]

                elif tag=='有開信過':
                    query=f''' SELECT distinct member_id FROM Script_performance.email_record where ruleID like "{ruleID+"%%"} and open = 1'''
                    cursor = con.execute(query)  
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                    df= df[df.member_id!=0]


                elif tag=='有點擊過':
                    query=f''' SELECT distinct member_id FROM Script_performance.email_record where ruleID like "{ruleID+"%%"} and click=1'''
                    cursor = con.execute(query)  
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[ 0 if str(x).startswith("09") else x for x in df.member_id]
                    df= df[df.member_id!=0]

                        
                elif tag=='有轉換過':
                    query=f''' SELECT distinct member_id FROM Script_performance.email_record where ruleID like "{ruleID+"%%"} and conversion=1'''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[ 0 if str(x).startswith("09")  else x for x in df.member_id]
                    df= df[df.member_id!=0]

            
            else:
           

                if tag=='有寄出過':
                    query=f''' SELECT distinct member_id FROM Email_schedule.schedule_recipient_record where ruleID like "{ruleID+"%%"}"'''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[ 0 if str(x).startswith("09")  else x for x in df.member_id]
                    df= df[df.member_id!=0]

                elif tag=='有開信過':
                    query=f''' SELECT distinct member_id FROM Email_performance.Email_record  where ruleID like "{ruleID+"%%"}" and open=1'''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[ 0 if str(x).startswith("09") else x for x in df.member_id]
                    df= df[df.member_id!=0]



                elif tag=='有點擊過':

                    query=f''' SELECT distinct member_id FROM Email_performance.Email_record where ruleID like "{ruleID+"%%"}" and click=1'''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[0 if str(x).startswith("09") else x for x in df.member_id]
                    df= df[df.member_id!=0]

                elif tag=='有轉換過':
                    query=f''' SELECT distinct member_id FROM Email_performance.Email_record where ruleID like "{ruleID+"%%"}" and conversion=1'''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                    df= df[df.member_id!=0]



    
        elif web=="SMS":
            if ruleID.startswith("vs_"):
                if tag=='有寄出過':
                    query=f''' SELECT distinct member_id FROM Script_performance.SMS_record where ruleID like "{ruleID+"%%"}"'''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                    df= df[df.member_id!=0]
                
                elif tag=='有開信過':
                    return([])
            
                elif tag=='有點擊過':
                    query=f''' SELECT distinct member_id FROM Script_performance.SMS_record where ruleID like "{ruleID+"%%"}" and click=1'''
                    cursor = con.execute(query)  
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[ 0 if str(x).startswith("09")  else x for x in df.member_id]
                    df= df[df.member_id!=0]

                        
                elif tag=='有轉換過':
                    query=f''' SELECT distinct member_id FROM Script_performance.SMS_record where ruleID like "{ruleID+"%%"}" and conversion=1'''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[ 0 if str(x).startswith("09") else x for x in df.member_id]
                    df= df[df.member_id!=0]


            else:
                if tag=='有寄出過':
                    query=f''' SELECT distinct member_id FROM SMS_schedule.schedule_recipient_record where ruleID="{ruleID}"'''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[0 if str(x).startswith("09")  else x for x in df.member_id]
                    df= df[df.member_id!=0]

                elif tag=='有開信過':
                    return([])
            
                elif tag=='有點擊過':
                    query=f''' SELECT distinct member_id FROM SMS_performance.SMS_record where ruleID="{ruleID}" and click=1'''
                    cursor = con.execute(query)  
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[ 0 if str(x).startswith("09") else x for x in df.member_id]
                    df= df[df.member_id!=0]

                        
                elif tag=='有轉換過':
                    query=f''' SELECT distinct member_id FROM SMS_performance.SMS_record where ruleID="{ruleID}" and conversion=1'''
                    cursor = con.execute(query) 
                    df= pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
                    df.member_id=[ x if "@" not in x else  0 for x in df.member_id ]
                    df.member_id=[0 if str(x).startswith("09") else x for x in df.member_id]
                    df= df[df.member_id!=0]

    engine.dispose()
    con.close()
    return(list(df["member_id"].apply(str))) 
# 價值分群
def value_label(cdb_id,tag,value):
    engine,con=connect_DB(cdb_id)
    if tag=="價值分群":
        path=f'''select member_id from Info.info_member_label where value_label in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())

    engine.dispose()
    con.close()
    return(list(df["member_id"].apply(str)))

# AI 標籤
def ai_label(cdb_id,tag,value):
    engine,con=connect_DB(cdb_id)

    if tag=="消費貢獻力":
        path=f'''select member_id from Info.info_member_behavior where b0 in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="顧客續購程度":
        path=f'''select member_id from Info.info_member_behavior where b1 in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="流失風險":
        path=f'''select member_id from Info.info_member_behavior where b2 in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="線上轉換潛力":
        path=f'''select member_id from Info.info_member_behavior where b3 in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="廣告回應程度":
        path=f'''select member_id from Info.info_member_behavior where b4 in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="顧客生命週期":
        path=f'''select member_id from Info.info_member_behavior where b5 in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    elif tag=="顧客終身價值":
        path=f'''select member_id from Info.info_member_behavior where b7 in {tuple(value+["",""])} '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    else:
        df=pd.DataFrame(columns=["member_id"])

    engine.dispose()
    con.close()
    return(list(df["member_id"].apply(str)))



# 購買行為
def purchase_behavior(cdb_id,web,tag,value,more_option):
    
    trans={"線上":"(1)","線下":"(0)","整體":"(0,1)"}
    count_trans={"-":"=","以上(含)":">=","以下(含)":"<="}
    cost_trans={"大於":">=","小於":"<="}
    
    
    if more_option:
        time_range=more_option.get("time_range")
        if time_range:
            day=time_range.get("days")
            end_time=time_range.get("end_time")
            start_time=time_range.get("start_time")
            if day:
                end_time=datetime.now()+timedelta(hours=8)
                start_time=datetime.now()+timedelta(hours=8)-timedelta(days=day)
        
        counts=more_option.get("count")
        count=counts.get("count")
        if count:
            moreOrless=counts.get("moreOrless")        
            count=count_trans[moreOrless]+str(count)
        else:
            count=">0"


        costs=more_option.get("cost")
        m=costs.get("moreOrlessForCost")
        if m:
            
            cost=costs.get("cost")
            lowCost=costs.get("lowCost")
            highCost=costs.get("highCost")
            if cost:
                cost_content=cost_trans[m]+str(cost)
            elif lowCost and highCost :
                cost_content=f"Between {lowCost} and {highCost}"
            else:
                cost_content=">0"
        else:
            cost_content=">0"
        
    else:
        end_time=datetime.now()+timedelta(hours=8)
        start_time=datetime(2000,1,1)
        count=">0"
        cost_content=">0"
        
    engine,con=connect_DB(cdb_id)

    if tag=="未購買的人":
        path=f'''select member_id from Info.info_member_info where online in {trans[web]}'''
        yes=f'''select distinct  member_id from Info.info_purchase_record where date between "{start_time}" and "{end_time}" '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=["member_id"])
        
        cursor=con.execute(yes)
        yes=pd.DataFrame(cursor.fetchall(),columns=["member_id"])
        df=set(df.member_id.apply(str))-set(yes.member_id.apply(str))
    
    elif tag=="有購買的人":
        
        yes1=f'''select member_id from Info.info_purchase_record where member_id is not null and online in {trans[web]} and  date between "{start_time}" and "{end_time}" group by member_id having count(distinct order_id) {count}; '''
        cursor=con.execute(yes1)
        yes1=pd.DataFrame(cursor.fetchall(),columns=["member_id"])

        yes2=f'''select member_id from ( select member_id,order_id,sum(sales) {cost_content} re from Info.info_purchase_record where member_id is not null and online in {trans[web]} and  date between "{start_time}" and "{end_time}" group by member_id,order_id ) i group by member_id having avg(re)=1; '''
        cursor=con.execute(yes2)
        yes2=pd.DataFrame(cursor.fetchall(),columns=["member_id"])
        df=set(yes1.member_id.apply(str))-(set(yes1.member_id.apply(str))-set(yes2.member_id.apply(str)))
        
    elif tag=="首購的人":
        
        path = f'''
        SELECT * FROM Info.info_member_info where online in {trans[web]} and join_date between  "{start_time}" and "{end_time}";'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())  
    
        yes2=f'''select member_id from ( select member_id,order_id,sum(sales) {cost_content} re from Info.info_purchase_record where member_id is not null and online in {trans[web]} and  date between "{start_time}" and "{end_time}" group by member_id,order_id ) i group by member_id having avg(re)=1; '''
        cursor=con.execute(yes2)
        a = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        a["member_id"]=a["member_id"].apply(eval)
        if len(a):
            l=df.member_id.astype(str).unique()
            b=a.member_id.astype(str).unique()
            df=set(l)-(set(l)-set(b))
        else:
            df=[]
    
    elif tag=="購買的商品名稱":
        
        path = f'''
        SELECT product_id FROM Info.info_brand where product_name in  {tuple(value+["",""])} ;'''
        cursor = con.execute(path)
        product=pd.DataFrame(cursor.fetchall(),columns=["product"])
        product=product["product"].to_list()
        
        path=f'''select member_id from Info.info_purchase_record where product_id in {tuple(product+["",""])} and online in {trans[web]} and  date between "{start_time}" and "{end_time}" group by member_id having count(distinct order_id) {count} and avg(sales {cost_content})=1 ; '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=["member_id"])
        df=set(df.member_id.apply(str))
        
    elif tag=="購買的類別名稱":
        
        path = f'''
        SELECT product_id FROM Info.info_brand where category_name in {tuple(value+["",""])};'''
        cursor = con.execute(path)
        product=pd.DataFrame(cursor.fetchall(),columns=["product"])
        product=product["product"].to_list()

        path=f'''select member_id from Info.info_purchase_record where product_id in {tuple(product+["",""])} and  online in {trans[web]} and  date between "{start_time}" and "{end_time}" group by member_id having count(distinct order_id) {count} and avg(sales {cost_content})=1; '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=["member_id"])
        df=df[df.member_id.notnull()]
        df=set(df.member_id.apply(str))
    elif tag=="購買的品牌名稱":
        
        path = f'''
        SELECT product_id FROM Info.info_brand where brand_name in {tuple(value+["",""])};'''
        cursor = con.execute(path)
        product=pd.DataFrame(cursor.fetchall(),columns=["product"])
        product=product["product"].to_list()

        path=f'''select member_id from Info.info_purchase_record where product_id  in {tuple(product+["",""])} and  online in {trans[web]} and  date between "{start_time}" and "{end_time}" group by member_id having count(distinct order_id) {count} and avg(sales {cost_content})=1; '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=["member_id"])
        df=set(df.member_id.apply(str))
    
    elif tag=="消費金額分級":
        
        path = f'''
        SELECT member_id FROM Info.info_member_label where monetary in {tuple(value+["",""])};'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())["member_id"].apply(str)
    elif tag=="累計消費金額":
        
        m=value.get("moreOrless")
        if m:
            
            cost=value.get("cost")
            if cost:
                cost_value=cost_trans[m]+str(cost)
            else:
                lowCost=value.get("low")
                highCost=value.get("high")
                cost_value=f"Between {lowCost} and {highCost}"
        else:
            cost_value=">0"


        path=f'''select member_id from Info.info_purchase_record where  online in {trans[web]} and  date between "{start_time}" and "{end_time}" group by member_id having sum(sales) {cost_value}; '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=["member_id"])
        df=set(df.member_id.apply(str))



    elif tag=="客單價分級":
        
        path = f'''
        SELECT member_id FROM Info.info_member_label where AOV in {tuple(value+["",""])};'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())["member_id"].apply(str)

    elif tag=="平均客單價":
        
        m=value.get("moreOrless")
        if m:
            
            cost=value.get("cost")
            if cost:
                cost_value=cost_trans[m]+str(cost)
            else:
                lowCost=value.get("low")
                highCost=value.get("high")
                cost_value=f"Between {lowCost} and {highCost}"
        else:
            cost_value=">0"

        path=f'''select member_id from Info.info_purchase_record where  online in {trans[web]} and  date between "{start_time}" and "{end_time}" group by member_id having (sum(sales)/count(distinct order_id)) {cost_value}; '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=["member_id"])
        df=set(df.member_id.apply(str))



    elif tag=="消費頻率分級":
        
        path = f'''
        SELECT member_id FROM Info.info_member_label where frequency in {tuple(value+["",""])};'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())["member_id"].apply(str) 

    elif tag=="累計消費次數":
        
        m=value.get("moreOrless")
        if m:
            
            cost=value.get("cost")
            if cost:
                cost_value=cost_trans[m]+str(cost)
            else:
                lowCost=value.get("low")
                highCost=value.get("high")
                cost_value=f"Between {lowCost} and {highCost}"
        else:
            cost_value=">0"

        path=f'''select member_id from Info.info_purchase_record where  online in {trans[web]} and  date between "{start_time}" and "{end_time}" group by member_id having count(distinct order_id) {cost_value}; '''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=["member_id"])
        df=set(df.member_id.apply(str))



    elif tag=="距離上次購買分級":
        
        path = f'''
        SELECT member_id FROM Info.info_member_label where last_purchase in {tuple(value+["",""])};'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())["member_id"].apply(str) 
    elif tag=="距離上次購買天數區隔":

        low=value.get("low") 
        high=value.get("high")
      
        
        if web=="線上":
            path = f'''
            SELECT distinct member_id FROM Info.info_member_stat_online where R between {low} and {high};'''
        elif web=="線下":
            path = f'''
            SELECT distinct member_id FROM Info.info_member_stat_offline where R between {low} and {high};'''
        elif web=="整體":
            path = f'''
            SELECT distinct member_id FROM Info.info_member_stat where R between {low} and {high};'''


        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())["member_id"].apply(str)
    else:
        df=[]

    engine.dispose()
    con.close()
    return(list(df)) 

# 域動標籤
def cf_behavior(cdb_id,tag,value,more_option,member=True):
    
    if more_option:
        time_range=more_option.get("time_range")
        if time_range:
            day=more_option.get("days")
            end_time=more_option.get("end_time")
            start_time=more_option.get("start_time")
            if day:
                end_time=datetime.now()+timedelta(hours=8)
                start_time=datetime.now()+timedelta(hours=8)-timedelta(days=day)  
        
    else:
        end_time=datetime.now()+timedelta(hours=8)
        start_time=datetime(2000,1,1)

    
                
     
            
    
    cf_id=get_cf_id(cdb_id)
    engine,con=connect_DB("clickforce")
    path=f"SELECT cid,member_id FROM {cf_id}.mapping;"
    cursor=con.execute(path)
    cid_trans=dict(cursor.fetchall())
    con.close()
    engine.dispose()
    
    
    
    
    engine,con=connect_DB(cdb_id)
    
    if tag=="興趣標籤":
        path=f'''SELECT ga_id cid FROM CF_label.interest_prefer where value in {tuple(value+["",''])} and  date between "{start_time}" and "{end_time}";'''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=["cid"])
        
    if tag =="關注媒體類型標籤":
        path=f'''SELECT ga_id cid FROM CF_label.media_prefer where value in {tuple(value+["",''])} and  date between "{start_time}" and "{end_time}";'''
        cursor=con.execute(path)
        df=pd.DataFrame(cursor.fetchall(),columns=["cid"])
    engine.dispose()
    con.close()
    out=list(df["cid"])
    if member:
        out=[str(cid_trans[x]) for x in out if x in cid_trans.keys()] 
    return(out) 

# 網站行為標籤
def online_behavior(cdb_id,web,tag,value,more_option,member=True):
    
    cf_id=get_cf_id(cdb_id)
    engine,con=connect_DB("clickforce")
    path=f"SELECT cid,member_id FROM {cf_id}.mapping;"
    cursor=con.execute(path)
    cid_trans=dict(cursor.fetchall())
    
    path=f"SELECT name,id FROM {cf_id}.name where type='標題';"
    cursor=con.execute(path)
    trans=dict(cursor.fetchall())
    con.close()
    engine.dispose()
    ids=trans[web]
    
    
     
    count_trans={"-":"=","以上(含)":">=","以下(含)":"<="}
    
    
    if more_option:
        time_range=more_option.get("time_range")
        if time_range:
            day=time_range.get("days")
            end_time=time_range.get("end_time")
            start_time=time_range.get("start_time")
            if day:
                end_time=datetime.now()+timedelta(hours=8)
                start_time=datetime.now()+timedelta(hours=8)-timedelta(days=day)
        
        counts=more_option.get("count")
        count=counts.get("count")
        if count:
            moreOrless=counts.get("moreOrless")        
            count=count_trans[moreOrless]+str(count)
        else:
            count=">0"

    else:
        end_time=datetime.now()+timedelta(hours=8)
        start_time=datetime(2000,1,1)
        count=">0"
                
                 
    engine,con=connect_DB(cdb_id)
    
    if tag =="造訪商品名稱":
        
        table="view_"+cdb_id+str(ids)
        
        path=f'''select cid from CF_label.`{table}` where item in {tuple(value+["",""])} and date between "{start_time}" and "{end_time}" group by cid having count(item) {count}'''
        cursor = con.execute(path) 
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]
    
    elif tag =="造訪商品類別":
        
        path=f'''SELECT product_name FROM Info.info_brand where category_name in {tuple(value+["",""])};'''
        cursor = con.execute(path)
        product = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(product):
            df= product["product_name"].to_list() 
        else:
            df=[]
        
        
        
        
        table="view_"+cdb_id+str(ids)
        engine,con=connect_DB(cdb_id)
        path=f'''select cid from CF_label.`{table}` where item in {tuple(product+["",""])} and date between "{start_time}" and "{end_time}" group by cid having count(item) {count}'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]

    elif tag =="造訪品牌名稱":
        
        path=f'''SELECT product_name FROM Info.info_brand where brand_name in {tuple(value+["",""])};'''
        cursor = con.execute(path)
        product = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(product):
            df= product["product_name"].to_list() 
        else:
            df=[]
        
       
        
        
        table="view_"+cdb_id+str(ids)
        engine,con=connect_DB(cdb_id)
        path=f'''select cid from CF_label.`{table}` where item in {tuple(product+["",""])} and date between "{start_time}" and "{end_time}" group by cid having count(item) {count}'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]
                                                                                     
    elif tag =="造訪頁面類別":
        a=list(value.values())[0]
        b=list(value.keys())[0]
        table="view_"+cdb_id+str(ids)
        table2="refer_"+cdb_id+str(ids)

        engine,con=connect_DB("clickforce")
        path=f"SELECT name,id FROM {cf_id}.name where type='標題';"
        cursor=con.execute(path)
        trans=dict(cursor.fetchall())
        ids=trans[web]

        path=f"SELECT name,type FROM {cf_id}.name ;"
        cursor=con.execute(path)
        trans2=dict(cursor.fetchall())

        con.close()
        engine.dispose()
        

        engine,con=connect_DB(cdb_id)
        path=f'''SELECT genre,title FROM Online.`{table2}` where genre not like "%%0%%" group by title,genre;'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        df=df.groupby("genre").aggregate(list).reset_index()
        trans1=dict(zip(df.genre,df.title))


        
        if len(a)==0:
            if b not in ["menu","首頁","banner"]:
                path=f'''select cid from CF_label.`{table}` where  item in {tuple(trans1[trans2[b]]+["",""])} and date between "{start_time}" and "{end_time}" group by cid having count(item) {count}'''
            elif b== "menu":
                path=f'''select cid from CF_label.`{table}` where item in {tuple(trans1["menu"]+["",""])} and date between "{start_time}" and "{end_time}" group by cid having count(item) {count}'''
            elif b =="首頁" :
                path=f'''select cid from CF_label.`{table}` where item in {tuple(trans1["首頁"]+["",""])} and date between "{start_time}" and "{end_time}" group by cid having count(item) {count}'''
            elif b =="banner" :
                path=f'''select cid from CF_label.`{table}` where item in {tuple(trans1["banner"]+["",""])} and date between "{start_time}" and "{end_time}" group by cid having count(item) {count}'''
            
        
        else:
            path=f'''select cid from CF_label.`{table}` where item in {tuple(list(a)+["",""])} and date between "{start_time}" and "{end_time}" group by cid having count(item) {count}'''
            
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        con.close()
        engine.dispose()
        if len(df):
            df=df["cid"]
        else:
            df=[]
       


                                                                           
    elif tag =="有造訪過網站":
        
        table="session_"+cdb_id+str(ids)
        
        path=f'''select cid from Online.`{table}` where start_time between "{start_time}" and "{end_time}" group by cid having count(cid) {count}'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]
    
    elif tag =="有登入過":
        
        table="session_"+cdb_id+str(ids)
        path=f'''select cid from Online.`{table}` where start_time between "{start_time}" and "{end_time}" and login=1 group by cid having count(cid) {count}'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]
                                                
    elif tag =="平均瀏覽時長":
        
        table="session_"+cdb_id+str(ids)
        path=f'''select cid from Online.`{table}` where start_time between "{start_time}" and "{end_time}"  group by cid having  avg(TIME_TO_SEC(timediff(end_time,start_time)))/60 between  {value["low"]} and {value["high"]}'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]
                                                                                
    elif tag =="有加入購物車但未結":
        
        table="cart_buy"+str(ids)
        path=f'''select cid from CF_label.`{table}` where date between "{start_time}" and "{end_time}"  group by cid having  sum(type="cart")>0 and sum(type="conversion")=0'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]
                                    
    elif tag =="有轉換過":
        
        table="cart_buy"+str(ids)
        path=f'''select cid from CF_label.`{table}` where date between "{start_time}" and "{end_time}"  group by cid having  sum(type="conversion")>0'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]
                                                                                     
    elif tag =="utm campaign source":
        
        table="utm_"+cdb_id+str(ids)
        path=f'''select distinct cid from CF_label.`{table}` where date between "{start_time}" and "{end_time}" and source in {tuple(value+['',''])}   '''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]
                                                                                     
    elif tag =="utm campaign medium":
        
        table="utm_"+cdb_id+str(ids)
        path=f'''select distinct cid from CF_label.`{table}` where date between "{start_time}" and "{end_time}" and medium in {tuple(value+['',''])}  '''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]

    elif tag =="utm campaign name":
        
        table="utm_"+cdb_id+str(ids)
        path=f'''select distinct cid from CF_label.`{table}` where date between "{start_time}" and "{end_time}"  and campaign in {tuple(value+['',''])}'''
        cursor = con.execute(path)
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
        if len(df):
            df=df["cid"]
        else:
            df=[]
                                                                                   
    engine.dispose()
    con.close()
    out=list(df)
    if member:
        out=[ str(cid_trans[x]) for x in out if x in cid_trans.keys()] 
    return(out) 



## 客制化標籤
def find(cdb_id,f):
    out={"laurel":{"LINE 營養研究室":laurel_custom_1,"LINE 桂冠食品":laurel_custom_2}}
    return out[cdb_id][f]

# 域動 客製標籤一
def laurel_custom_1(cdb_id,web,tag,value,more_option):

    engine,con=connect_DB(cdb_id)
    path=f'''SELECT Line_id FROM CF_label.custome2 where type = "{web.split("_")[-1]}" and value in {tuple(value+["",""])} ;'''
    cursor = con.execute(path)
    df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())

    path=f'''SELECT member_id FROM Info.info_member_info where Line_id in {tuple(list(df["Line_id"])+["",""])}'''
    cursor = con.execute(path)
    s1 = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())

    path=f'''SELECT member_id FROM Info.info_member_info where Line_id2 in {tuple(list(df["Line_id"])+["",""])}'''
    cursor = con.execute(path)
    s2 = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    df=pd.concat([s1,s2])


    con.close()
    engine.dispose()
    return list(df["member_id"].apply(str).unique())

# 域動 客製標籤二
def laurel_custom_2(cdb_id,web,tag,value,more_option):
    
    engine,con=connect_DB(cdb_id)
    path=f'''SELECT Line_id FROM CF_label.custome1 where type = "{web.split("_")[-1]}" and tag="{tag}" and value in {tuple(value+["",""])} ;'''
    cursor = con.execute(path)
    df = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())

    path=f'''SELECT member_id FROM Info.info_member_info where Line_id in {tuple(list(df["Line_id"])+["",""])}'''
    cursor = con.execute(path)
    s1 = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())

    path=f'''SELECT member_id FROM Info.info_member_info where Line_id2 in {tuple(list(df["Line_id"])+["",""])}'''
    cursor = con.execute(path)
    s2 = pd.DataFrame(cursor.fetchall(),columns=cursor.keys())
    df=pd.concat([s1,s2])


    con.close()
    engine.dispose()
    return list(df["member_id"].apply(str).unique())


