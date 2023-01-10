##### Airflow to schedule automaticly #####
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime
from airflow.operators.subdag import SubDagOperator
import sys
sys.path.append('/opt/airflow')

from lib.ssp_lambda import cap_factory_lambda, familymart_lambda
email = ["calvin@retailingdata.com.tw", "ader@retailingdata.com.tw"]

### Parameters for airfloe dags ###
default_args = {
    'owner': 'Calvin',
    'start_date': datetime(2022, 2, 24, 0, 0),
    'retries': 0,
    "weight_rule": "upstream",
    "priority_weight": 1
    }


### Airflow dags setting ###
with DAG('ssp', schedule_interval='0 0 */1 * *', catchup=False, default_args=default_args, tags=["ssp"]) as ssp_dags:
    
    ### cap factory ###
    # run etl code for cap factory #
    run_bottle = PythonOperator(
        task_id = 'cap_factory_lambda',
        python_callable = cap_factory_lambda,
        op_kwargs = {"cdb_id": "ssp_cap_factory"},
        provide_context = True )
    
    # Send email if code failed #
    send_email_run_bottle = EmailOperator(
        task_id = 'send_email_run_bottle',
        to = email,
        trigger_rule = "one_failed",
        subject = 'Airflow Alert',
        html_content = """ <h3>Something wrong has happened in run_bottle module.
        Please check on Airflow server for detail information </h3> """ )
       
    # Finish #
    finished_run_bottle = BashOperator(
        task_id='finished_run_bottle',
        trigger_rule="all_success",
        bash_command='echo Success' )
    
    ### familymart ###
    # run etl code for familymart #
    run_familymart_ssp = PythonOperator(
        task_id = "familymart_lambda",
        python_callable = familymart_lambda,
        op_kwargs = {"cdb_id": "ssp_familymart"},
        provide_context = True )
    
    # Send email if code failed #
    send_email_run_familymart = EmailOperator(
        task_id = "send_email_run_familymart",
        to = email,
        trigger_rule = "one_failed",
        subject = 'Airflow Alert',
        html_content = """ <h3>Something wrong has happened in run_familymart module.
        Please check on Airflow server for detail information!! </h3> """ )
    
    # Finish #
    finished_run_familymart = BashOperator(
        task_id='finished_run_familymart',
        trigger_rule = "all_success",
        bash_command = 'echo Success' )
    
    ### Define workflow ###
    run_bottle >> finished_run_bottle
    run_bottle >> send_email_run_bottle
    
    run_familymart_ssp >> finished_run_familymart
    run_familymart_ssp >> send_email_run_familymart



