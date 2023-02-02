from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
from datetime import datetime

report_period = '2022-03-01'

def _check_data_availability(report_period): 
    if report_period in ['2022-01-01', '2022-02-01']:
        return 'start_pipeline'
    else:
        return 'email_no_data'
        
def _email_no_data():
    print("No data in DB")

def _collect_data_1():
    return [1,2,3]

def _collect_data_2():
    return [4,5,6]

def _create_report(ti):
    d = ti.xcom_pull(task_ids = ['collect_data_1', 'collect_data_2'])
    print("1111111112222222")
    print(d)
    return d

def _show_result(ti):
    dd = ti.xcom_pull(task_ids = ['create_report'])
    print(pd.DataFrame(dd[0]))
	

with DAG(
	"b2b_churn_report", 
	start_date=datetime(2023, 1, 1),
	#schedule_interval="@daily", 
	catchup=False) as dag:

        check_data_availability = BranchPythonOperator(
            task_id="check_data_availability",
            python_callable=_check_data_availability,
            op_kwargs={
                'report_period': report_period
            }
        )

        email_no_data = PythonOperator(
            task_id="email_no_data",
            python_callable=_email_no_data
        )

        start_pipeline = DummyOperator(
            task_id="start_pipeline"
        )

        collect_data_1 = PythonOperator(
            task_id="collect_data_1",
            python_callable=_collect_data_1
        )

        collect_data_2 = PythonOperator(
            task_id="collect_data_2",
            python_callable=_collect_data_2
        )

        create_report = PythonOperator(
            task_id="create_report",
            python_callable=_create_report
        )

        show_result = PythonOperator(
            task_id="show_result",
            python_callable=_show_result
        )

#check_data_availability >> [collect_data_1, collect_data_2] >> create_report >> show_result
check_data_availability >> [email_no_data, start_pipeline]
start_pipeline >> [collect_data_1, collect_data_2] >> create_report >> show_result
