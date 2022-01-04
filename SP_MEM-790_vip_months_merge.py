from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.allecomposer import BigQueryDataSensor
from dataengine.composer.extras.bigquery_table_partition_changed_sensor import BigQueryTablePartitionChangedSensor
from dataengine.composer.extras import slack_notifier
import pendulum
import sys

daily_date = "{{ ds }}"

local_tz = pendulum.timezone("Europe/Warsaw")

notifier = slack_notifier.SlackNotifier(connection_id='slack_conn', success_icon = ':clappingjoker:', failure_icon = ':facepalm:')

## ZAPYTANIA

query = """  
MERGE `sc-10024-com-analysts-prod.commercial.tl_ga6_vip_months` AS vip_months
USING (
SELECT 
    cast(FORMAT_DATE("%Y%m", current_date()-1) as int) * 100 + 1 month_,
    sf.USERID 	us_id,
    users.residence_country_code res_country_code
    FROM `sc-12618-it-business-prod.salesforce_restricted.imp_sf_account` sf
    left join `sc-9369-dataengineering-prod.dwh.tl_gi4_users` users
    on users.account_id = cast(sf.userid as int)
    and users._PARTITIONDATE = current_date()-1
    where 
    1=1
    and VIP = 'True'
    and USERID <> ''
) AS vip_new
ON vip_months.us_id = cast(vip_new.us_id as int)
and vip_months.month_ = vip_new.month_
WHEN MATCHED THEN update set vip_months.res_country_code = vip_new.res_country_code
WHEN NOT MATCHED THEN
INSERT(month_ , us_id, res_country_code)
VALUES(vip_new.month_, cast(vip_new.us_id as int), vip_new.res_country_code)
   """

default_args = {
    'owner': 'Szymon',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 2, tzinfo=local_tz),
    'email': ['szymon.plachta@allegro.pl'],
    'email_on_failure': True,
    'on_failure_callback': notifier.task_fail_slack_alert,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
    }

dag = DAG(
    'SP_MEM-790_vip_months_merge',
    default_args=default_args,
    schedule_interval="0 6 * * *",
    concurrency=3,
    max_active_runs=3,
	tags = ["Szymon"])

##SENSORY

sf_account = BigQueryTablePartitionChangedSensor(
    task_id='sf_account',
    project_id='sc-12618-it-business-prod',
    dataset_id ='salesforce_restricted',
    table_id ='imp_sf_account',
    dag=dag)

users = BigQueryDataSensor(
    task_id='users',
    project_id='sc-9369-dataengineering-prod',
    dataset_id ='dwh',
    table_id ='tl_gi4_users',
    dialect='standard',
    where_clause="_partitiondate = '{}'".format(daily_date),
    bigquery_conn_id='google_cloud_default',
    poke_interval=200,
    dag=dag)

#OPERATORY

bq_operator_1 = BigQueryOperator(
   task_id='load_table',
   dag=dag,
   bql=query,
   bigquery_conn_id='google_cloud_default',
   use_legacy_sql=False
)

bq_operator_1.set_upstream([sf_account, users])

