from airflow import DAG
from airflow.operators.bash  import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import date,timedelta,datetime


dag_name = "au-entity"
Dname = "AU-ENTITY"
root = "/home/ubuntu/sanctions-scripts/AU-ENTITY/"

today_date  = date.today()
yesterday = today_date - timedelta(days = 1)

ip_path = f"{root}inputfiles"
input_filename  = f'{dag_name}-inout-{today_date.day}-{today_date.month}-{today_date.year}.json'

def slack_notification(context):
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_webhook',
        message=slack_msg)
    return failed_alert.execute(context=context)


default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2022,4,25),
        'schedule_interval':"@daily",
        'email': ['airflow@airflow.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'catchup':False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback':slack_notification,
        }

dag = DAG(Dname, default_args=default_args)
t1 = BashOperator(
            task_id='crawler-run',
                bash_command=f'scrapy runspider {root}scode.py -o {ip_path}/{input_filename}',
                    dag=dag)

t2 = BashOperator(
            task_id='proccesing-data',
                bash_command=f'python3 {root}code.py',
                    dag=dag)

t3 = BashOperator(
            task_id='uploading-to-DB',
                bash_command=f'python3 {root}updateESDB.py',
                    dag=dag)
t1 >> t2 >> t3
