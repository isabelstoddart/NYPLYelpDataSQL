# See readme-workflow.txt for explanation of errors we ran into while running this file in Airflow

import datetime

from airflow import models
from airflow.operators.bash_operator import BashOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 1)
}

#SQL Variables
raw_dataset = 'dataset2'
new_dataset = 'workflow'
sql_cmd_start = 'bq query --use_legacy_sql=false '

sql_user = 'create table ' + new_dataset + '.User_Temp as select user_id, name, review_count, yelping_since, useful, funny, cool, fans, elite, average_stars, compliment_hot, compliment_more, compliment_profile, compliment_cute, compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny, compliment_writer, compliment_photos ' \
         'from ' + raw_dataset + '.User_ ' \

# Beam Variables
LOCAL_MODE=1 # run beam jobs locally
DIST_MODE=2 # run beam jobs on Dataflow

mode=LOCAL_MODE

if mode == LOCAL_MODE:
    business_script = 'transform_Business_single.py'
    elite_script = 'transform_Elite_single.py'
    friend_script = 'transform_Friend_single.py'
    
if mode == DIST_MODE:
    business_script = 'transform_Business_cluster.py'
    elite_script = 'transform_Elite_cluster.py'
    friend_script = 'transform_Friend_cluster.py' 

# DAG section
with models.DAG('workflow',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    
    # Beam Tasks
    business_beam = BashOperator(
            task_id='business_beam',
            bash_command='python /home/stoddartisabel/' + business_script)
            
    elite_beam = BashOperator(
            task_id='elite_beam',
            bash_command='python /home/stoddartisabel/' + elite_script)
            
    friend_beam = BashOperator(
            task_id='friend_beam',
            bash_command='python /home/stoddartisabel/' + friend_script)
    
    # SQL Task
    create_user_table = BashOperator(
            task_id='create_user_table',
            bash_command=sql_cmd_start + '"' + sql_user + '"')
            
    [business_beam, elite_beam, friend_beam] >> create_user_table
