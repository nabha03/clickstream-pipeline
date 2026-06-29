from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import socket
import psycopg2

default_args = {
    "owner": "nabha",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="clickstream_etl_pipeline",
    description="Kafka → Spark → PostgreSQL clickstream ETL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/10 * * * *",  # every 10 minutes
    catchup=False,
    tags=["clickstream", "kafka", "spark", "etl"],
)

# Task 1: Check Kafka
def check_kafka_health(**context):
    sock = socket.create_connection(("kafka", 29092), timeout=10)
    sock.close()
    print("✅ Kafka is reachable")

t_check_kafka = PythonOperator(
    task_id="check_kafka_health",
    python_callable=check_kafka_health,
    dag=dag,
)

# Task 2: Check PostgreSQL
def check_postgres_health(**context):
    conn = psycopg2.connect(
        host="postgres", port=5432, database="clickstream",
        user="sparkuser", password="sparkpass", connect_timeout=10,
    )
    conn.close()
    print("✅ PostgreSQL is reachable")

t_check_postgres = PythonOperator(
    task_id="check_postgres_health",
    python_callable=check_postgres_health,
    dag=dag,
)

# Task 3: Run Kafka producer for 60 seconds
t_run_producer = BashOperator(
    task_id="run_kafka_producer_batch",
    bash_command="timeout 60 python /opt/airflow/scripts/producer.py || true",
    dag=dag,
)

# Task 4: Submit Spark ETL job
t_run_spark = BashOperator(
    task_id="submit_spark_etl_job",
    bash_command="""
        docker exec spark-submit /opt/spark/bin/spark-submit \
            --master local[*] \
            --jars /opt/spark/extra-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,\
/opt/spark/extra-jars/kafka-clients-3.4.1.jar,\
/opt/spark/extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,\
/opt/spark/extra-jars/commons-pool2-2.11.1.jar,\
/opt/spark/extra-jars/postgresql-42.6.0.jar \
            --conf spark.driver.host=spark-submit \
            --conf spark.driver.bindAddress=0.0.0.0 \
            /opt/spark/work-dir/spark_consumer.py || true
    """,
    dag=dag,
    execution_timeout=timedelta(minutes=5),
)

# Task 5: Verify data in PostgreSQL
def verify_postgres_data(**context):
    conn = psycopg2.connect(
        host="postgres", port=5432, database="clickstream",
        user="sparkuser", password="sparkpass",
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM clickstream_events;")
    total = cur.fetchone()[0]
    conn.close()
    print(f"📊 Total rows: {total}")
    if total == 0:
        raise ValueError("❌ No data in clickstream_events!")
    print("✅ Data verified")

t_verify_data = PythonOperator(
    task_id="verify_postgres_data",
    python_callable=verify_postgres_data,
    dag=dag,
)

# Task 6: Done
t_done = EmptyOperator(task_id="pipeline_complete", dag=dag)

# DAG flow:
# check_kafka ──┐
#               ├──► producer ──► spark ──► verify ──► done
# check_postgres ┘
[t_check_kafka, t_check_postgres] >> t_run_producer >> t_run_spark >> t_verify_data >> t_done