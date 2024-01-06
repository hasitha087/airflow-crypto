from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from airflow.operators.python_operator import PythonOperator
from encrypt import encryption, crypto_data
from airflow.models import Variable


default_args = {
    'owner': 'airflow',    
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def encrypt_data():
    """
    Function use to encrypt email column using RSA 
    and insert encrypted data into anyfin.dpd_stats table.
    """

    postgres = PostgresHook(postgres_conn_id="postgres_conn2")
    conn = postgres.get_conn()

    readsql = 'sql/dpd_data.sql'
    #encrycol = 'email'
    encrycol = Variable.get("df_col")
    key = 'keys/rsa_public_key.pem'
    insertquery = "INSERT INTO dpd_stats (customer_id, email) values(%s, %s)"

    crypto_data(readsql, encrycol, key, conn, insertquery, encryption)

dag_etl = DAG(
    dag_id = "dag_etl",
    catchup=False,
    default_args=default_args,
    schedule_interval='@daily',	
    dagrun_timeout=timedelta(minutes=60),
    description='Daily ETL job',
    start_date = airflow.utils.dates.days_ago(1) 
)

# Drop anyfin.cx_history_bak backup table
drop_backup_table = PostgresOperator(
    sql = "sql/drop_backup_table.sql",
    task_id = "drop_backup_table",
    postgres_conn_id = "postgres_conn2",
    dag = dag_etl
    )

# Insert old data into backup table before processing
create_backup_table = PostgresOperator(
    sql = "sql/create_backup_table.sql",
    task_id = "create_backup_table",
    postgres_conn_id = "postgres_conn2",
    dag = dag_etl
    )

# Drop anyfin.cx_history table
drop_table = PostgresOperator(
    sql = 'sql/drop_table.sql',
    task_id = "drop_table",
    postgres_conn_id = "postgres_conn2",
    dag = dag_etl
    )

# Create new anyfin.cx_history using new data
insert_data = PostgresOperator(
    sql = "sql/insert_data.sql",
    task_id = "insert_data",
    postgres_conn_id = "postgres_conn2",
    dag = dag_etl
    )

# Truncate anyfin.dpd_stats table
trunc_dpd_data = PostgresOperator(
    sql = "sql/trunc_dpd.sql",
    task_id = "trunc_dpd_data",
    postgres_conn_id = "postgres_conn2",
    dag = dag_etl
    )

# Encrypt data and insert into anyfin.dpd_stats table
encrypt_dpd_data = PythonOperator(
    task_id="encrypt_data",
    python_callable=encrypt_data,
    dag = dag_etl
)


drop_backup_table >> create_backup_table >> drop_table >> insert_data >> trunc_dpd_data >> encrypt_dpd_data


if __name__ == "__main__":
    dag_etl.cli()