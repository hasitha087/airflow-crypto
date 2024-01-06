from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from airflow.operators.python_operator import PythonOperator
from encrypt import decryption, crypto_data


default_args = {
    'owner': 'airflow',    
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def decrypt_data():
    """
    Function use to decrypt email column using RSA 
    and insert decrypted data into anyfin.dpd_stats_decrypt table. 
    """

    postgres = PostgresHook(postgres_conn_id="postgres_conn2")
    conn = postgres.get_conn()

    readsql = 'sql/dpd_data_decrypt.sql'
    decrycol = 'email'
    key = 'keys/rsa_private_key.pem'
    insertquery = "INSERT INTO dpd_stats_decrypt (customer_id, email) values(%s, %s)"

    crypto_data(readsql, decrycol, key, conn, insertquery, decryption)


dag_decrypt = DAG(
    dag_id = "dag_decrypt",
    catchup=False,
    default_args=default_args,
    schedule_interval='@daily',	
    dagrun_timeout=timedelta(minutes=60),
    description='ETL to decrypt data',
    start_date = airflow.utils.dates.days_ago(1) 
)

# Truncate anyfin.dpd_stats_decrypt table
trunc_dpd_data_decrypt = PostgresOperator(
    sql = "sql/trunc_dpd_decrypt.sql",
    task_id = "trunc_dpd_data_decrypt",
    postgres_conn_id = "postgres_conn2",
    dag = dag_decrypt
    )

# decrypt data and insrt into anyfin.dpd_stats_decrypt table
decrypt_dpd_data = PythonOperator(
    task_id="decrypt_data",
    python_callable=decrypt_data,
    dag = dag_decrypt
)


trunc_dpd_data_decrypt >> decrypt_dpd_data


if __name__ == "__main__":
    dag_decrypt.cli()