from airflow.providers.postgres.hooks.postgres import PostgresHook
def get_postgres_connection_details(postgres_conn_id) -> dict:
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_connection(postgres_conn_id)
        jdbc_url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
        db_user = conn.login
        db_password = conn.password
        return {
            "jdbc_url": jdbc_url,
            "db_user": db_user,
            "db_password": db_password,
        }