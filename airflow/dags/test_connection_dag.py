from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def test_postgres_connection():
    """Тестирует подключение к нашей базе данных"""
    print("🔍 Тестируем подключение к PostgreSQL...")
    
    try:
        hook = PostgresHook(postgres_conn_id='analytics_db')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Проверяем таблицы
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        
        print("✅ Подключение успешно!")
        print("📊 Доступные таблицы:")
        for table in tables:
            print(f"   - {table[0]}")
            
        # Проверяем данные в user_views
        cursor.execute("SELECT COUNT(*) FROM user_views")
        count = cursor.fetchone()[0]
        print(f"   📈 Записей в user_views: {count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        raise

def generate_sample_data():
    """Генерирует тестовые данные"""
    print("🎲 Генерация тестовых данных...")
    
    try:
        hook = PostgresHook(postgres_conn_id='analytics_db')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Вставляем тестовые данные
        cursor.execute("""
            INSERT INTO user_views (user_id, movie_id, duration_seconds, event_type, event_timestamp)
            VALUES 
            ('test_user_1', 'test_movie_1', 300, 'start', NOW()),
            ('test_user_2', 'test_movie_2', 600, 'pause', NOW()),
            ('test_user_1', 'test_movie_1', 150, 'stop', NOW())
        """)
        conn.commit()
        
        print("✅ Тестовые данные добавлены")
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка генерации данных: {e}")
        raise

with DAG(
    'test_analytics_connection',
    default_args=default_args,
    description='Тестовый DAG для проверки подключения к аналитической БД',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['test', 'analytics']
) as dag:

    test_connection = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection
    )

    generate_data = PythonOperator(
        task_id='generate_sample_data',
        python_callable=generate_sample_data
    )

    test_connection >> generate_data