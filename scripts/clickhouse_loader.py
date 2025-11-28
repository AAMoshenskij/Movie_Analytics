#!/usr/bin/env python3
import pandas as pd
import psycopg2
from clickhouse_driver import Client
from datetime import datetime, timedelta

class ClickHouseLoader:
    def __init__(self):
        # PostgreSQL connection
        self.pg_conn = psycopg2.connect(
            host="localhost", database="analytics",
            user="admin", password="password"
        )
        
        # ClickHouse connection
        self.ch_client = Client(
            host='localhost',
            port=9000,
            user='default',
            password='',
            database='analytics'
        )
    
    def load_daily_data_to_clickhouse(self):
        """Загружает данные из PostgreSQL в ClickHouse"""
        
        print("🔄 Загрузка данных из PostgreSQL в ClickHouse...")
        
        # Читаем данные из PostgreSQL
        query = """
        SELECT 
            user_id, movie_id, duration_seconds, event_type, device,
            watch_category, original_timestamp, session_id, is_valid
        FROM user_views_processed 
        WHERE processing_timestamp >= NOW() - INTERVAL '1 day'
        """
        
        df = pd.read_sql(query, self.pg_conn)
        
        if df.empty:
            print("ℹ️ Нет новых данных для загрузки")
            return
        
        # Добавляем вычисляемые поля для ClickHouse
        df['event_date'] = pd.to_datetime(df['original_timestamp']).dt.date
        df['event_timestamp'] = pd.to_datetime(df['original_timestamp'])
        df['is_valid'] = df['is_valid'].astype(int)
        
        # Подготавливаем данные для вставки
        data = df[[
            'user_id', 'movie_id', 'duration_seconds', 'event_type', 
            'device', 'watch_category', 'event_date', 'event_timestamp',
            'session_id', 'is_valid'
        ]].to_dict('records')
        
        # Вставляем в ClickHouse
        self.ch_client.execute(
            """INSERT INTO user_views_fact (
                user_id, movie_id, duration_seconds, event_type, device,
                watch_category, event_date, event_timestamp, session_id, is_valid
            ) VALUES""",
            data
        )
        
        print(f"✅ Загружено {len(data)} записей в ClickHouse")
        
        # Обновляем агрегированные данные
        self.update_aggregated_metrics()
    
    def update_aggregated_metrics(self):
        """Обновляет агрегированные метрики в ClickHouse"""
        
        print("📊 Обновление агрегированных метрик...")
        
        # Очищаем старые данные за сегодня
        today = datetime.now().date()
        self.ch_client.execute(
            "ALTER TABLE daily_metrics DELETE WHERE metric_date = %(date)s",
            {'date': today}
        )
        
        # Вставляем новые агрегированные данные
        self.ch_client.execute("""
            INSERT INTO daily_metrics
            SELECT 
                event_date as metric_date,
                count(*) as total_views,
                uniq(user_id) as unique_users,
                sum(duration_seconds) as total_watch_time,
                avg(duration_seconds) as avg_watch_time
            FROM user_views_fact 
            WHERE event_date = %(date)s
            GROUP BY event_date
        """, {'date': today})
        
        # Обновляем популярность фильмов
        self.ch_client.execute("""
            ALTER TABLE movie_popularity DELETE WHERE 1=1
        """)
        
        self.ch_client.execute("""
            INSERT INTO movie_popularity
            SELECT 
                movie_id,
                count(*) as total_views,
                uniq(user_id) as unique_users,
                avg(duration_seconds) as avg_watch_time,
                now() as last_updated
            FROM user_views_fact 
            WHERE event_date >= today() - 7
            GROUP BY movie_id
            ORDER BY total_views DESC
        """)
        
        print("✅ Агрегированные метрики обновлены")
    
    def run_continuous_loading(self):
        """Непрерывная загрузка данных"""
        print("🚀 Запуск непрерывной загрузки данных в ClickHouse...")
        
        import time
        while True:
            try:
                self.load_daily_data_to_clickhouse()
                time.sleep(300)  # Ждем 5 минут
            except Exception as e:
                print(f"❌ Ошибка: {e}")
                time.sleep(60)

if __name__ == "__main__":
    loader = ClickHouseLoader()
    loader.run_continuous_loading()