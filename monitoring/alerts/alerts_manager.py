#!/usr/bin/env python3
import psycopg2
import json
import smtplib
from email.mime.text import MimeText
from datetime import datetime, timedelta

class AlertManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost", database="analytics", 
            user="admin", password="password"
        )
    
    def check_data_quality_alerts(self):
        """Проверка качества данных"""
        cur = self.conn.cursor()
        
        # Проверка на пропущенные значения
        cur.execute("""
            SELECT COUNT(*) 
            FROM user_views_processed 
            WHERE user_id IS NULL OR movie_id IS NULL
            AND processing_timestamp >= NOW() - INTERVAL '1 hour'
        """)
        null_count = cur.fetchone()[0]
        
        if null_count > 10:
            self.send_alert("Data Quality Alert", 
                          f"Found {null_count} records with null values in the last hour")
    
    def check_performance_alerts(self):
        """Проверка производительности системы"""
        cur = self.conn.cursor()
        
        # Проверка задержки обработки
        cur.execute("""
            SELECT AVG(EXTRACT(EPOCH FROM (processing_timestamp - original_timestamp))) as avg_latency
            FROM user_views_processed 
            WHERE processing_timestamp >= NOW() - INTERVAL '30 minutes'
        """)
        avg_latency = cur.fetchone()[0]
        
        if avg_latency and avg_latency > 300:  # более 5 минут
            self.send_alert("Performance Alert", 
                          f"High processing latency: {avg_latency:.2f} seconds")
    
    def check_business_alerts(self):
        """Бизнес-алерты"""
        cur = self.conn.cursor()
        
        # Резкое падение просмотров
        cur.execute("""
            WITH hourly_views AS (
                SELECT 
                    DATE_TRUNC('hour', processing_timestamp) as hour,
                    COUNT(*) as view_count
                FROM user_views_processed 
                WHERE processing_timestamp >= NOW() - INTERVAL '2 hours'
                GROUP BY 1
            )
            SELECT (MAX(view_count) - MIN(view_count)) / NULLIF(MAX(view_count), 0) * 100 as drop_percentage
            FROM hourly_views
        """)
        drop_pct = cur.fetchone()[0]
        
        if drop_pct and drop_pct > 50:  # падение более 50%
            self.send_alert("Business Alert", 
                          f"Significant drop in views: {drop_pct:.1f}%")
    
    def send_alert(self, subject, message):
        """Отправка алерта"""
        print(f"🚨 ALERT: {subject}")
        print(f"   {message}")
        
        # Здесь может быть интеграция с:
        # - Email
        # - Slack
        # - PagerDuty
        # - Telegram
        
        # Сохранение алерта в базу для дашборда
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO alerts (alert_type, message, severity, created_at)
            VALUES (%s, %s, %s, %s)
        """, (subject, message, 'high', datetime.now()))
        self.conn.commit()
    
    def run_monitoring(self):
        """Запуск мониторинга"""
        print("🔍 Запуск мониторинга...")
        
        while True:
            try:
                self.check_data_quality_alerts()
                self.check_performance_alerts()
                self.check_business_alerts()
                
                time.sleep(300)  # Проверка каждые 5 минут
                
            except Exception as e:
                print(f"❌ Ошибка мониторинга: {e}")
                time.sleep(60)

if __name__ == "__main__":
    monitor = AlertManager()
    monitor.run_monitoring()