#!/usr/bin/env python3
from kafka import KafkaProducer, KafkaAdminClient
import time
import socket

def test_kafka_connection():
    print("🔍 Тестирование подключения к Kafka...")
    
    # Сначала проверим доступность порта
    print("1. Проверка порта 9092...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex(('localhost', 9092))
    sock.close()
    
    if result == 0:
        print("✅ Порт 9092 доступен")
    else:
        print("❌ Порт 9092 недоступен")
        return False
    
    # Тестируем Kafka Producer
    print("2. Тестирование Kafka Producer...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            request_timeout_ms=10000
        )
        print("✅ Kafka Producer создан успешно!")
        
        # Тестируем отправку сообщения
        print("3. Тестирование отправки сообщения...")
        future = producer.send('test_topic', b'test_message')
        
        # Ждем подтверждения
        result = future.get(timeout=10)
        print(f"✅ Сообщение отправлено! Partition: {result.partition}, Offset: {result.offset}")
        
        producer.close()
        
    except Exception as e:
        print(f"❌ Ошибка Producer: {e}")
        return False
    
    # Тестируем Kafka Admin для получения метаданных
    print("4. Получение метаданных Kafka...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='test_admin'
        )
        
        topics = admin_client.list_topics()
        print(f"✅ Успешно! Доступные топики: {len(topics)}")
        
        for topic in sorted(topics):
            print(f"   - {topic}")
            
        admin_client.close()
        
    except Exception as e:
        print(f"❌ Ошибка получения метаданных: {e}")
        return False
    
    return True

if __name__ == "__main__":
    if test_kafka_connection():
        print("\n🎉 Kafka работает корректно!")
    else:
        print("\n💥 Есть проблемы с Kafka")