# scripts/simple_generator.py
import json
import random
import time
from datetime import datetime

def generate_simple_event():
    return {
        "user_id": f"user_{random.randint(1, 1000)}",
        "movie_id": f"movie_{random.randint(1, 500)}",
        "duration": random.randint(10, 7200),
        "event_type": random.choice(['start', 'stop', 'pause']),
        "timestamp": datetime.now().isoformat()
    }

# Сохраняем в файл для начала
with open('sample_data.json', 'w') as f:
    for i in range(1000):
        f.write(json.dumps(generate_simple_event()) + '\n')
        if i % 100 == 0:
            print(f"Generated {i} records...")