# scripts/analyze_sample.py
import pandas as pd
import json

# Прочитайте сгенерированные данные
with open('sample_data.json', 'r') as f:
    data = [json.loads(line) for line in f.readlines()[:100]]

df = pd.DataFrame(data)
print("Первые 5 записей:")
print(df.head())
print("\nИнформация о данных:")
print(df.info())
print("\nСтатистика:")
print(df.describe())