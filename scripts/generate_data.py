import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# Настройки генерации
NUM_USERS = 1000 # Сколько пользователей за день
START_DATE = datetime.now().strftime("%Y-%m-%d")

# Путь, куда сохраним файл (внутри контейнера это будет /opt/airflow/data)
OUTPUT_PATH = "/opt/airflow/data/raw_logs.csv"

def generate_events():
    data = []
    
    # Имитируем пользователей
    for i in range(NUM_USERS):
        user_id = f"user_{i}"
        
        # Случайное разделение на группы (A/B тест)
        # 50% пользователей в группу A, 50% в группу B
        group = 'A' if random.random() < 0.5 else 'B'
        
        # Логика конверсии (воронка: view -> click -> purchase)
        # В группе B конверсия будет чуть выше!
        
        # 1. Просмотр товара (делают все)
        event_time = datetime.strptime(START_DATE, "%Y-%m-%d") + timedelta(seconds=random.randint(0, 86400))
        data.append({
            "user_id": user_id,
            "timestamp": event_time,
            "group": group,
            "event": "view"
        })
        
        # Шанс клика
        click_prob = 0.30 if group == 'A' else 0.35  # В группе B кликают на 5% чаще
        
        if random.random() < click_prob:
            # 2. Клик
            event_time += timedelta(seconds=random.randint(10, 120))
            data.append({
                "user_id": user_id,
                "timestamp": event_time,
                "group": group,
                "event": "click"
            })
            
            # Шанс покупки (только если кликнул)
            purchase_prob = 0.10 if group == 'A' else 0.15 # В группе B покупают чаще
            
            if random.random() < purchase_prob:
                # 3. Покупка
                event_time += timedelta(seconds=random.randint(30, 600))
                data.append({
                    "user_id": user_id,
                    "timestamp": event_time,
                    "group": group,
                    "event": "purchase",
                    "price": random.choice([19.99, 49.99, 99.99]) # Сумма покупки
                })

    # Создаем DataFrame
    df = pd.DataFrame(data)
    
    # Проверка: если файл уже есть, дописываем в него, если нет - создаем
    # Это нужно, чтобы мы могли запускать скрипт каждый день и копить данные
    header = not os.path.exists(OUTPUT_PATH)
    
    df.to_csv(OUTPUT_PATH, mode='a', header=header, index=False)
    print(f"Сгенерировано {len(df)} событий и сохранено в {OUTPUT_PATH}")

if __name__ == "__main__":
    generate_events()