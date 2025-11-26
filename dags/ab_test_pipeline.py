from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np
from scipy import stats
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Функция для создания подключения (чтобы не дублировать код)
def get_engine():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_connection('postgres_default')
    connection_string = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(connection_string)

def load_data_to_postgres():
    filepath = "/opt/airflow/data/raw_logs.csv"
    df = pd.read_csv(filepath)
    engine = get_engine()
    df.to_sql('user_logs', engine, if_exists='append', index=False)
    print(f"Успешно загружено {len(df)} строк")

def analyze_ab_test():
    """
    1. Читаем данные из БД
    2. Считаем конверсию в покупку
    3. Считаем p-value
    """
    engine = get_engine()
    
    # Читаем только нужные колонки
    df = pd.read_sql("SELECT user_id, \"group\", event FROM user_logs", engine)
    
    # Агрегация: для каждого юзера ставим 1, если была покупка, иначе 0
    # pivot_table создает таблицу, где индекс - user_id, колонки - типы событий, значения - количество
    metrics = df.pivot_table(index=['user_id', 'group'], columns='event', aggfunc='size', fill_value=0).reset_index()
    
    # Создаем колонку is_purchase: 1 если purchase > 0, иначе 0
    # (Если колонки purchase нет в данных, создаем её с нулями)
    if 'purchase' not in metrics.columns:
        metrics['purchase'] = 0
    
    metrics['is_purchase'] = (metrics['purchase'] > 0).astype(int)
    
    # Разбиваем на группы
    group_a = metrics[metrics['group'] == 'A']['is_purchase']
    group_b = metrics[metrics['group'] == 'B']['is_purchase']
    
    # Считаем среднюю конверсию
    conv_a = group_a.mean()
    conv_b = group_b.mean()
    
    print(f"--- РЕЗУЛЬТАТЫ A/B ТЕСТА ---")
    print(f"Пользователей в группе A: {len(group_a)}, Конверсия: {conv_a:.2%}")
    print(f"Пользователей в группе B: {len(group_b)}, Конверсия: {conv_b:.2%}")
    
    # Проводим T-test (сравниваем средние)
    # H0: Разницы нет. H1: Разница есть.
    t_stat, p_value = stats.ttest_ind(group_a, group_b)
    
    print(f"T-statistic: {t_stat:.4f}")
    print(f"P-value: {p_value:.5f}")
    
    if p_value < 0.05:
        print("РЕЗУЛЬТАТ: Различия СТАТИСТИЧЕСКИ ЗНАЧИМЫ! Победил вариант " + ("B" if conv_b > conv_a else "A"))
    else:
        print("РЕЗУЛЬТАТ: Различий нет (не можем отвергнуть H0).")

with DAG(
    dag_id='ab_test_pipeline_v1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='python /opt/airflow/scripts/generate_data.py'
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS user_logs (
            user_id VARCHAR(50),
            timestamp TIMESTAMP,
            "group" VARCHAR(10),
            event VARCHAR(50),
            price FLOAT
        );
        """
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_postgres
    )
    
    # Новый шаг аналитики
    analyze_results = PythonOperator(
        task_id='analyze_results',
        python_callable=analyze_ab_test
    )

    # Цепочка выполнения
    generate_data >> create_table >> load_data >> analyze_results