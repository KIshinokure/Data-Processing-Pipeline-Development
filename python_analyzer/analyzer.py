import time
import os
import polars as pl
import duckdb
import matplotlib.pyplot as plt

DATA_PATH = "/data/industrial_metrics.parquet"
OUTPUT_PLOT = "/data/machinery_report.png"

def analyze_data():
    if not os.path.exists(DATA_PATH) or os.path.getsize(DATA_PATH) == 0:
        print("[Python] Ожидание наполнения Parquet-файла данными...")
        return

    print("\n--- [Python] Запуск фазы аналитики данных ---")
    
    df = pl.read_parquet(DATA_PATH)
    print(f"Загружено записей из Parquet: {df.shape[0]}")
    
    summary = df.group_by("machine_id").agg([
        pl.col("avg_temperature").mean().alias("global_avg_temp"),
        pl.col("max_pressure").max().alias("absolute_max_pressure"),
        pl.col("msg_count").sum().alias("total_ticks_processed")
    ])
    print("\n[Polars] Сводные показатели по станкам:")
    print(summary)

    conn = duckdb.connect()
    anomaly_query = f"""
        SELECT 
            epoch_ms(timestamp) as event_time,
            machine_id,
            avg_temperature,
            max_pressure
        FROM '{DATA_PATH}'
        WHERE avg_temperature > 85.0
        ORDER BY timestamp DESC
        LIMIT 5
    """
    try:
        anomalies_df = conn.execute(anomaly_query).fetchdf()
        print("\n[DuckDB] Последние 5 зафиксированных температурных аномалий (>85°C):")
        if not anomalies_df.empty:
            print(anomalies_df.to_string(index=False))
        else:
            print("Аномалий не обнаружено. Оборудование работает штатно.")
    except Exception as e:
        print(f"Ошибка выполнения SQL в DuckDB: {e}")

    try:
        pd_df = df.to_pandas()
        pd_df['datetime'] = pl.from_pandas(pd_df)['timestamp'].cast(pl.Datetime("ms")).to_pandas()
        
        plt.figure(figsize=(10, 6))
        for machine, group in pd_df.groupby('machine_id'):
            group = group.sort_values('datetime')
            plt.plot(group['datetime'], group['avg_temperature'], label=f"{machine} Temp", marker='o', markersize=3)
        
        plt.title("Мониторинг температуры оборудования (Modbus/OPC Эмуляция)")
        plt.xlabel("Время")
        plt.ylabel("Температура (°C)")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.gcf().autofmt_xdate()
        
        plt.savefig(OUTPUT_PLOT)
        plt.close()
        print(f"[Python] График успешно обновлен и сохранен в: {OUTPUT_PLOT}")
    except Exception as e:
        print(f"Ошибка построения графиков: {e}")

if __name__ == "__main__":
    time.sleep(7)
    while True:
        try:
            analyze_data()
        except Exception as e:
            print(f"Критическая ошибка анализатора: {e}")
        time.sleep(15)
