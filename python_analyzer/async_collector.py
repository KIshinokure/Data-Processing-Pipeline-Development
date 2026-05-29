import asyncio
import time
import random

async def simulate_sensor_read():
    # Имитация задержки датчика и валидации
    await asyncio.sleep(0.01)
    return random.uniform(0, 120), random.uniform(0, 60)

async def main():
    print("Запуск бенчмарка Python Asyncio Collector...")
    start = time.time()
    
    # Собираем 10 000 записей конкурентно
    records = 10000
    tasks = [simulate_sensor_read() for _ in range(records)]
    await asyncio.gather(*tasks)
    
    elapsed = time.time() - start
    print(f"Собрано {records} записей.")
    print(f"Время выполнения (Python): {elapsed:.4f} сек")
    print(f"Пропускная способность: {records/elapsed:.0f} req/sec")

if __name__ == "__main__":
    asyncio.run(main())
