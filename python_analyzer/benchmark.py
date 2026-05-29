import asyncio
import aiohttp
import time

async def fetch(session):
    try:
        async with session.get("http://localhost:8081") as response:
            return await response.status
    except:
        return None

async def main():
    start = time.time()
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session) for _ in range(1000)]
        await asyncio.gather(*tasks)
    print(f"Python Asyncio Benchmark: 1000 requests in {time.time() - start:.4f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
