import asyncio

from parameter_server import Client, KeyRange


async def main():
    async with await Client.connect("127.0.0.1:50051", worker_id=10) as client:
        before = client.pull(KeyRange(2, 5))
        values = await before.wait()
        update = client.push(values.keys, [1.0] * len(values.data), deps=[before])
        print((await client.pull(values.keys, deps=[update]).wait()).data)


asyncio.run(main())
