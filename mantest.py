from pulsar.asyncio import Client
from pulsar import ConsumerType
from _pulsar import MessageId
import asyncio

async def main():
    client = Client("pulsar://159.69.189.225")
    consumer = await client.subscribe("test-topic", "test-sub", ConsumerType.Exclusive)
    await consumer.seek((167,6,-1,0))

    # try:
    #     while True:
    #         msgs = await consumer.batch_receive()
    #         last_msg = None
    #         for msg in msgs:
    #             print(msg)
    #             last_msg = msg
    #             # await consumer.acknowledge(msg)
    #             # print("acknowledged", msg.message_id())
    #         if last_msg:
    #             await consumer.acknowledge_cumulative(last_msg)
    #             print("acknowledged messages up until", last_msg.message_id())
    # except KeyboardInterrupt:
    #     await consumer.close()
    #     await client.close()


if __name__ == '__main__':
    asyncio.run(main())