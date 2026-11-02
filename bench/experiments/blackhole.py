"""TCP black hole: accepts connections, reads, never responds.  python blackhole.py PORT"""
import asyncio
import sys


async def handle(reader, writer):
    while await reader.read(65536):
        pass


async def main():
    server = await asyncio.start_server(handle, "127.0.0.1", int(sys.argv[1]))
    async with server:
        await server.serve_forever()

asyncio.run(main())
