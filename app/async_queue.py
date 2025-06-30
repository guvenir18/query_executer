import asyncio
from asyncio import Queue

class QueueWorker:
    def __init__(self, callback):
        self.callback = callback
        self.queue = Queue()
        self._thread = asyncio.create_task(self._worker_loop())

    async def _worker_loop(self):
        while True:
            queries, query_template = await self.queue.get()
            try:
                await self.callback(queries, query_template)
            except Exception as e:
                print("Error:", e)
            finally:
                self.queue.task_done()

    def schedule_callback(self, queries, query_template):
        self.queue.put_nowait((queries, query_template))