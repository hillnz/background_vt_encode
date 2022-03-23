from asyncio import Queue, QueueEmpty, Task
import asyncio
from typing import AsyncIterable, Callable, Iterable, Set, TypeVar


T = TypeVar('T')
JobQueue = Queue[T | None]
JobStep = Callable[[AsyncIterable[T]], AsyncIterable[tuple[T, bool]]]

async def pipeline(*steps: JobStep[T], initial_value: Iterable[T], finally_: JobStep[T], buffer_size: int = 1):
    """Pass jobs through steps in order. Steps run concurrently."""

    async def yield_from_queue(queue: JobQueue[T]) -> AsyncIterable[T]:
        while item := await queue.get():
            yield item
            queue.task_done()

    async def run_step(step: JobStep[T], in_queue: JobQueue[T], out_queue: JobQueue[T], final_queue: JobQueue[T] | None):
        async for job, continue_ in step(yield_from_queue(in_queue)):
            if continue_ or final_queue is None:
                await out_queue.put(job)
            else:
                await final_queue.put(job)
            
    async def feed_queue(queue: JobQueue[T], jobs: Iterable[T]):
        for job in jobs:
            await queue.put(job)
        await queue.put(None)

    pending: Set[Task[T | None] | Task[None]] = set()
    queues: list[JobQueue[T]] = []

    final_in_queue: JobQueue[T] = Queue(buffer_size)

    in_queue: JobQueue[T] = Queue(buffer_size)
    queues.append(in_queue)
    pending.add(asyncio.create_task(feed_queue(in_queue, initial_value)))

    # Run all steps concurrently, feeding the output of each into the next
    for n, step in enumerate(steps):
        if n == len(steps) - 1:
            out_queue = final_in_queue
        else:
            out_queue: JobQueue[T] = Queue(buffer_size)
        queues.append(out_queue)
        pending.add(asyncio.create_task(run_step(step, in_queue, out_queue, final_in_queue)))
        in_queue = out_queue
        
    # And the finally step
    final_out_queue: JobQueue[T] = Queue(buffer_size)
    queues.append(final_out_queue)
    pending.add(asyncio.create_task(run_step(finally_, final_in_queue, final_out_queue, None)))

    # Final step's results
    queue_task = asyncio.create_task(in_queue.get())
    pending.add(queue_task)

    try:
        # Wait for all tasks to finish
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is queue_task:
                    # Yield final step's result
                    result = await queue_task
                    if result is not None:
                        yield result
                        queue_task = asyncio.create_task(in_queue.get())
                        pending.add(queue_task)
                else:
                    await task
    except:
        # Cancel all tasks
        for task in pending:
            task.cancel()
        # Run incomplete jobs through the finally step
        async def remaining_jobs():
            for q in reversed(queues):
                try:
                    while True:
                        job = q.get_nowait()
                        if job is not None:
                            yield job
                except QueueEmpty:
                    pass
        [ _ async for _ in finally_(remaining_jobs()) ]
                
        raise

