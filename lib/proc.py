from asyncio.subprocess import PIPE, create_subprocess_exec
from typing import Any
from contextlib import asynccontextmanager


class ExitCodeError(Exception):
    pass


@asynccontextmanager
async def run(program: str, *args: str, nice: int = 0, **kwargs: Any):
    if nice != 0:
        args = tuple([ 'nice', '-n', str(nice), program ] + list(args))
    else:
        args = (program,) + args
    proc = await create_subprocess_exec(*args, **kwargs)
    try:
        yield proc
    finally:
        if await proc.wait() != 0:
            raise ExitCodeError(f'{program} exited with non-zero exit code')


async def read(program: str, *args: str) -> str:
    async with run(program, *args, stdout=PIPE) as proc:
        assert proc.stdout
        data = await proc.stdout.read()
        return data.decode()


async def readlines(program: str, *args: str):
    async with run(program, *args, stdout=PIPE) as proc:
        assert proc.stdout
        while proc.returncode is None:
            line = await proc.stdout.readline()
            if not line:
                break
            yield line.decode()

        if await proc.wait() != 0:
            raise ExitCodeError(f'{program} exited with non-zero exit code')
