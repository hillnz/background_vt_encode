import asyncio
from dataclasses import dataclass
import dataclasses
import os
import re
from re import Pattern
from typing import AsyncGenerator, Optional

from . import proc

@dataclass
class RcloneOptions:
    bwlimit: Optional[str]


    def asargs(self):
        args: list[str] = []
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if value is None:
                continue
            args.append(f'--{f.name}')
            args.append(str(value))

        return args


async def find_files(path: str, pattern: Pattern[str], skip_dirs: list[str] = [], options: RcloneOptions | None = None) -> AsyncGenerator[str, None]:

    if path.endswith('/'):
        path = path[:-1]

    opts = options.asargs() if options else []
    # consume entire input to prevent concurrent/blocked rclones
    lines = [ l async for l in proc.readlines('rclone', 'lsf', path, *opts) ]
    for line in lines:
        absolute = path + '/' + line.strip()
        
        if absolute.endswith('/'):
            # is dir
            if line.strip() in skip_dirs:
                continue
            async for p in find_files(absolute, pattern):
                yield p

        elif pattern.match(absolute):
            yield absolute


async def copy(src: str, dst: str, options: RcloneOptions | None = None):
    opts = options.asargs() if options else []
    async with proc.run('rclone', '--progress', 'copyto', src, dst, *opts, nice=10) as _:
        pass


async def move(src: str, dst: str, options: RcloneOptions | None = None):
    opts = options.asargs() if options else []
    async with proc.run('rclone', '--progress', 'moveto', src, dst, *opts, nice=10) as _:
        pass


async def delete(filepath: str, options: RcloneOptions | None = None):
    opts = options.asargs() if options else []
    async with proc.run('rclone', 'delete', filepath, *opts) as _:
        pass


async def purge(filepath: str, options: RcloneOptions | None = None):
    opts = options.asargs() if options else []
    async with proc.run('rclone', 'purge', filepath, *opts) as _:
        pass


async def serve_http(remote_path: str, addr: str, read_only: bool = True, options: RcloneOptions | None = None):
    opts = options.asargs() if options else []
    args = ['serve', 'http', remote_path, '--addr', addr] + opts
    if read_only:
        args.append('--read-only')
    return await asyncio.create_subprocess_exec('rclone', *args)


async def differs(local_file: str, remote_file: str, options: RcloneOptions | None = None):
    
    remote_dir = os.path.dirname(remote_file)
    
    opts = options.asargs() if options else []
    try:
        result = await proc.read('rclone', 'cryptcheck', '--match', '-', local_file, remote_dir, *opts)
        result = result.strip()

        return result != os.path.basename(local_file)
    except proc.ExitCodeError:
        return True


async def _test():
    async for p in find_files('jotta:/plex-media/Movies', pattern=re.compile(r'^.*\.mp4$')):
        print(p)

if __name__ == '__main__':
    asyncio.run(_test())
