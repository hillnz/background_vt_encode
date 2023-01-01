#!/usr/bin/env python3

import asyncio
import logging
import os
from posixpath import splitext
import re
import socket
from asyncio.subprocess import Process
from contextlib import closing
from dataclasses import dataclass
from os import environ
from os.path import basename
from typing import AsyncIterable, List, Literal, Optional, cast
from urllib.parse import quote as quote_url
from asyncio import Lock
import aiohttp

from lib import handbrake, rclone
from lib.pipeline import pipeline
from lib.probe import ProbeData, probe
from lib.proc import ExitCodeError

# start_new_session

log = logging.getLogger(__name__)
# TODO
logging.basicConfig(level=logging.DEBUG)

Transcoder = Literal['handbrake', 'noop']

VIDEO_DIR = environ.get('ENCODE_VIDEO_DIR', 'jotta:/plex-media')
BUFFER_SIZE = int(environ.get('ENCODE_BUFFER_SIZE', '1'))
TRANSCODER = cast(Transcoder, (environ.get('ENCODE_TRANSCODER', 'handbrake')))
OUTPUT_DIR = environ.get('ENCODE_OUTPUT_DIR', '')

ENCODER_WHITELIST = [
    'Handbrake', 'Lavf'
]
VIDEO_EXTENSIONS = ['mkv', 'mp4', 'avi', 'mov', 'ts', 'm4v']
TARGET_CODEC = 'hevc'

RUN_DIR = '/tmp/com.hillnz.background_encode'
PID_FILE = os.path.join(RUN_DIR, 'encode.pid')
HB_PID_FILE = os.path.join(RUN_DIR, 'handbrake.pid')

THIS_DIR = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
TMP_DIR = os.path.join(THIS_DIR, 'tmp')
log.info(f'TMP_DIR: {TMP_DIR}')
DONE_LOG = os.path.join(TMP_DIR, 'done.log')
RESUME_LOG = os.path.join(TMP_DIR, 'resume.log')
BLACKLIST = os.path.join(THIS_DIR, 'blacklist.txt')

@dataclass(init=False)
class EncodeJob:
    extension: Optional[str] = None
    local_path: Optional[str] = None
    remote_path: Optional[str] = None
    http_server: Optional[Process] = None
    format: Optional[ProbeData] = None
    output_path: Optional[str] = None

Jobs = AsyncIterable[EncodeJob]

last_done_dir = ""
def write_done(job: EncodeJob):
    global last_done_dir

    if job.remote_path:
        with open(DONE_LOG, 'a') as f:
            f.write(f'{job.remote_path}\n')
        remote_dir = os.path.dirname(job.remote_path)
        if remote_dir != last_done_dir:
            with open(RESUME_LOG, 'a') as f:
                f.write(f'{remote_dir}\n')
            last_done_dir = remote_dir        


def read_done(job: EncodeJob):
    try:
        with open(DONE_LOG, 'r') as f:
            for line in f:
                if job.remote_path and line.startswith(job.remote_path):
                    return True
        with open(BLACKLIST, 'r') as f:
            for line in f:
                if job.remote_path and os.path.basename(job.remote_path).startswith(line.strip()):
                    return True
    except:
        pass
    return False


def get_done_dirs() -> list[str]:
    try:
        with open(RESUME_LOG, 'r') as f:
            return [ line.strip() for line in f ]
    except:
        return []
            

async def noop_transcode(source_file: str, output_dir: str):
    filename = os.path.basename(source_file)
    dest_file = os.path.join(output_dir, splitext(filename)[0] + '.mkv')
    # Wait for destination file to appear
    while True:
        if os.path.isfile(dest_file):
            return dest_file
        await asyncio.sleep(1)


list_lock = Lock()
async def list(jobs: Jobs):
    """List all files in VIDEO_DIR with the given extensions"""
    async with list_lock:
        log.info('list: %s', VIDEO_DIR)
        escaped = [ re.escape(j.extension) async for j in jobs if j.extension ]
        pattern = re.compile(r'^.*\.(' + '|'.join(escaped) + r')$')
        async for p in rclone.find_files(VIDEO_DIR, pattern, skip_dirs=get_done_dirs()):
            log.info('list: found %s', p)
            job = EncodeJob()
            job.extension = os.path.splitext(p)[1][1:]
            job.remote_path = p
            filename = os.path.basename(job.remote_path)
            job.output_path = os.path.join(OUTPUT_DIR or os.path.dirname(job.remote_path), splitext(filename)[0] + '.mkv')
            yield job, True


probe_lock = Lock()
async def probe_format(jobs: Jobs):
    async for job in jobs:
        async with probe_lock:

            if not job.remote_path:
                yield job, False
                continue
            
            encode = True
            for remote_path in [job.remote_path, job.output_path]:
                if not encode:
                    break

                assert remote_path
                
                if read_done(job):
                    log.info('probe: %s - skipping due to done.log', basename(remote_path))
                    encode = False
                    break

                # bind a port to get a random free one
                with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
                    s.bind(('', 0))
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    port = s.getsockname()[1]

                    addr = f'127.0.0.1:{port}'
                    job.http_server = await rclone.serve_http(remote_path, addr)
                    local_path = job.local_path = f'http://{addr}/{quote_url(os.path.basename(remote_path))}'
                    log.info('serve_http: %s', job.local_path)

                try:
                    # Wait for server to be up
                    success = False
                    async with aiohttp.ClientSession() as session:
                        for _ in range(5):
                            log.info('serve_http: awaiting %s', local_path)
                            try:
                                async with session.head(local_path) as resp:
                                    if resp.status == 200:
                                        success = True
                                        break
                            except aiohttp.ClientConnectorError:
                                pass
                            await asyncio.sleep(2)
                    if not success:
                        if remote_path == job.output_path:
                            break
                        raise Exception(f'serve_http: {local_path} failed')        

                    job.format = await probe(job.local_path)
                    video_stream = next(( s for s in job.format.streams if s.codec_type == 'video' ), None)
                    codec = video_stream.codec_name if video_stream else None
                    if job.format.format.tags:
                        encoder = job.format.format.tags.encoder or 'unknown'
                    else:
                        encoder = 'unknown'
                    log.info('probe_format: %s - codec %s encoder %s', 
                        basename(job.local_path), 
                        codec,
                        encoder)

                    if job.format.format.bit_rate < 400000:
                        log.info('probe_format: %s - skipping due to very low bitrate', basename(job.local_path))
                        write_done(job)
                        encode = False
                        continue

                    if any(encoder.startswith(e) for e in ENCODER_WHITELIST) and codec == TARGET_CODEC:
                        encode = False

                    os.makedirs(TMP_DIR, exist_ok=True)
                    if not encode:
                        write_done(job)
                        log.info('probe_format: %s - skipping', basename(job.local_path))

                finally:
                    job.http_server.kill()
                    job.http_server = None

                if job.output_path == job.remote_path:
                    break
            
            yield job, encode



download_lock = Lock()
async def download(jobs: Jobs):
    download_dir = os.path.join(TMP_DIR, 'download')
    os.makedirs(download_dir, exist_ok=True)
    
    async for job in jobs:
        async with download_lock:
            if not job.remote_path:
                yield job, False
                continue

            job.local_path = os.path.join(download_dir, os.path.basename(job.remote_path))
            
            if os.path.isfile(job.local_path) and not await rclone.differs(job.local_path, job.remote_path):
                log.info('download: %s - skipping', basename(job.local_path))
                yield job, True
                continue

            log.info('download: %s to %s', job.remote_path, job.local_path)
            tmp_file = job.local_path + '.tmp'
            await rclone.copy(job.remote_path, tmp_file)
            os.rename(tmp_file, job.local_path)
            yield job, True


transcode_lock = Lock()
async def transcode(jobs: Jobs):
    async for job in jobs:
        async with transcode_lock:
            if not job.local_path:
                yield job, False
                continue

            output_dir = os.path.join(TMP_DIR, 'output')
            os.makedirs(output_dir, exist_ok=True)

            if TRANSCODER == 'handbrake':
                log.info('transcode with handbrake: %s', basename(job.local_path))
                job.output_path = await handbrake.encode(job.local_path, output_dir, HB_PID_FILE)
            else:
                log.info('noop transcode: %s', basename(job.local_path))
                job.output_path = await noop_transcode(job.local_path, output_dir)

            log.info('transcode: output %s', basename(job.output_path))
            yield job, True


upload_lock = Lock()
async def upload(jobs: Jobs):
    async for job in jobs:
        async with upload_lock:
            if not job.output_path or not job.remote_path:
                yield job, False
                continue

            original_dir = os.path.dirname(job.remote_path)
            new_name = os.path.basename(job.output_path)
            new_path = os.path.join(OUTPUT_DIR or original_dir, new_name)
            tmp_path = new_path + '.tmp'

            log.info('upload: %s to %s', basename(job.output_path), new_path)
            await rclone.copy(job.output_path, tmp_path)
            log.info('delete: %s', basename(job.remote_path))
            await rclone.delete(job.remote_path)
            log.info('move: %s to %s', basename(tmp_path), basename(new_path))
            await rclone.move(tmp_path, new_path)

            write_done(job)
            yield job, True

async def cleanup(jobs: Jobs):
    global last_done_dir

    async for job in jobs:
        log.info('cleanup: %s', job.remote_path)
        if job.http_server:
            try:
                job.http_server.kill()
            except ExitCodeError:
                pass
            except GeneratorExit:
                pass
            job.http_server = None
        if read_done(job):
            if job.local_path and os.path.exists(job.local_path):
                os.remove(job.local_path)
            if job.output_path and os.path.exists(job.output_path):
                os.remove(job.output_path)
        yield job, True


async def start():
    """Start (or resume) encoding"""

    try:
        existing_pid = int(open(PID_FILE).read())
        # if process is running, exit
        try:
            os.kill(existing_pid, 0)
            log.error('already running, exiting')
            return
        except ProcessLookupError:
            pass
    except FileNotFoundError:
        pass

    try:
        os.makedirs(RUN_DIR, exist_ok=True)
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))


        steps = [
            list, 
            probe_format,
            download,
            transcode,
            upload,
            cleanup,
        ]
        
        jobs: List[EncodeJob] = []
        for ext in VIDEO_EXTENSIONS:
            job = EncodeJob()
            job.extension = ext
            jobs.append(job)

        [ _ async for _ in pipeline(*steps, initial_value=jobs, finally_=cleanup, buffer_size=BUFFER_SIZE) ]

    finally:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)


async def main():
    await start()


if __name__ == '__main__':
    asyncio.run(main())
