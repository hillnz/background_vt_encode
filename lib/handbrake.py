import asyncio
import logging
from math import floor
import os
import shlex
from itertools import chain
from os.path import basename, splitext

from . import proc
from .probe import probe


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


QUALITY = 50
X265_QUALITY = 22
AUDIO_BITRATE = 48


async def run(*args: str, **kwargs: str):

    extra_args = list(chain.from_iterable([ [k, v] for k, v in kwargs.items() ]))

    run_args = [
        *args,
        *extra_args
    ]

    log.debug('HandBrakeCLI %s', shlex.join(run_args))
    try:
        async with proc.run('HandBrakeCLI', *run_args, nice=10) as hb_proc:
            if 'pid_file' in kwargs:
                with open(kwargs['pid_file'], 'w') as f:
                    f.write(str(hb_proc.pid))
    finally:
        if 'pid_file' in kwargs:
            os.remove(kwargs['pid_file'])


async def encode(source_file: str, out_dir: str, pid_file: str):

    source_file_size = os.path.getsize(source_file)
    
    metadata = await probe(source_file)

    # format
    filename = basename(source_file)
    dest_file = os.path.join(out_dir, splitext(filename)[0] + '.mkv')

    # Check if already encoded
    if os.path.isfile(dest_file):
        try:
            dest_metadata = await probe(dest_file)
            if dest_metadata.video_stream:
                if dest_metadata.video_stream.codec_name == 'hevc' and abs(metadata.video_duration - dest_metadata.video_duration) < 5:
                    log.info('encode: %s - skipping', filename)
                    return dest_file
        except:
            pass


    # encoder
    video_stream = metadata.video_stream
    assert video_stream
    encoder = 'x265_10bit' # if video_stream.is_hdr else 'x265'

    # audio
    audio_streams = metadata.audio_streams
    audio_streams.sort(key=lambda s: s.index)
    channel_counts = [ s.channels for s in audio_streams ]
    audio_bitrates = [ AUDIO_BITRATE * c for c in channel_counts ]
        
    base_args = [
        '--no-comb-detect',
        '--no-deinterlace',
        '--no-detelecine',
        '--no-nlmeans',
        '--no-chroma-smooth',
        '--no-unsharp',
        '--no-lapsharp',
        '--no-deblock',
        '--all-subtitles',
        '--all-audio',
    ]

    base_kwargs = {
        '--input': source_file,
        '--output': dest_file,
        '--format': 'av_mkv',
        '--encoder': encoder,
        '--aencoder': 'opus',
        '--ab': ','.join(map(str, audio_bitrates)),
        '--mixdown': '7point1',
        '--quality': str(X265_QUALITY),
    }

    await run(*base_args, **base_kwargs, pid_file=pid_file)

    new_file_size = os.path.getsize(dest_file)
    if new_file_size > source_file_size:
        log.info('File too big, will use target bitrate')
        
        del base_kwargs['--quality']
        target_bitrate = floor((source_file_size * 8 / metadata.video_duration / 1000 - sum(audio_bitrates)) / 100) * 100 - 100
        base_kwargs['--vb'] = str(target_bitrate)
        base_args.append('--two-pass')
        await run(*base_args, **base_kwargs, pid_file=pid_file)

    return dest_file


if __name__ == '__main__':
    asyncio.run(encode(
        '/Users/jono/Developer/background_vt_encode/tmp/download/The Green Planet - S01E03 - Seasonal Worlds.mkv',
        '/tmp',
        '/tmp/hb_test.pid'
    ))
