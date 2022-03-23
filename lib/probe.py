import asyncio
import json
import logging
import shlex
from pprint import pprint
from typing import Optional

from pydantic import BaseModel, Field, validator

from . import proc

log = logging.getLogger(__name__)


class ProbeTags(BaseModel):
    encoder: Optional[str] = Field(None, alias='ENCODER')
    duration: Optional[str] = Field(None, alias='DURATION')

    @validator('duration')
    def parse_duration(cls, v: str) -> float:
        parts = reversed(v.split(':'))
        return sum(float(p) * 60 ** i for i, p in enumerate(parts))

    class Config:
        allow_population_by_field_name = True


class ProbeFormat(BaseModel):
    duration: float
    tags: Optional[ProbeTags]
    bit_rate: int


class ProbeStream(BaseModel):
    index: int
    bits_per_raw_sample: Optional[int]
    codec_name: Optional[str]
    codec_type: str
    channels: int = 0
    pix_fmt: Optional[str]
    profile: Optional[str]
    tags: Optional[ProbeTags]

    @property
    def is_hdr(self):
        return \
            (self.bits_per_raw_sample is not None and self.bits_per_raw_sample >= 10) or \
            (self.pix_fmt is not None and '10' in self.pix_fmt) or \
            (self.pix_fmt is not None and '16' in self.pix_fmt) or \
            (self.profile is not None and '10' in self.profile)


class ProbeData(BaseModel):
    format: ProbeFormat
    streams: list[ProbeStream]

    @property
    def video_stream(self) -> Optional[ProbeStream]:
        return next(( s for s in self.streams if s.codec_type == 'video' ), None)

    @property
    def audio_streams(self) -> list[ProbeStream]:
        return [ s for s in self.streams if s.codec_type == 'audio' ]

    @property
    def video_duration(self) -> float:
        video = self.video_stream
        if video is not None:
            tags = video.tags
            if tags is not None:
                duration = tags.duration
                if duration is not None:
                    return float(duration)
        return self.format.duration


async def probe(file: str):
    args = ['-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', file]
    log.debug('ffprobe %s', shlex.join(args))
    data = await proc.read('ffprobe', *args)
    data_dict = json.loads(data)
    return ProbeData(**data_dict)


async def _test():
    pprint(await probe('test_files/264.mp4'))

if __name__ == '__main__':
    asyncio.run(_test())
