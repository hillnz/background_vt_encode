# renovate: datasource=docker depName=python
ARG PYTHON_VERSION=3.12.2
FROM --platform=$BUILDPLATFORM python:${PYTHON_VERSION} AS deps

RUN pip install poetry

ADD pyproject.toml poetry.lock /app/
RUN cd /app && poetry export --without-hashes >requirements.txt

FROM python:${PYTHON_VERSION}

RUN apt-get update && apt-get install -y \
    ffmpeg

RUN curl https://rclone.org/install.sh | bash

RUN groupadd --gid 1000 -r app && \
    useradd --no-log-init --uid 1000 -r -g app app && \
    mkdir -p /app /home/app && \
    chown -R app:app /app /home/app

ADD . /app
WORKDIR /app
COPY --from=deps /app/requirements.txt /app/
USER app
RUN pip install -r requirements.txt

ENTRYPOINT [ "python" ]
CMD [ "./encode.py" ]
