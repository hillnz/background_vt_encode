#!/usr/bin/env bash

set -e

while true; do
    sleep 5

    encode_pid=
    count=0
    while [ "$encode_pid" = "" ] || ! kill -0 "$encode_pid"; do
        encode_pid="$(cat /tmp/com.hillnz.background_encode/encode.pid || printf '')"
        sleep 1
        count=$((count + 1))
        if [ "$count" -gt "10" ]; then
            exit
        fi
    done

    idle_time=$(($(ioreg -c IOHIDSystem | sed -e '/HIDIdleTime/ !{ d' -e 't' -e '}' -e 's/.* = //g' -e 'q') / 1000000000))

    hb_pid="$(cat /tmp/com.hillnz.background_encode/handbrake.pid || printf '')"
    if [ "$hb_pid" = "" ]; then
        continue
    fi

    if pmset -g batt | grep AC >/dev/null && [ $idle_time -gt 60 ]; then
        kill -CONT "$hb_pid" || true
    else
        kill -STOP "$hb_pid" || true
    fi

done &

while true; do
    ENCODE_VIDEO_DIR=nas:/pool/convert/download ENCODE_OUTPUT_DIR=nas:/pool/convert/output poetry run ./encode.py
    echo "Finished, will relaunch shortly"
    sleep 5
done
