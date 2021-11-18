#!/usr/bin/env python3

import argparse
import collections
import io
import json
import os
import sys
from datetime import datetime

import tempfile
from time import sleep

import singer
from google.cloud import storage
from jsonschema.validators import Draft4Validator

logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug("Emitting state {}".format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key="", sep="__"):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def _load_to_gcs(client, bucket_name, object_path, object_name, file_to_load):
    file_to_load.flush()
    blob_name = os.path.join(object_path, object_name)

    # check if file is not empty
    if os.stat(file_to_load.name).st_size == 0:
        # logger.info("loading gs://{}/{} to GCS".format(bucket_name, blob_name))
        pass
    else:
        logger.info("loading gs://{}/{} to GCS".format(bucket_name, blob_name))

        # run load job or raise error
        try:
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(file_to_load.name)

            # truncate rows from sent file
            file_to_load.seek(0)
            file_to_load.truncate()

            return True

        except Exception as err:
            logger.error(
                "failed to load to bucket {} from file: {}".format(bucket_name, str(err))
            )
            raise err

def sync_records(tmp_dir, stream_config, bucket_name, object_path, now, append_timestamp_folder, nb_previous_stream):
    for stream, tmp_file in stream_config.items():
        _load_to_gcs(
            client=storage.Client(),
            bucket_name=bucket_name,
            object_path=os.path.join(object_path, stream, now)
            if append_timestamp_folder
            else os.path.join(object_path, stream),
            object_name=f"{stream}_{nb_previous_stream}.json",
            file_to_load=tmp_file,
        )

    tmp_dir.cleanup()



def persist_lines(config, lines):

    bucket_name = config["bucket_name"]
    object_path = config.get("object_path", "")
    append_timestamp_folder = config.get("append_timestamp_folder", False)

    sync_batch = config.get("sync_batch", 1000000)
    sync_if_stream_changes = config.get("sync_if_stream_changes", False)

    stream_config = dict()
    previous_stream = None

    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    now = datetime.now().strftime("%Y%m%dT%H%M%S")

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if "type" not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o["type"]

        if t == "RECORD":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )

            if previous_stream is None:
                previous_stream = o["stream"]

            # If the record needs to be flattened, uncomment this line
            flattened_record = flatten(o["record"])

            # create empty temp file for every new stream
            if o["stream"] not in stream_config:
                stream_config[o["stream"]] = {}
                stream_config[o["stream"]]['tmp_file'] = open(tempfile.NamedTemporaryFile().name, 'w+t') # open(os.path.join(tmp_dir.name, stream), "w+t")
                stream_config[o["stream"]]['nb_sync'] = 1
                stream_config[o["stream"]]['nb_records'] = 0

            # add new row
            stream_config[o["stream"]]['tmp_file'].write(str(flattened_record))
            stream_config[o["stream"]]['tmp_file'].write("\n")

            # add nb received records
            stream_config[o["stream"]]['nb_records'] += 1

            # sync previous stream or current stream if it reach nb_records to sync
            if stream_config[o["stream"]]['nb_records'] % sync_batch == 0 or (previous_stream != o["stream"] and sync_if_stream_changes):
                # get stream to sync
                stream = None
                if stream_config[o["stream"]]['nb_records'] % sync_batch == 0:
                    stream = o["stream"]
                    logger.debug(f"Sync {stream} : records reached nb records to sync")
                else:
                    stream = previous_stream
                    logger.debug("Sync {} records : received new stream {}".format(stream, o["stream"]))

                _load_to_gcs(
                    client=storage.Client(),
                    bucket_name=bucket_name,
                    object_path=os.path.join(object_path, stream, now) if append_timestamp_folder else os.path.join(object_path, stream),
                    object_name="{}_{}.json".format(stream, stream_config[stream]['nb_sync']),
                    file_to_load=stream_config[stream]['tmp_file'],
                )

                stream_config[stream]['nb_sync'] += 1

            previous_stream = o["stream"]
            state = None
        elif t == "STATE":
            logger.debug("Setting state to {}".format(o["value"]))
            state = o["value"]

        elif t == "SCHEMA":
            if "stream" not in o:
                raise Exception(
                    "Line is missing required key 'stream': {}".format(line)
                )
            stream = o["stream"]
            schemas[stream] = o["schema"]
            validators[stream] = Draft4Validator(o["schema"])
            if "key_properties" not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o["key_properties"]
        else:
            logger.warning(
                "Unknown message type {} in message {}".format(o["type"], o)
            )

    for stream in stream_config.keys():
        _load_to_gcs(
            client=storage.Client(),
            bucket_name=bucket_name,
            object_path=os.path.join(object_path, stream, now) if append_timestamp_folder else os.path.join(object_path, stream),
            object_name="{}_{}.json".format(stream, stream_config[stream]['nb_sync']),
            file_to_load=stream_config[stream]['tmp_file'],
        )

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file")
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    state = persist_lines(config, input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == "__main__":
    main()
