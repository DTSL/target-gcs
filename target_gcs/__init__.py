#!/usr/bin/env python3

import argparse
import collections
import io
import json
import os
import sys
from datetime import datetime

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
    blob_name = os.path.join(object_path, object_name)
    logger.info("loading gs://{}/{} to GCS".format(bucket_name, blob_name))

    # run load job or raise error
    try:

        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        file_to_load.flush()
        blob.upload_from_filename(file_to_load.name)

        return True

    except Exception as err:
        logger.error(
            "failed to load to bucket {} from file: {}".format(bucket_name, str(err))
        )
        raise err


def persist_lines(config, lines):
    import tempfile

    bucket_name = config["bucket_name"]
    object_path = config.get("object_path", "")
    append_timestamp_folder = config.get("append_timestamp_folder", False)

    tmp_dir = tempfile.TemporaryDirectory()
    tmp_files = dict()

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
            if o["stream"] not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        o["stream"]
                    )
                )

            # Get schema for this record's stream
            schema = schemas[o["stream"]]

            # Validate record
            validators[o["stream"]].validate(o["record"])

            # If the record needs to be flattened, uncomment this line
            flattened_record = flatten(o["record"])

            # create empty temp file for every new stream
            if o["stream"] not in tmp_files:
                tmp_files[o["stream"]] = open(
                    os.path.join(tmp_dir.name, o["stream"]), "w+t"
                )

            # add new row
            tmp_file = tmp_files[o["stream"]]
            tmp_file.write(str(flattened_record))
            tmp_file.write("\n")

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

    for stream, tmp_file in tmp_files.items():
        _load_to_gcs(
            client=storage.Client(),
            bucket_name=bucket_name,
            object_path=os.path.join(object_path, stream, now)
            if append_timestamp_folder
            else os.path.join(object_path, stream),
            object_name=f"{stream}.json",
            file_to_load=tmp_file,
        )
    tmp_dir.cleanup()

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
