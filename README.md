# h2olog-collector-gcs

A log collector for [h2olog](https://github.com/toru/h2olog) to store per-connection logs in Google Cloud Storage.

## Prerequisites

* Go compiler (>= 1.16)
* [h2olog](https://github.com/toru/h2olog)
* Google Cloud Storage bucket
* GCP authentication file named `authn.json`
  * permission for `storage.objects.create` for the target bucket

## Build

`make all` to build a binary for the current machine.

Or, you can use `make release-linux` to build a binary for Linux.

## Visualize the logs

### Given `$URI` is a log object URI in GCS

`$URI` is a URI format: `gcs://$bucket/$object`.

To download it in the local filesystem (e.g. `object.json`):

```sh
gsutil cp $URI object.json
```

To show its metadata:

```sh
cat object.json | jq 'del(.payload)'
```

### Extract raw h2olog outputs from the log file

```sh
jq -c '.payload[]' < object.json > raw.jsonl
```

### Convert the output to QLog

```sh
qlog-adapter.py raw.jsonl > qlog.json
```

`qlog-adapter.py` is not bundled in this repo but placed in the h2o repo.

### Visualize it with QVis

Upload `qlog.json` to https://qvis.quictools.info/

## Copyright

Copyright (c) 2019-2020 Fastly, Inc., FUJI Goro

See LICENSE for the license.
