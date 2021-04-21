# h2olog-collector-gcs

A log collector for [h2olog](https://github.com/toru/h2olog) to store per-connection logs in Google Cloud Storage.

## Prerequisites

* Go compiler (>= 1.16)
* [h2olog](https://github.com/toru/h2olog)
* Google Cloud Storage bucket
* GCP authentication file named `authn.json`


## Build

`make all` to build a binary for the current machine.

Or, you can use `make release-linux` to build a binary for Linux.

## Copyright

Copyright (c) 2019-2020 Fastly, Inc., FUJI Goro

See LICENSE for the license.
