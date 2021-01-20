#!/usr/bin/env bash

set -o nounset
set -o pipefail
set -o errexit

docker build -t simplecrop -f Dockerfile ..
