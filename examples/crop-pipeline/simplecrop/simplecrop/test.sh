#!/usr/bin/env bash

./build.sh && cd test && ./sc

echo
echo "Diffing test run with stored data"

diff -r ../output output
