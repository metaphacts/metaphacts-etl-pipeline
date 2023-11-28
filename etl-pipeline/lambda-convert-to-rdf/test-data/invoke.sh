#!/bin/sh

REQUEST="${1:-example-request.json}"

TESTDATA="$(dirname $0)/$REQUEST"

curl -X POST http://localhost:8080 --data "@$TESTDATA"
