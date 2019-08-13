#!/bin/bash

set -eux

git clone https://github.com/adjust/istore.git
pushd istore
make install
popd

go install -gcflags '-N -l'
go test ./...
