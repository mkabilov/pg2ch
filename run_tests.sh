#!/bin/bash

set -eux

go get github.com/ildus/pqt
go install -gcflags '-N -l'
pushd tests
go test
popd
