#!/bin/bash

set -eux

go get github.com/ildus/pqt
go get github.com/stretchr/testify/assert
go install -gcflags '-N -l'
pushd tests
go test
popd
