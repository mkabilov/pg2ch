#!/bin/bash

set -eux

go install -gcflags '-N -l'
go test ./...

