#!/bin/bash

set -e
set -x

pg_ctlcluster 12 main start
