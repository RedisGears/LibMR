#!/bin/bash

[[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/../../../ && pwd)
if [[ $DEBUG == 1 ]]; then
    MODULE_PATH=$HERE/../target/debug/libmr_test.so
else
    MODULE_PATH=$HERE/../target/release/libmr_test.so
fi


python3 -m RLTest --module $MODULE_PATH --clear-logs "$@" --module-args "password" --oss_password "password"
