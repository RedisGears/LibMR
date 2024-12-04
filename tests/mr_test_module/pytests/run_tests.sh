#!/usr/bin/env bash

[[ $VERBOSE == 1 ]] && set -x
[[ $IGNERR == 1 ]] || set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/../../../ && pwd)
OS=$(uname -s 2>/dev/null)
if [[ $OS == Darwin ]]; then
    LIB_EXTENTION=dylib
else
    LIB_EXTENTION=so
fi

if [[ $DEBUG == 1 ]]; then
    MODULE_PATH=$HERE/../target/debug/libmr_test.$LIB_EXTENTION
else
    MODULE_PATH=$HERE/../target/release/libmr_test.$LIB_EXTENTION
fi


python3 -m RLTest --no-progress --verbose-information-on-failure --module $MODULE_PATH --clear-logs "$@" --oss_password "password"
