#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
if [[ $(uname -s) == Darwin ]]; then
    gc_flags=-Wl,-dead_strip
    openssl_prefix=${OPENSSL_PREFIX:-$(brew --prefix openssl)}
else
    gc_flags=-Wl,--gc-sections
    openssl_prefix=${OPENSSL_PREFIX:-/usr}
fi
# CFLAGS intentionally supports multiple compiler arguments.
# shellcheck disable=SC2086
"${CC:-cc}" ${CFLAGS:-} -DREDISMODULE_EXPERIMENTAL_API -DREDISMODULE_MAIN \
    -DREDISMODULE_SDK_RLEC -DMODULE_NAME=MRTESTS \
    -ffunction-sections -fdata-sections -g -O0 \
    -Ideps/hiredis -Ideps/hiredis/adapters -Ideps/libevent/include \
    -I"$openssl_prefix/include" -L"$openssl_prefix/lib" \
    tests/unit/cluster_topology.c src/utils/dict.c src/utils/adlist.c src/utils/siphash.c \
    "$gc_flags" deps/hiredis/libhiredis.a deps/hiredis/libhiredis_ssl.a \
    deps/libevent/.libs/libevent.a -lssl -lcrypto -lpthread -o "$out/topology-test"
"$out/topology-test"
