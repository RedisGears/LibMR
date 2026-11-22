#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
build_dir=$(mktemp -d)
trap 'rm -rf "$build_dir"' EXIT
compiler=${CC:-clang}
link_flags=(-Wl,--gc-sections)
if [[ $(uname -s) == Darwin ]]; then
    compiler=${CC:-/usr/bin/clang}
    export SDKROOT=${SDKROOT:-$(xcrun --sdk macosx --show-sdk-path)}
    link_flags=(-Wl,-dead_strip)
fi
# Dead-section removal excludes unrelated Redis/network entry points from the
# included translation unit. Disable global ASan registration so unrelated
# callback tables do not retain those entry points. Heap/stack ASan and UBSan
# remain enabled to catch double release and stale access.
"$compiler" -std=gnu11 -g -O1 -ffunction-sections -fdata-sections \
    -fsanitize=address,undefined -mllvm -asan-globals=0 -DVALGRIND -DREDISMODULE_MAIN \
    -DREDISMODULE_EXPERIMENTAL_API -DREDISMODULE_SDK_RLEC -DMODULE_NAME=MRTESTS \
    -Ideps/hiredis -Ideps/libevent/include \
    tests/unit/execution_cleanup.c src/utils/adlist.c \
    "${link_flags[@]}" -lpthread -o "$build_dir/execution_cleanup"
"$build_dir/execution_cleanup"
