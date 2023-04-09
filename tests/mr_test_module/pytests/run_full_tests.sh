#!/bin/bash
set -x
set -e

echo oss
DEBUG=$DEBUG ./run_tests.sh "$@"
echo single shard cluster
DEBUG=$DEBUG ./run_tests.sh --env oss-cluster --shards-count 1 "$@"
echo 2 shards cluster
DEBUG=$DEBUG ./run_tests.sh --env oss-cluster --shards-count 2 "$@"
echo 3 shards cluster
DEBUG=$DEBUG ./run_tests.sh --env oss-cluster --shards-count 3 "$@"

echo ssl certificates
bash ../generate_tests_cert.sh

echo 2 shards cluster ssl enabled
DEBUG=$DEBUG ./run_tests.sh --env oss-cluster --shards-count 2 --tls --tls-cert-file ../tests/tls/redis.crt --tls-key-file ../tests/tls/redis.key --tls-ca-cert-file ../tests/tls/ca.crt --tls-passphrase foobar "$@"

echo 3 shards cluster ssl enabled
DEBUG=$DEBUG ./run_tests.sh --env oss-cluster --shards-count 3 --tls --tls-cert-file ../tests/tls/redis.crt --tls-key-file ../tests/tls/redis.key --tls-ca-cert-file ../tests/tls/ca.crt --tls-passphrase foobar "$@"