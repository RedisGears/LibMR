ROOT=$(realpath ../../)
HERE=$(realpath ./)

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
ifeq ($(uname_S),Darwin)
	MODULE_EXT=dylib
else
	MODULE_EXT=so
endif

ifeq ($(DEBUG),1)
	EXTRA_ARGS=
	MODULE_PATH=$(HERE)/target/debug/libmr_test.$(MODULE_EXT)
	DEBUG_RUN=1
else
	EXTRA_ARGS=--release
	MODULE_PATH=$(HERE)/target/release/libmr_test.$(MODULE_EXT)
	DEBUG_RUN=0
endif

ifeq ($(COVERAGE),1)
	COVERAGE_RUN=1
else
	COVERAGE_RUN=0
endif

RUSTFLAGS=-L$(ROOT)/src/
MODULE_NAME=MRTESTS

all: build

build_libmr:
	DEBUG=$(DEBUG_RUN) COVERAGE=$(COVERAGE_RUN) OPENSSL_PREFIX=$(OPENSSL_PREFIX) MODULE_NAME=$(MODULE_NAME) make -C $(ROOT)/src/

build: build_libmr
	LIBCLANG_PATH="$(LIBCLANG_PATH)" \
	OPENSSL_PREFIX="$(OPENSSL_PREFIX)" \
	PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" \
	RUSTFLAGS="$(RUSTFLAGS)" \
	MODULE_NAME=$(MODULE_NAME) \
	cargo build $(EXTRA_ARGS)

run: build
	redis-server --loadmodule $(MODULE_PATH)

run_cluster: build
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-only --env oss-cluster --shards-count 3 -s

test: build
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/ --env oss-cluster --shards-count 1
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/ --env oss-cluster --shards-count 2
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/ --env oss-cluster --shards-count 3

test_ssl: build
	bash ./generate_tests_cert.sh
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/ --env oss-cluster --shards-count 2 --tls --tls-cert-file ./tests/tls/redis.crt --tls-key-file ./tests/tls/redis.key --tls-ca-cert-file ./tests/tls/ca.crt --tls-passphrase foobar
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/ --env oss-cluster --shards-count 3 --tls --tls-cert-file ./tests/tls/redis.crt --tls-key-file ./tests/tls/redis.key --tls-ca-cert-file ./tests/tls/ca.crt --tls-passphrase foobar

test_valgrind: build
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/ -V --vg-suppressions ./leakcheck.supp
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/ --env oss-cluster --shards-count 1 -V --vg-suppressions ./leakcheck.supp
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/ --env oss-cluster --shards-count 2 -V --vg-suppressions ./leakcheck.supp
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh -t ./pytests/ --env oss-cluster --shards-count 3 -V --vg-suppressions ./leakcheck.supp

clean:
	cargo clean
