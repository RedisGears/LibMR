ROOT=$(realpath ../../)
HERE=$(realpath ./)

ifeq ($(DEBUG),1)
	EXTRA_ARGS=
	MODULE_PATH=$(HERE)/target/debug/libmr_test.so
	DEBUG_RUN=1
else
	EXTRA_ARGS=--release
	MODULE_PATH=$(HERE)/target/release/libmr_test.so
	DEBUG_RUN=0
endif

ifeq ($(COVERAGE),1)
	COVERAGE_RUN=1
else
	COVERAGE_RUN=0
endif

RUSTFLAGS=-L$(ROOT)/src/

all: build

build_libmr:
	DEBUG=$(DEBUG_RUN) COVERAGE=$(COVERAGE_RUN) MODULE_NAME=MRTESTS make -C $(ROOT)/src/

build: build_libmr
	RUSTFLAGS="$(RUSTFLAGS)" cargo build $(EXTRA_ARGS)

run: build
	redis-server --loadmodule $(MODULE_PATH)

run_cluster: build
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-only --env oss-cluster --shards-count 3 -s

test: build
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-reuse -t ./pytests/
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-reuse -t ./pytests/ --env oss-cluster --shards-count 1
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-reuse -t ./pytests/ --env oss-cluster --shards-count 2
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-reuse -t ./pytests/ --env oss-cluster --shards-count 3

test_valgrind: build
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-reuse -t ./pytests/ -V
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-reuse -t ./pytests/ --env oss-cluster --shards-count 1 -V
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-reuse -t ./pytests/ --env oss-cluster --shards-count 2 -V
	DEBUG=$(DEBUG_RUN) $(HERE)/pytests/run_tests.sh --env-reuse -t ./pytests/ --env oss-cluster --shards-count 3 -V

clean:
	cargo clean
