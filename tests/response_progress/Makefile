# Uses the same LibMR archive as the main test module (make libmr first).
CC ?= cc
PYTHON ?= python3
LIBMR_A ?= ../../src/libmr.a
LDLIBS ?= -lssl -lcrypto -lpthread

module.so: module.c $(LIBMR_A)
	$(CC) -shared -fPIC -DREDISMODULE_EXPERIMENTAL_API -I../../src -o $@ $< $(LIBMR_A) $(LDLIBS)

.PHONY: test clean
test: module.so
	$(PYTHON) -m RLTest --module $(CURDIR)/module.so --env oss-cluster --shards-count 3 --randomize-ports --no-progress --enable-debug-command --log-dir ../mr_test_module/logs/response-progress $(ARGS) --test test_progress.py:test_response_progress

clean:
	rm -f module.so
