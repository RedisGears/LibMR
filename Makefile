all: libmr
clean: clean_libmr
	make clean -C ./tests/mr_test_module/

build_deps:
	make -C deps/

libmr_only:
	make -C src/

libmr: build_deps libmr_only

# Auto-detect environment for tests
uname_S := $(shell uname -s 2>/dev/null)
ifeq ($(uname_S),Darwin)
	LIBCLANG_PATH ?= $(shell brew --prefix llvm 2>/dev/null)/lib
	OPENSSL_PREFIX ?= $(shell brew --prefix openssl 2>/dev/null || echo /usr/local/opt/openssl)
	PKG_CONFIG_PATH ?= $(shell echo "$(OPENSSL_PREFIX)/lib/pkgconfig")
	VENV_PYTHON := $(shell test -x $(CURDIR)/.venv/bin/python && echo $(CURDIR)/.venv/bin/python || echo python)
else
	LIBCLANG_PATH ?= $(shell llvm-config --libdir 2>/dev/null)
	OPENSSL_PREFIX ?= /usr
	PKG_CONFIG_PATH ?= $(shell echo /usr/lib/$$(dpkg-architecture -qDEB_HOST_MULTIARCH 2>/dev/null || echo x86_64-linux-gnu)/pkgconfig)
	VENV_PYTHON := $(shell test -x $(CURDIR)/.venv/bin/python && echo $(CURDIR)/.venv/bin/python || echo python3)
endif

run_tests:
	LIBCLANG_PATH="$(LIBCLANG_PATH)" OPENSSL_PREFIX="$(OPENSSL_PREFIX)" PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" PYTHON="$(VENV_PYTHON)" make -C ./tests/mr_test_module/ test

run_tests_valgrind:
	LIBCLANG_PATH="$(LIBCLANG_PATH)" OPENSSL_PREFIX="$(OPENSSL_PREFIX)" PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" PYTHON="$(VENV_PYTHON)" DEBUG=1 make -C ./tests/mr_test_module/ test_valgrind

run_tests_ssl:
	LIBCLANG_PATH="$(LIBCLANG_PATH)" OPENSSL_PREFIX="$(OPENSSL_PREFIX)" PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" PYTHON="$(VENV_PYTHON)" make -C ./tests/mr_test_module/ test_ssl

clean_libmr:
	make clean -C src/

