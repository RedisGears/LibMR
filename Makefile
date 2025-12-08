all: libmr
clean: clean_libmr
	make clean -C ./tests/mr_test_module/

build_deps:
	make -C deps/

libmr_only:
	make -C src/

libmr: build_deps libmr_only

# Auto-detect environment and set variables for tests
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
ifeq ($(uname_S),Darwin)
	# macOS: use Homebrew paths
	LIBCLANG_PATH ?= $(shell /bin/sh -c 'if command -v brew >/dev/null 2>&1; then P=$$(brew --prefix llvm 2>/dev/null); [ -n "$$P" ] && echo "$$P/lib" || echo ""; else echo ""; fi')
	OPENSSL_PREFIX ?= $(shell /bin/sh -c 'command -v brew >/dev/null 2>&1 && brew --prefix openssl 2>/dev/null || echo /usr/local/opt/openssl')
	PKG_CONFIG_PATH ?= $(shell /bin/sh -c 'if command -v brew >/dev/null 2>&1; then P=$$(brew --prefix openssl 2>/dev/null); [ -n "$$P" ] && echo "$$P/lib/pkgconfig" || echo /usr/local/opt/openssl/lib/pkgconfig; else echo /usr/local/opt/openssl/lib/pkgconfig; fi')
	# Auto-detect venv python (use absolute path)
	VENV_PYTHON := $(shell test -x $(CURDIR)/.venv/bin/python && echo $(CURDIR)/.venv/bin/python || echo python)
else
	# Linux: use system paths
	LIBCLANG_PATH ?= $(shell llvm-config --libdir 2>/dev/null || echo "")
	OPENSSL_PREFIX ?= /usr
	# PKG_CONFIG_PATH: try multiarch path first, fallback to standard locations
	# Works on x86_64, arm64, and other architectures
	PKG_CONFIG_PATH ?= $(shell /bin/sh -c 'ARCH=$$(dpkg-architecture -qDEB_HOST_MULTIARCH 2>/dev/null || echo x86_64-linux-gnu); if [ -d "/usr/lib/$$ARCH/pkgconfig" ]; then echo "/usr/lib/$$ARCH/pkgconfig"; elif [ -d "/usr/lib/pkgconfig" ]; then echo "/usr/lib/pkgconfig"; else echo ""; fi')
	# Auto-detect venv python (use absolute path)
	VENV_PYTHON := $(shell test -x $(CURDIR)/.venv/bin/python && echo $(CURDIR)/.venv/bin/python || echo python3)
endif

run_tests:
	@echo "Running tests with:"
	@echo "  LIBCLANG_PATH=$(LIBCLANG_PATH)"
	@echo "  OPENSSL_PREFIX=$(OPENSSL_PREFIX)"
	@echo "  PYTHON=$(VENV_PYTHON)"
	LIBCLANG_PATH="$(LIBCLANG_PATH)" \
	OPENSSL_PREFIX="$(OPENSSL_PREFIX)" \
	PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" \
	PYTHON="$(VENV_PYTHON)" \
	make -C ./tests/mr_test_module/ test

run_tests_valgrind:
	LIBCLANG_PATH="$(LIBCLANG_PATH)" \
	OPENSSL_PREFIX="$(OPENSSL_PREFIX)" \
	PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" \
	PYTHON="$(VENV_PYTHON)" \
	DEBUG=1 make -C ./tests/mr_test_module/ test_valgrind

run_tests_ssl:
	LIBCLANG_PATH="$(LIBCLANG_PATH)" \
	OPENSSL_PREFIX="$(OPENSSL_PREFIX)" \
	PKG_CONFIG_PATH="$(PKG_CONFIG_PATH)" \
	PYTHON="$(VENV_PYTHON)" \
	make -C ./tests/mr_test_module/ test_ssl

clean_libmr:
	make clean -C src/

