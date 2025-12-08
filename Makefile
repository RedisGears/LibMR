all: libmr
clean: clean_libmr
	make clean -C ./tests/mr_test_module/

build_deps: check_system_deps
	make -C deps/

libmr_only:
	make -C src/

libmr: build_deps libmr_only

run_tests:
	make -C ./tests/mr_test_module/ test

run_tests_valgrind:
	DEBUG=1 make -C ./tests/mr_test_module/ test_valgrind

run_tests_ssl:
	make -C ./tests/mr_test_module/ test_ssl

clean_libmr:
	make clean -C src/

# Verify (and optionally install) system dependencies on Linux.
# Usage:
#   make build_deps                  # verify only, fail with guidance if missing
#   AUTO_INSTALL=1 make build_deps   # attempt sudo apt-get install on Ubuntu/Debian
.PHONY: check_system_deps
check_system_deps:
	@uname_S=$$(uname -s 2>/dev/null || echo not); \
	if [ "$$uname_S" = "Linux" ]; then \
	  set -e; \
	  missing=""; \
	  command -v llvm-config >/dev/null 2>&1 || missing="$$missing llvm/clang"; \
	  command -v clang >/dev/null 2>&1 || missing="$$missing clang"; \
	  command -v pkg-config >/dev/null 2>&1 || missing="$$missing pkg-config"; \
	  dpkg -s libssl-dev >/dev/null 2>&1 || missing="$$missing libssl-dev"; \
	  command -v make >/dev/null 2>&1 || missing="$$missing build-essential"; \
	  command -v automake >/dev/null 2>&1 || missing="$$missing automake"; \
	  command -v libtool >/dev/null 2>&1 || missing="$$missing libtool"; \
	  command -v python3 >/dev/null 2>&1 || missing="$$missing python3"; \
	  python3 -m pip --version >/dev/null 2>&1 || missing="$$missing python3-pip"; \
	  command -v redis-server >/dev/null 2>&1 || missing="$$missing redis-server"; \
	  if [ -n "$$missing" ]; then \
	    if [ "$$AUTO_INSTALL" = "1" ]; then \
	      echo "Installing missing packages: $$missing"; \
	      sudo apt-get update; \
	      sudo apt-get install -y llvm clang libclang-dev libssl-dev pkg-config build-essential automake libtool python3-venv python3-pip redis-server; \
	    else \
	      echo "Missing system packages on Linux:$$missing"; \
	      echo "Install them, or re-run with: AUTO_INSTALL=1 make build_deps"; \
	      exit 1; \
	    fi; \
	  fi; \
	fi

