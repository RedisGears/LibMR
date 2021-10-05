all: libmr
clean: clean_libmr

build_deps:
	make -C deps/
	
libmr: build_deps
	make -C src/
	
run_tests:
	make -C ./tests/mr_test_module/ test
	
run_tests_valgrind:
	DEBUG=1 make -C ./tests/mr_test_module/ test_valgrind
	
clean_libmr:
	make clean -C src/
	
