all: libmr
clean: clean_libmr
	make clean -C ./tests/mr_test_module/

build_deps:
	FOR_PROFILE=$(FOR_PROFILE) make -C deps/

libmr_only:
	FOR_PROFILE=$(FOR_PROFILE) make -C src/

libmr: build_deps libmr_only
	
run_tests:
	make -C ./tests/mr_test_module/ test
	
run_tests_valgrind:
	DEBUG=1 make -C ./tests/mr_test_module/ test_valgrind
	
run_tests_ssl:
	make -C ./tests/mr_test_module/ test_ssl
	
clean_libmr:
	make clean -C src/
	
