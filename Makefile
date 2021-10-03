all: libmr
clean: clean_libmr

build_deps:
	make -C deps/
	
libmr:
	make -C src/
	
run_tests:
	make -C ./tests/mr_test_module/ test
	
clean_libmr:
	make clean -C src/
	
