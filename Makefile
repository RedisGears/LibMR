all: libmr
clean: clean_libmr

deps:
	make -C deps/
	
libmr:
	make -C src/
	
clean_libmr:
	make clean -C src/
	
