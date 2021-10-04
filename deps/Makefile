all: build_hiredis build_libevent

build_hiredis:
	make -C ./hiredis/
	
build_libevent:
	cd libevent; autoconf -v -i -f; CFLAGS=-fPIC ./configure; make
	
clean:
	make -C ./hiredis/ clean
	make -C ./libevent/ clean
