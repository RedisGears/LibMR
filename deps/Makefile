all: build_hiredis build_libevent

build_hiredis: 
ifneq ("$(wildcard built_hiredis)","")
	echo hiredis already built
else
	MAKEFLAGS='' USE_SSL=1 make -C ./hiredis/
endif
	touch built_hiredis

build_libevent:
ifneq ("$(wildcard built_libevent)","")
	echo libevent already built
else
	cd libevent; autoreconf -v -i -f; CFLAGS=-fPIC ./configure; make
endif
	touch built_libevent
	
clean:
	make -C ./hiredis/ clean
	make -C ./libevent/ clean
	rm built_libevent
	rm built_hiredis
