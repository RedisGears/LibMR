all: build_hiredis build_libevent

build_hiredis:
	make -C ./hiredis/

build_libevent:
	if !([ -f ./build_libevent_done ]) then \
	cd libevent; autoreconf -v -i -f; CFLAGS=-fPIC ./configure; make; cd ../ ; touch ./build_libevent_done; \
	fi
	
clean:
	make -C ./hiredis/ clean
	make -C ./libevent/ clean
