all: build_hiredis build_libevent

GCC_FLAGS=-fPIC
ifeq ($(FOR_PROFILE),1)
	GCC_FLAGS+=-fno-omit-frame-pointer
endif

build_hiredis:
ifneq ("$(wildcard built_hiredis)","")
	echo hiredis already built
else
	MAKEFLAGS='' CFLAGS="$(GCC_FLAGS)" USE_SSL=1 make -C ./hiredis/
endif
	touch built_hiredis

build_libevent:
ifneq ("$(wildcard built_libevent)","")
	echo libevent already built
else
	cd libevent; autoreconf -v -i -f; CFLAGS="$(GCC_FLAGS)" ./configure PKG_CONFIG_PATH=$(PKG_CONFIG_PATH); make
endif
	touch built_libevent

clean:
	make -C ./hiredis/ clean
	make -C ./libevent/ clean
	rm built_libevent
	rm built_hiredis
