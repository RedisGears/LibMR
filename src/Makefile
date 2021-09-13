ifeq ($(DEBUG),1)
	GCC_FLAGS=-O0 -g -DREDISMODULE_EXPERIMENTAL_API
else
	GCC_FLAGS=-O2 -DREDISMODULE_EXPERIMENTAL_API
endif

GCC_FLAGS+=-fvisibility=hidden -fPIC -DREDISMODULE_EXPERIMENTAL_API \
-I../deps/hiredis/ \
-I../deps/hiredis/adapters/ \
-I../deps/libevent/include/

SOURCES=mr.o record.o cluster.o event_loop.o utils/adlist.o utils/buffer.o utils/dict.o utils/thpool.o

HIREDIS=../deps/hiredis/libhiredis.a
LIBEVENT=../deps/libevent/.libs/libevent.a
LIBEVENT_PTHREADS=../deps/libevent/.libs/libevent_pthreads.a

ARTIFACT_NAME=libmr

all: $(ARTIFACT_NAME)

%.o : %.c
	gcc -c $(GCC_FLAGS) $< -o $@ -DMODULE_NAME=$(MODULE_NAME)

$(ARTIFACT_NAME): $(SOURCES)
	ld $(SOURCES) $(HIREDIS) $(LIBEVENT) $(LIBEVENT_PTHREADS) -r -o $(ARTIFACT_NAME).o
	objcopy --localize-hidden $(ARTIFACT_NAME).o
	ar rcs $(ARTIFACT_NAME).a $(ARTIFACT_NAME).o

clean:
	rm $(ARTIFACT_NAME).a *.o utils/*.o
