
ifeq ($(VG),1)
override VALGRIND:=1
endif

ifeq ($(VALGRIND),1)
override DEBUG:=1
endif

ROOT=.
MK.pyver:=3
include deps/readies/mk/main

BINDIR=$(BINROOT)/$(SRCDIR)

#----------------------------------------------------------------------------------------------

define HELP
make setup      # install packages required for build
make fetch      # download and prepare dependant modules (i.e., python, libevent)

make build
  DEBUG=1         # build debug variant
  VG|VALGRIND=1   # build with VALGRIND (implies DEBUG=1)
  VARIANT=name    # build as variant `name`
  DEPS=1          # also build dependant modules
make clean        # remove binary files
  ALL=1           # remove binary directories
  DEPS=1          # also clean dependant modules

make deps          # build dependant modules
make libevent      # build libevent
make hiredis       # build hiredis
make all           # build all libraries and packages

make test          # run tests
  VG|VALGRIND=1    # run tests with Valgrind
  TEST=test        # run specific `test` with Python debugger
  TEST_ARGS=args   # additional RLTest arguments
  GDB=1            # (with TEST=...) run with GDB

make platform      # build for given platform
  OSNICK=nick      # OS/distribution to build for
  ARTIFACTS=1      # copy artifacts from builder container
  TEST=1           # run tests
endef

MK_ALL_TARGETS=bindirs deps build

include $(MK)/defs

#----------------------------------------------------------------------------------------------

DEPENDENCIES=libevent hiredis

ifneq ($(filter all deps $(DEPENDENCIES),$(MAKECMDGOALS)),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

LIBEVENT_BINDIR=bin/$(FULL_VARIANT.release)/libevent
include build/libevent/Makefile.defs

#----------------------------------------------------------------------------------------------

HIREDIS_BINDIR=bin/$(FULL_VARIANT.release)/hiredis
include build/hiredis/Makefile.defs

#----------------------------------------------------------------------------------------------

CC=gcc
SRCDIR=src

define _SOURCES:=
	mr.c
	record.c
	cluster.c
	event_loop.c
	crc16.c
	utils/adlist.c
	utils/buffer.c
	utils/dict.c
	utils/thpool.c
	utils/siphash.c
endef

SOURCES=$(addprefix $(SRCDIR)/,$(filter %,$(subst $(__NL), ,$(_SOURCES))))
OBJECTS=$(patsubst $(SRCDIR)/%.c,$(BINDIR)/%.o,$(SOURCES))

CC_DEPS = $(patsubst $(SRCDIR)/%.c, $(BINDIR)/%.d, $(SOURCES))

define _CC_FLAGS
	-fPIC
	-std=gnu99
	-MMD
	-MF $(@:.o=.d)

	
	-I$(SRCDIR)
	-I$(BINDIR)
	-Ideps
	-I.
	-Ideps/libevent/include
	-Ibin/$(FULL_VARIANT.release)/libevent/include
	-Ideps/hiredis
	-Ideps/hiredis/adapters

	-DMODULE_NAME=jojo
	-DREDISMODULE_EXPERIMENTAL_API
endef
CC_FLAGS += $(filter %,$(subst $(__NL), ,$(_CC_FLAGS)))

TARGET=$(BINROOT)/libmr.a

ifeq ($(VALGRIND),1)
CC_FLAGS += -DVALGRIND
endif

ifeq ($(DEBUG),1)
CC_FLAGS += -g -O0
LD_FLAGS += -g
else
CC_FLAGS += -O2 -Wno-unused-result
endif

ifeq ($(OS),macos)
LD_FLAGS += \
	-framework CoreFoundation \
	-undefined dynamic_lookup
endif

#----------------------------------------------------------------------------------------------

ifeq ($(SHOW_LD_LIBS),1)
LD_FLAGS += -Wl,-t
endif

ifeq ($(OS),macos)
EMBEDDED_LIBS += $(LIBEVENT) $(HIREDIS)
endif

MISSING_DEPS:=
ifeq ($(wildcard $(LIBEVENT)),)
MISSING_DEPS += $(LIBEVENT)
endif

ifeq ($(wildcard $(HIREDIS)),)
MISSING_DEPS += $(HIREDIS)
endif

#----------------------------------------------------------------------------------------------

ifneq ($(MISSING_DEPS),)
DEPS=1
endif

#----------------------------------------------------------------------------------------------

.NOTPARALLEL:

MK_CUSTOM_CLEAN=1

.PHONY: deps $(DEPENDENCIES) static test setup fetch platform

include $(MK)/rules

#----------------------------------------------------------------------------------------------

-include $(CC_DEPS)

$(BINDIR)/%.o: $(SRCDIR)/%.c
	@echo Compiling $<...
	$(SHOW)$(CC) $(CC_FLAGS) -c $< -o $@

#----------------------------------------------------------------------------------------------

$(TARGET): $(MISSING_DEPS) $(OBJECTS)
	@echo Creating $@...
	$(SHOW)$(AR) rcs $@ $(OBJECTS)
	$(SHOW)$(READIES)/bin/symlink -f -d bin -t $(TARGET)

#----------------------------------------------------------------------------------------------

# we avoid $(SUDO) here since we expect 'sudo make setup'

setup:
	@echo Setting up system...
	$(SHOW)./deps/readies/bin/getpy3
	$(SHOW)python3 ./system-setup.py

fetch:
	$(SHOW)$(MAKE) --no-print-directory -C build/libevent source

#----------------------------------------------------------------------------------------------

ifeq ($(DEPS),1)

deps: $(LIBPYTHON) $(LIBEVENT) $(HIREDIS) $(GEARS_PYTHON)

#----------------------------------------------------------------------------------------------

libevent: $(LIBEVENT)

$(LIBEVENT):
	@echo Building libevent...
	$(SHOW)$(MAKE) --no-print-directory -C build/libevent DEBUG=

#----------------------------------------------------------------------------------------------

hiredis: $(HIREDIS)

$(HIREDIS):
	@echo Building hiredis...
	$(SHOW)$(MAKE) --no-print-directory -C build/hiredis DEBUG=

#----------------------------------------------------------------------------------------------

else

deps: ;

endif # DEPS

#----------------------------------------------------------------------------------------------

ifeq ($(DIAG),1)
$(info *** MK_CLEAN_DIR=$(MK_CLEAN_DIR))
$(info *** LIBEVENT=$(LIBEVENT))
$(info *** HIREDIS=$(HIREDIS))
$(info *** MISSING_DEPS=$(MISSING_DEPS))
endif

clean:
ifeq ($(ALL),1)
	$(SHOW)rm -rf $(BINDIR) $(TARGET) $(TARGET.snapshot) $(notdir $(TARGET)) $(BINROOT)/redislabs
else
	-$(SHOW)find $(BINDIR) -name '*.[oadh]' -type f -delete
	$(SHOW)rm -f $(TARGET) $(TARGET.snapshot) $(TARGET:.so=.a)
endif
ifeq ($(DEPS),1)
	$(SHOW)$(foreach DEP,$(DEPENDENCIES),$(MAKE) --no-print-directory -C build/$(DEP) clean;)
endif

#----------------------------------------------------------------------------------------------

ifeq ($(GDB),1)
RLTEST_GDB=-i
endif

ifeq ($(VALGRIND),1)
TEST_FLAGS += VALGRIND=1
endif

test: __sep
ifneq ($(TEST),)
	@set -e; \
	cd pytest; \
	BB=1 $(TEST_FLAGS) python3 -m RLTest --test $(TEST) $(TEST_ARGS) \
		$(RLTEST_GDB) -s -v --module $(abspath $(TARGET))
else
	$(SHOW)set -e; \
	cd pytest; \
	$(TEST_FLAGS) MOD=$(abspath $(TARGET)) GEARSPY_PATH=$(abspath $(GEARS_PYTHON)) ./run_tests.sh
endif

#----------------------------------------------------------------------------------------------

platform:
	$(SHOW)make -C build/platforms build OSNICK=$(OSNICK) TEST=$(TEST) ARTIFACTS=$(ARTIFACTS)
