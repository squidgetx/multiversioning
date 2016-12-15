CFLAGS=-O3 -g -Wall -Wextra -Werror -std=c++0x -Wno-sign-compare
CFLAGS+=-DSNAPSHOT_ISOLATION=0 -DSMALL_RECORDS=0 -DREAD_COMMITTED=1
LIBS=-lnuma -lpthread -lrt -lcityhash 
CXX=g++

LIBPATH=./libs/lib/
INC_DIRS=include libs/include
INCLUDE=$(foreach d, $(INC_DIRS), -I$d)
SRC_DIRS=src src/logging
SOURCES:=$(foreach d, $(SRC_DIRS), $(wildcard $(d)/*.cc $(d)/*.c))
HEKATON:=$(wildcard $(SRC)/hek*.cc $(SRC)/hek*.c)
HEK_OBJ:=$(patsubst $(SRC)/%.cc,build/%.o,$(HEKATON))
OBJECTS:=$(patsubst src/%.cc,build/%.o,$(SOURCES))
START:=$(wildcard start/*.cc start/*.c)
START_OBJECTS:=$(patsubst start/%.cc,start/%.o,$(START))
TEST:=test
TESTSOURCES:=$(wildcard $(TEST)/*.cc)
TESTOBJECTS:=$(patsubst test/%.cc,test/%.o,$(TESTSOURCES))
NON_HEK_OBJECTS:=$(filter-out $(HEK_OBJ),$(OBJECTS))
NON_MAIN_STARTS:=$(filter-out start/main.o,$(START_OBJECTS))


DEPSDIR:=.deps
DEPCFLAGS=-MD -MF $(DEPSDIR)/$*.d -MP

all:CFLAGS+=-DTESTING=0 -DUSE_BACKOFF=1 -fno-omit-frame-pointer
all:env build/db

test:CFLAGS+=-DTESTING=1 -DUSE_BACKOFF=1 
test:env build/tests

-include $(wildcard $(DEPSDIR)/*.d) $(wildcard $(DEPSDIR/logging/*.d))

build/%.o: src/%.cc $(DEPSDIR)/stamp GNUmakefile
	@mkdir -p build
	@echo + cc $<
	@$(CXX) $(CFLAGS) $(DEPCFLAGS) $(INCLUDE) -c -o $@ $<

$(TESTOBJECTS):$(OBJECTS)

test/%.o: test/%.cc $(DEPSDIR)/stamp GNUmakefile
	@echo + cc $<
	@$(CXX) $(CFLAGS) -Wno-missing-field-initializers -Wno-conversion-null $(DEPCFLAGS) -Istart -I$(SRC) -I$(INCLUDE) -c -o $@ $<

start/%.o: start/%.cc $(DEPSDIR)/stamp GNUmakefile
	@echo + cc $<
	@$(CXX) $(CFLAGS) $(DEPCFLAGS) $(INCLUDE) -Istart -c -o $@ $<

build/db:$(START_OBJECTS) $(OBJECTS)
	@$(CXX) $(CFLAGS) -o $@ $^ -L$(LIBPATH) $(LIBS)

build/tests:$(OBJECTS) $(TESTOBJECTS) $(NON_MAIN_STARTS)
	@$(CXX) $(CFLAGS) -o $@ $^ $(LIBS)

$(DEPSDIR)/stamp:
	@mkdir -p $(DEPSDIR)
	@touch $@

.PHONY: clean env

env:
	@mkdir -p $(DEPSDIR) $(DEPSDIR)/logging build/logging

clean:
	rm -rf build $(DEPSDIR) $(TESTOBJECTS) start/*.o
