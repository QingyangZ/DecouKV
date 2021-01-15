CC=g++
CFLAGS=-std=c++11 -g -Wall -pthread  -fgnu-tm
INCLUDES=-I ./
LDFLAGS= -lpthread
SUBDIRS=db core
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(OBJECTS) $(EXEC)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@


$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@

clean:
	$(RM) $(OBJECTS)
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

