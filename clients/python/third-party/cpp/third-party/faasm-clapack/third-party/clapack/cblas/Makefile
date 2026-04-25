TOPDIR=..
include $(TOPDIR)/make.inc

SRCS = $(wildcard *.c)
OBJS = $(patsubst %.c,%.o,$(SRCS))

all: $(CBLASLIB)

$(CBLASLIB): $(OBJS)
ifeq ($(LIBEXT), .a)
	$(ARCH) $(ARCHFLAGS) $@ $(OBJS)
	$(RANLIB) $@
else
	$(CC) $(LDFLAGS) $(OBJS) -o $@
endif

%.o: %.c 
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o

