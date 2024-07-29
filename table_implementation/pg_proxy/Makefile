# contrib/pg_proxy/Makefile

MODULE_big = pg_proxy
OBJS = \
	$(WIN32RES) \
	proxy.o

EXTENSION = pg_proxy
DATA = pg_proxy--1.0.sql
PG_CFLAGS = -lev
PG_CPPFLAGS = -ljson-c
SHLIB_LINK = -lev
PG_CFLAGS += -I/usr/include/json-c
PG_LDFLAGS += -lev -ljson-c -lbson-1.0
PG_CPPFLAGS += -I/usr/include/json-c
SHLIB_LINK += -lev -ljson-c -lpq -lbson-1.0

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_proxy
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif