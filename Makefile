# contrib/pg_freespacemap/Makefile

MODULE_big = pg_pretty_printer
OBJS = \
	$(WIN32RES) \
	pg_pretty_printer.o

EXTENSION = pg_pretty_printer
DATA = pg_pretty_printer--1.0.sql
PGFILEDESC = "pg_pretty_printer - monitoring of free space map"

override COMPILER = $(CXX) $(CFLAGS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_pretty_printer
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
