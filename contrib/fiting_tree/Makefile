# contrib/fiting_tree/Makefile

MODULE_big = fiting_tree
OBJS = \
	$(WIN32RES) \
	ft_handler.o \
	ft_build.o \
	ft_scan.o \
	ft_insert.o \
	ft_vacuum.o \
	ft_cost.o \
	ft_utils.o

EXTENSION = fiting_tree
DATA = fiting_tree--1.0.sql
PGFILEDESC = "fiting_tree - FITing-Tree learned index access method"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/fiting_tree
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
