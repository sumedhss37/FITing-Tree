# Copyright (c) 2024, PostgreSQL Global Development Group
# CS349 — FITing-Tree learned index (Checkpoint 1)

fiting_tree_sources = files(
  'ft_handler.c',
  'ft_build.c',
  'ft_scan.c',
  'ft_insert.c',
  'ft_vacuum.c',
  'ft_cost.c',
  'ft_utils.c',
)

if host_system == 'windows'
  fiting_tree_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'fiting_tree',
    '--FILEDESC', 'fiting_tree - FITing-Tree learned index access method',
  ])
endif

fiting_tree = shared_module('fiting_tree',
  fiting_tree_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += fiting_tree

install_data(
  'fiting_tree.control',
  'fiting_tree--1.0.sql',
  kwargs: contrib_data_args,
)
