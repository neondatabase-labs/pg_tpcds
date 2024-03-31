# Copyright (c) 2022-2023, PostgreSQL Global Development Group

pg_trgm_sources = files(
  'pg_pretty_printer.c',
)

if host_system == 'windows'
  pg_trgm_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_pretty_printer',
    '--FILEDESC', 'pg_pretty_printer - trigram matching',])
endif

pg_pretty_printer = shared_module('pg_pretty_printer',
  pg_trgm_sources,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_pretty_printer

install_data(
  'pg_pretty_printer--1.0.sql',
  'pg_pretty_printer.control',
  kwargs: contrib_data_args,
)


