/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/cons.h"

static lispval build_info=VOID;
static lispval _kno_features_symbol;

#define config_int(var)                                 \
  kno_store(build_info,kno_intern(# var),KNO_INT(var));
#define config_bool(var)                                        \
  kno_add(build_info,_kno_features_symbol,kno_intern(# var));

#define config_string(var) config_string_helper(# var,var)

static void config_string_helper(u8_string var,u8_string val)
{
  lispval string=kno_mkstring(val);
  kno_store(build_info,kno_intern(var),string);
  kno_decref(string);
}

KNO_EXPORT lispval kno_get_build_info()
{
  return kno_incref(build_info);
}

KNO_EXPORT lispval config_get_build_info(lispval var,void *data)
{
  return kno_incref(build_info);
}

KNO_EXPORT void kno_init_build_info()
{
  _kno_features_symbol=kno_intern("features");
  if (VOIDP(build_info))
    build_info=kno_make_slotmap(64,0,NULL);

#ifdef KNO_WORDS_ARE_ALIGNED
  config_bool(KNO_WORDS_ARE_ALIGNED);
#endif
#ifdef HAVE_STDATOMIC_H
  config_bool(HAVE_STDATOMIC_H);
#endif
#ifdef KNO_SHARE_DIR
  config_string(KNO_SHARE_DIR);
#endif
#ifdef KNO_PROFILING_ENABLED
  config_bool(KNO_PROFILING_ENABLED);
#endif

  config_int(SIZEOF_INT);
  config_int(SIZEOF_SHORT);
  config_int(SIZEOF_LONG);
  config_int(SIZEOF_LONG_LONG);
  config_int(SIZEOF_VOID_P);
  config_int(SIZEOF_TIME_T);
  config_int(SIZEOF_OFF_T);
  config_int(SIZEOF_SIZE_T);
  config_int(SIZEOF_LISPVAL);

#ifdef KNO_DEFAULT_MALLOC
  config_string(KNO_DEFAULT_MALLOC);
#endif

#ifdef KNO_EXEC_WRAPPER
  config_string(KNO_EXEC_WRAPPER);
#endif
#ifdef KNO_LOCAL_MODULE_DIR
  config_string(KNO_LOCAL_MODULE_DIR);
#endif
#ifdef KNO_INSTALLED_MODULE_DIR
  config_string(KNO_INSTALLED_MODULE_DIR);
#endif
#ifdef KNO_STDLIB_MODULE_DIR
  config_string(KNO_STDLIB_MODULE_DIR);
#endif

#ifdef KNO_LIBSCM_DIR
  config_string(KNO_LIBSCM_DIR);
#endif
#ifdef KNO_BOOT_CONFIG
  config_string(KNO_BOOT_CONFIG);
#endif
#ifdef KNO_EXEC
  config_string(KNO_EXEC);
#endif
#ifdef KNO_DBSERVER
  config_string(KNO_DBSERVER);
#endif

#ifdef KNO_LARGEFILES_ENABLED
  config_bool(KNO_LARGEFILES_ENABLED);
#endif
#ifdef KNO_PREFETCHING_ENABLED
  config_bool(KNO_PREFETCHING_ENABLED);
#endif
#ifdef KNO_PROFILING_ENABLED
  config_bool(KNO_PROFILING_ENABLED);
#endif
#ifdef KNO_CALLTRACK_ENABLED
  config_bool(KNO_CALLTRACK_ENABLED);
#endif
#ifdef KNO_FILECONFIG_ENABLED
  config_bool(KNO_FILECONFIG_ENABLED);
#endif
#ifdef KNO_HTMLDUMP_ENABLED
  config_bool(KNO_HTMLDUMP_ENABLED);
#endif
#ifdef KNO_IPEVAL_ENABLED
  config_bool(KNO_IPEVAL_ENABLED);
#endif

#ifdef HAVE_STDATOMIC_H
  config_bool(HAVE_STDATOMIC_H);
#endif
#ifdef HAVE_PTHREAD_H
  config_bool(HAVE_PTHREAD_H);
#endif
#ifdef HAVE_CRYPTO_SET_LOCKING_CALLBACK
  config_bool(HAVE_CRYPTO_SET_LOCKING_CALLBACK);
#endif
#ifdef HAVE_UCHAR
  config_bool(HAVE_UCHAR);
#endif
#ifdef HAVE_SYS_FILE_H
  config_bool(HAVE_SYS_FILE_H);
#endif
#ifdef HAVE_SYS_RESOURCE_H
  config_bool(HAVE_SYS_RESOURCE_H);
#endif
#ifdef HAVE_RESOURCE_H
  config_bool(HAVE_RESOURCE_H);
#endif
#ifdef HAVE_STRUCT_RUSAGE_RU_INBLOCK
  config_bool(HAVE_STRUCT_RUSAGE_RU_INBLOCK);
#endif
#ifdef HAVE_STRUCT_RUSAGE_RU_NVCSW
  config_bool(HAVE_STRUCT_RUSAGE_RU_NVCSW);
#endif
#ifdef HAVE_STRUCT_RUSAGE_RU_MAJFLT
  config_bool(HAVE_STRUCT_RUSAGE_RU_MAJFLT);
#endif
#ifdef HAVE_STRUCT_RUSAGE_RU_NSWAP
  config_bool(HAVE_STRUCT_RUSAGE_RU_NSWAP);
#endif
#ifdef HAVE_SYS_TYPES_H
  config_bool(HAVE_SYS_TYPES_H);
#endif
#ifdef HAVE_SYS_WAIT_H
  config_bool(HAVE_SYS_WAIT_H);
#endif
#ifdef HAVE_SIGNAL_H
  config_bool(HAVE_SIGNAL_H);
#endif
#ifdef HAVE_STRDUP
  config_bool(HAVE_STRDUP);
#endif
#ifdef HAVE_STRNDUP
  config_bool(HAVE_STRNDUP);
#endif
#ifdef HAVE_FLOCK
  config_bool(HAVE_FLOCK);
#endif
#ifdef HAVE_LOCKF
  config_bool(HAVE_LOCKF);
#endif
#ifdef HAVE_FCNTL
  config_bool(HAVE_FCNTL);
#endif
#ifdef HAVE_SYS_SYSINFO_H
  config_bool(HAVE_SYS_SYSINFO_H);
#endif
#ifdef HAVE_SYSINFO
  config_bool(HAVE_SYSINFO);
#endif
#ifdef HAVE_SYSCONF
  config_bool(HAVE_SYSCONF);
#endif
#ifdef HAVE_SYSCTL
  config_bool(HAVE_SYSCTL);
#endif
#ifdef HAVE_PREAD
  config_bool(HAVE_PREAD);
#endif
#ifdef HAVE_MMAP
  config_bool(HAVE_MMAP);
#endif
#ifdef HAVE_MKSTEMP
  config_bool(HAVE_MKSTEMP);
#endif
#ifdef HAVE_FSEEKO
  config_bool(HAVE_FSEEKO);
#endif
#ifdef HAVE_REALPATH
  config_bool(HAVE_REALPATH);
#endif
#ifdef HAVE_SYS_UTSNAME_H
  config_bool(HAVE_SYS_UTSNAME_H);
#endif
#ifdef HAVE_UNAME
  config_bool(HAVE_UNAME);
#endif
#ifdef HAVE_STATFS
  config_bool(HAVE_STATFS);
#endif
#ifdef HAVE_SLEEP
  config_bool(HAVE_SLEEP);
#endif
#ifdef HAVE_NANOSLEEP
  config_bool(HAVE_NANOSLEEP);
#endif
#ifdef HAVE_GETUID
  config_bool(HAVE_GETUID);
#endif
#ifdef HAVE_GETEUID
  config_bool(HAVE_GETEUID);
#endif
#ifdef HAVE_GETGID
  config_bool(HAVE_GETGID);
#endif
#ifdef HAVE_GETEGID
  config_bool(HAVE_GETEGID);
#endif
#ifdef HAVE_SETUID
  config_bool(HAVE_SETUID);
#endif
#ifdef HAVE_SETEUID
  config_bool(HAVE_SETEUID);
#endif
#ifdef HAVE_SETGID
  config_bool(HAVE_SETGID);
#endif
#ifdef HAVE_SETEGID
  config_bool(HAVE_SETEGID);
#endif
#ifdef HAVE_GETPWNAM
  config_bool(HAVE_GETPWNAM);
#endif
#ifdef HAVE_GETPWNAM_R
  config_bool(HAVE_GETPWNAM_R);
#endif
#ifdef HAVE_GETGRNAM
  config_bool(HAVE_GETGRNAM);
#endif
#ifdef HAVE_GETGRNAM_R
  config_bool(HAVE_GETGRNAM_R);
#endif
#ifdef HAVE_UNISTD_H
  config_bool(HAVE_UNISTD_H);
#endif
#ifdef HAVE_SYS_STAT_H
  config_bool(HAVE_SYS_STAT_H);
#endif
#ifdef HAVE_SYS_VFS_H
  config_bool(HAVE_SYS_VFS_H);
#endif
#ifdef HAVE_SYS_STATFS_H
  config_bool(HAVE_SYS_STATFS_H);
#endif
#ifdef HAVE_PWD_H
  config_bool(HAVE_PWD_H);
#endif
#ifdef HAVE_STRFTIME
  config_bool(HAVE_STRFTIME);
#endif
#ifdef HAVE_GETTIMEOFDAY
  config_bool(HAVE_GETTIMEOFDAY);
#endif
#ifdef HAVE_FTIME
  config_bool(HAVE_FTIME);
#endif
#ifdef HAVE_TM_ZONE
  config_bool(HAVE_TM_ZONE);
#endif
#ifdef HAVE_TM_GMTOFF
  config_bool(HAVE_TM_GMTOFF);
#endif
#ifdef HAVE_LOCALTIME_R
  config_bool(HAVE_LOCALTIME_R);
#endif
#ifdef HAVE_GMTIME_R
  config_bool(HAVE_GMTIME_R);
#endif
#ifdef HAVE_SIGPROCMASK
  config_bool(HAVE_SIGPROCMASK);
#endif
#ifdef HAVE_SIGSETMASK
  config_bool(HAVE_SIGSETMASK);
#endif
#ifdef HAVE_CONSTRUCTOR_ATTRIBUTES
  config_bool(HAVE_CONSTRUCTOR_ATTRIBUTES);
#endif
#ifdef HAVE_THREAD_STORAGE_CLASS
  config_bool(HAVE_THREAD_STORAGE_CLASS);
#endif
#ifdef HAVE_BUILTIN_EXPECT
  config_bool(HAVE_BUILTIN_EXPECT);
#endif
#ifdef HAVE_BUILTIN_PREFETCH
  config_bool(HAVE_BUILTIN_PREFETCH);
#endif
#ifdef HAVE_HISTEDIT_H
  config_bool(HAVE_HISTEDIT_H);
#endif
#ifdef HAVE_EDITLINE_READLINE_H
  config_bool(HAVE_EDITLINE_READLINE_H);
#endif
#ifdef HAVE_LIBEDIT
  config_bool(HAVE_LIBEDIT);
#endif
#ifdef HAVE_CURL_CURL_H
  config_bool(HAVE_CURL_CURL_H);
#endif
#ifdef HAVE_LIBEXIF_EXIF_DATA_H
  config_bool(HAVE_LIBEXIF_EXIF_DATA_H);
#endif
#ifdef HAVE_GOOGLE_PROFILER_H
  config_bool(HAVE_GOOGLE_PROFILER_H);
#endif
#ifdef HAVE_LIBPROFILER
  config_bool(HAVE_LIBPROFILER);
#endif
#ifdef HAVE_GPERFTOOLS_HEAP_PROFILER_H
  config_bool(HAVE_GPERFTOOLS_HEAP_PROFILER_H);
#endif
#ifdef HAVE_GPERFTOOLS_HEAP_CHECKER_H
  config_bool(HAVE_GPERFTOOLS_HEAP_CHECKER_H);
#endif
#ifdef HAVE_GPERFTOOLS_PROFILER_H
  config_bool(HAVE_GPERFTOOLS_PROFILER_H);
#endif
#ifdef HAVE_GPERFTOOLS_MALLOC_EXTENSION_C_H
  config_bool(HAVE_GPERFTOOLS_MALLOC_EXTENSION_C_H);
#endif
#ifdef HAVE_SQL_H
  config_bool(HAVE_SQL_H);
#endif
#ifdef HAVE_SQLITE3_H
  config_bool(HAVE_SQLITE3_H);
#endif
#ifdef HAVE_SQLITE3_OPEN_V2
  config_bool(HAVE_SQLITE3_OPEN_V2);
#endif
#ifdef HAVE_SQLITE3_PREPARE_V2
  config_bool(HAVE_SQLITE3_PREPARE_V2);
#endif
#ifdef HAVE_SQLITE3_CLOSE_V2
  config_bool(HAVE_SQLITE3_CLOSE_V2);
#endif
#ifdef HAVE_SQLITE3_ERRSTR
  config_bool(HAVE_SQLITE3_ERRSTR);
#endif
#ifdef HAVE_FFI_H
  config_bool(HAVE_FFI_H);
#endif
#ifdef HAVE_LIBFFI
  config_bool(HAVE_LIBFFI);
#endif
#ifdef HAVE_ARCHIVE_H
  config_bool(HAVE_ARCHIVE_H);
#endif
#ifdef HAVE_LIBARCHIVE
  config_bool(HAVE_LIBARCHIVE);
#endif
#ifdef HAVE_LIBDUMA
  config_bool(HAVE_LIBDUMA);
#endif
#ifdef HAVE_DUMA_H
  config_bool(HAVE_DUMA_H);
#endif
#ifdef HAVE_MALLOC_H
  config_bool(HAVE_MALLOC_H);
#endif
#ifdef HAVE_MALLOC_MALLOC_H
  config_bool(HAVE_MALLOC_MALLOC_H);
#endif
#ifdef HAVE_MCHECK_H
  config_bool(HAVE_MCHECK_H);
#endif
#ifdef HAVE_MALLOC_ZONE_STATISTICS
  config_bool(HAVE_MALLOC_ZONE_STATISTICS);
#endif
#ifdef HAVE_MALLINFO
  config_bool(HAVE_MALLINFO);
#endif
#ifdef HAVE_MALLOC_INFO
  config_bool(HAVE_MALLOC_INFO);
#endif
#ifdef HAVE_OPEN_MEMSTREAM
  config_bool(HAVE_OPEN_MEMSTREAM);
#endif
#ifdef HAVE_MSTATS
  config_bool(HAVE_MSTATS);
#endif
#ifdef HAVE_MTRACE
  config_bool(HAVE_MTRACE);
#endif
#ifdef HAVE_GOOGLE_PROFILER_H
  config_bool(HAVE_GOOGLE_PROFILER_H);
#endif
#ifdef HAVE_FCGIAPP_H
  config_bool(HAVE_FCGIAPP_H);
#endif
#ifdef HAVE_LIBFCGI
  config_bool(HAVE_LIBFCGI);
#endif
#ifdef HAVE_UCHAR
  config_bool(HAVE_UCHAR);
#endif
#ifdef HAVE_MAGICK_WAND_H
  config_bool(HAVE_MAGICK_WAND_H);
#endif
#ifdef HAVE_TIDY_TIDY_H
  config_bool(HAVE_TIDY_TIDY_H);
#endif
#ifdef HAVE_TIDY_TIDYBUFFIO_H
  config_bool(HAVE_TIDY_TIDYBUFFIO_H);
#endif
#ifdef HAVE_TIDY_H
  config_bool(HAVE_TIDY_H);
#endif
#ifdef HAVE_TIDYBUFFIO_H
  config_bool(HAVE_TIDYBUFFIO_H);
#endif
#ifdef HAVE_WAITPID
  config_bool(HAVE_WAITPID);
#endif
#ifdef HAVE_WAIT4
  config_bool(HAVE_WAIT4);
#endif
#ifdef HAVE_SNAPPYC_H
  config_bool(HAVE_SNAPPYC_H);
#endif
#ifdef HAVE_LIBSNAPPY
  config_bool(HAVE_LIBSNAPPY);
#endif
#ifdef HAVE_LIBZIP
  config_bool(HAVE_LIBZIP);
#endif
#ifdef HAVE_ZIP_SET_FILE_EXTRA
  config_bool(HAVE_ZIP_SET_FILE_EXTRA);
#endif
#ifdef HAVE_ZIP_SET_FILE_COMMENT
  config_bool(HAVE_ZIP_SET_FILE_COMMENT);
#endif
#ifdef HAVE_ZIP_SET_FILE_COMPRESSION
  config_bool(HAVE_ZIP_SET_FILE_COMPRESSION);
#endif

#if (WORDS_BIGENDIAN)
  kno_add(build_info,kno_intern("byte_order"),
          kno_intern("little_endian"));
#else
  kno_add(build_info,kno_intern("byte_order"),
          kno_intern("big_endian"));
#endif

  config_string(KNO_VERSION);
  config_string(KNO_REVISION);
  config_string(KNO_SHARE_DIR);
  config_string(KNO_DATA_DIR);
  config_string(KNO_RUN_DIR);
  config_string(KNO_LOG_DIR);
  config_string(KNO_SERVLET_LOG_DIR);
  config_string(KNO_DAEMON_RUN_DIR);
  config_string(KNO_SERVLET_RUN_DIR);
  config_string(KNO_DAEMON_LOG_DIR);
  config_string(KNO_WEBUSER);
  config_string(KNO_WEBGROUP);
  config_string(KNO_DAEMON);
  config_string(KNO_ADMIN_GROUP);
  config_string(KNO_CONFIG_FILE_PATH);
  config_string(KNO_DEFAULT_LOADPATH);
  config_string(KNO_DEFAULT_DLOADPATH);
  config_string(KNO_DLOAD_SUFFIX);
  config_string(KNO_EXEC);
  config_string(KNO_DBSERVER);
  config_string(KNO_EXEC_WRAPPER);
  config_string(KNO_LOCAL_MODULE_DIR);
  config_string(KNO_INSTALLED_MODULE_DIR);
  config_string(KNO_STDLIB_MODULE_DIR);
  config_string(KNO_UNPACKAGE_DIR);

#ifdef KNO_WITH_FILE_LOCKING
  config_bool(KNO_WITH_FILE_LOCKING);
#endif
#ifdef TIME_WITH_SYS_TIME
  config_bool(TIME_WITH_SYS_TIME);
#endif
#ifdef TM_IN_SYS_TIME
  config_bool(TM_IN_SYS_TIME);
#endif
#ifdef WORDS_BIGENDIAN
  config_bool(WORDS_BIGENDIAN);
#endif
#ifdef TIME_WITH_SYS_TIME
  config_bool(TIME_WITH_SYS_TIME);
#endif
#ifdef TM_IN_SYS_TIME
  config_bool(TM_IN_SYS_TIME);
#endif
#ifdef KNO_WITH_EXIF
  config_bool(KNO_WITH_EXIF);
#endif
#ifdef KNO_WITH_PROFILING
  config_bool(KNO_WITH_PROFILING);
#endif
#ifdef KNO_WITH_PROFILER
  config_bool(KNO_WITH_PROFILER);
#endif
#ifdef KNO_ENABLE_FFI
  config_bool(KNO_ENABLE_FFI);
#endif
#ifdef WITH_GOOGLE_PROFILER
  config_bool(WITH_GOOGLE_PROFILER);
#endif
#ifdef WITH_FASTCGI
  config_bool(WITH_FASTCGI);
#endif
#ifdef U8_THREAD_DEBUG
  config_bool(U8_THREAD_DEBUG);
#endif

#ifdef KNO_MAJOR_VERSION
  config_int(KNO_MAJOR_VERSION);
#endif
#ifdef KNO_MINOR_VERSION
  config_int(KNO_MINOR_VERSION);
#endif
#ifdef KNO_RELEASE_VERSION
  config_int(KNO_RELEASE_VERSION);
#endif
#ifdef KNO_PATCH_LEVEL
  config_int(KNO_PATCH_LEVEL);
#endif
#ifdef KNO_BRANCH
  config_string(KNO_BRANCH);
#endif


#ifdef KNO_ENABLE_LOCKFREE
  config_bool(KNO_ENABLE_LOCKFREE);
#endif
#ifdef KNO_FORCE_TLS
  config_bool(KNO_FORCE_TLS);
#endif
#ifdef KNO_GLOBAL_IPEVAL
  config_bool(KNO_GLOBAL_IPEVAL);
#endif
#ifdef KNO_WITH_CURL
  config_bool(KNO_WITH_CURL);
#endif
#ifdef KNO_WITH_EDITLINE
  config_bool(KNO_WITH_EDITLINE);
#endif
#ifdef KNO_N_PTRLOCKS
  config_int(KNO_N_PTRLOCKS);
#endif

#ifdef KNO_BUGJAR_DIR
  config_string(KNO_BUGJAR_DIR);
#endif

  kno_register_config("BUILDINFO",
                      "Information about compile-time features",
                      config_get_build_info,NULL,NULL);

  struct KNO_SLOTMAP *table= (kno_slotmap) build_info;
  KNO_XTABLE_SET_READONLY(table,1);
}

