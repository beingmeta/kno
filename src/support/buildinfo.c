/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/cons.h"

static lispval build_info=VOID;
static lispval _fd_features_symbol;

#define config_int(var) \
  fd_store(build_info,fd_intern(# var),FD_INT(var));
#define config_bool(var) \
  fd_add(build_info,_fd_features_symbol,fd_intern(# var));

#define config_string(var) config_string_helper(# var,var)

static void config_string_helper(u8_string var,u8_string val)
{
  lispval string=lispval_string(val);
  fd_store(build_info,fd_intern(var),string);
  fd_decref(string);
}

FD_EXPORT lispval fd_get_build_info()
{
  return fd_incref(build_info);
}

FD_EXPORT lispval config_get_build_info(lispval var,void *data)
{
  return fd_incref(build_info);
}

FD_EXPORT void fd_init_build_info()
{
  _fd_features_symbol=fd_intern("FEATURES");
  if (VOIDP(build_info))
    build_info=fd_make_slotmap(64,0,NULL);

#ifdef FD_WORDS_ARE_ALIGNED
  config_bool(FD_WORDS_ARE_ALIGNED);
#endif
#ifdef HAVE_STDATOMIC_H
  config_bool(HAVE_STDATOMIC_H);
#endif
#ifdef FD_SHARE_DIR
  config_string(FD_SHARE_DIR);
#endif
#ifdef FD_PROFILING_ENABLED
  config_bool(FD_PROFILING_ENABLED);
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

#ifdef FD_DEFAULT_MALLOC
  config_string(FD_DEFAULT_MALLOC);
#endif

#ifdef FD_EXEC_WRAPPER
  config_string(FD_EXEC_WRAPPER);
#endif
#ifdef FD_LOCAL_MODULE_DIR
  config_string(FD_LOCAL_MODULE_DIR);
#endif
#ifdef FD_LOCAL_SAFE_MODULE_DIR
  config_string(FD_LOCAL_SAFE_MODULE_DIR);
#endif
#ifdef FD_INSTALLED_MODULE_DIR
  config_string(FD_INSTALLED_MODULE_DIR);
#endif
#ifdef FD_INSTALLED_SAFE_MODULE_DIR
  config_string(FD_INSTALLED_SAFE_MODULE_DIR);
#endif
#ifdef FD_BUILTIN_MODULE_DIR
  config_string(FD_BUILTIN_MODULE_DIR);
#endif
#ifdef FD_BUILTIN_SAFE_MODULE_DIR
  config_string(FD_BUILTIN_SAFE_MODULE_DIR);
#endif

#ifdef FD_LIBSCM_DIR
  config_string(FD_LIBSCM_DIR);
#endif
#ifdef FD_BOOT_CONFIG
  config_string(FD_BOOT_CONFIG);
#endif
#ifdef FD_EXEC
  config_string(FD_EXEC);
#endif
#ifdef FD_DBSERVER
  config_string(FD_DBSERVER);
#endif

#ifdef FD_LARGEFILES_ENABLED
  config_bool(FD_LARGEFILES_ENABLED);
#endif
#ifdef FD_PREFETCHING_ENABLED
  config_bool(FD_PREFETCHING_ENABLED);
#endif
#ifdef FD_PROFILING_ENABLED
  config_bool(FD_PROFILING_ENABLED);
#endif
#ifdef FD_CALLTRACK_ENABLED
  config_bool(FD_CALLTRACK_ENABLED);
#endif
#ifdef FD_FILECONFIG_ENABLED
  config_bool(FD_FILECONFIG_ENABLED);
#endif
#ifdef FD_HTMLDUMP_ENABLED
  config_bool(FD_HTMLDUMP_ENABLED);
#endif
#ifdef FD_IPEVAL_ENABLED
  config_bool(FD_IPEVAL_ENABLED);
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
  fd_add(build_info,fd_intern("BYTE_ORDER"),
         fd_intern("LITTLE_ENDIAN"));
#else
  fd_add(build_info,fd_intern("BYTE_ORDER"),
         fd_intern("BIG_ENDIAN"));
#endif

  config_string(FD_VERSION);
  config_string(FRAMERD_REVISION);
  config_string(FD_SHARE_DIR);
  config_string(FD_DATA_DIR);
  config_string(FD_RUN_DIR);
  config_string(FD_LOG_DIR);
  config_string(FD_SERVLET_LOG_DIR);
  config_string(FD_DAEMON_RUN_DIR);
  config_string(FD_SERVLET_RUN_DIR);
  config_string(FD_DAEMON_LOG_DIR);
  config_string(FD_WEBUSER);
  config_string(FD_WEBGROUP);
  config_string(FD_DAEMON);
  config_string(FD_ADMIN_GROUP);
  config_string(FD_CONFIG_FILE_PATH);
  config_string(FD_DEFAULT_LOADPATH);
  config_string(FD_DEFAULT_SAFE_LOADPATH);
  config_string(FD_DEFAULT_DLOADPATH);
  config_string(FD_DLOAD_SUFFIX);
  config_string(FD_EXEC);
  config_string(FD_DBSERVER);
  config_string(FD_EXEC_WRAPPER);
  config_string(FD_LOCAL_MODULE_DIR);
  config_string(FD_LOCAL_SAFE_MODULE_DIR);
  config_string(FD_INSTALLED_MODULE_DIR);
  config_string(FD_INSTALLED_SAFE_MODULE_DIR);
  config_string(FD_BUILTIN_MODULE_DIR);
  config_string(FD_BUILTIN_SAFE_MODULE_DIR);
  config_string(FD_UNPACKAGE_DIR);

#ifdef FD_WITH_FILE_LOCKING
  config_bool(FD_WITH_FILE_LOCKING);
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
#ifdef FD_WITH_EXIF
  config_bool(FD_WITH_EXIF);
#endif
#ifdef FD_WITH_PROFILING
  config_bool(FD_WITH_PROFILING);
#endif
#ifdef FD_WITH_PROFILER
  config_bool(FD_WITH_PROFILER);
#endif
#ifdef FD_ENABLE_FFI
  config_bool(FD_ENABLE_FFI);
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

#ifdef FD_MAJOR_VERSION
  config_int(FD_MAJOR_VERSION);
#endif
#ifdef FD_MINOR_VERSION
  config_int(FD_MINOR_VERSION);
#endif
#ifdef FD_RELEASE_VERSION
  config_int(FD_RELEASE_VERSION);
#endif

#ifdef FD_ENABLE_LOCKFREE
  config_bool(FD_ENABLE_LOCKFREE);
#endif
#ifdef FD_FORCE_TLS
  config_bool(FD_FORCE_TLS);
#endif
#ifdef FD_GLOBAL_IPEVAL
  config_bool(FD_GLOBAL_IPEVAL);
#endif
#ifdef FD_WITH_CURL
  config_bool(FD_WITH_CURL);
#endif
#ifdef FD_WITH_EDITLINE
  config_bool(FD_WITH_EDITLINE);
#endif
#ifdef FD_N_PTRLOCKS
  config_int(FD_N_PTRLOCKS);
#endif

#ifdef FD_BUGJAR_DIR
  config_string(FD_BUGJAR_DIR);
#endif

  fd_register_config("BUILDINFO",
                     "Information about compile-time features",
                     config_get_build_info,NULL,NULL);

  struct FD_SLOTMAP *table= (fd_slotmap) build_info;
  table->table_readonly=1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
