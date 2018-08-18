/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#if HAVE_GPERFTOOLS_PROFILER_H
#include <gperftools/profiler.h>
#endif
#if HAVE_GPERFTOOLS_HEAP_PROFILER_H
#include <gperftools/heap-profiler.h>
#endif
#if HAVE_GPERFTOOLS_MALLOC_EXTENSION_C_H
#include <gperftools/malloc_extension_c.h>
#endif

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8netfns.h>

#include <ctype.h>
#include <math.h>
#include <sys/time.h>

#if HAVE_MALLOC_H
#include <malloc.h>
#endif

#if HAVE_MALLOC_MALLOC_H
#include <malloc/malloc.h>
#endif

/* Google profiling tools */

#if HAVE_GPERFTOOLS_HEAP_PROFILER_H
static lispval gperf_heap_profile(lispval arg)
{
  int running = IsHeapProfilerRunning();
  if (FALSEP(arg)) {
    if (running) {
      HeapProfilerStop();
    return FD_TRUE;}
    else return FD_FALSE;}
  else if (running) return FD_FALSE;
  else if (STRINGP(arg)) {
    HeapProfilerStart(CSTRING(arg));
    return FD_TRUE;}
  else {
    HeapProfilerStart(u8_appid());
    return FD_TRUE;}
}

static lispval gperf_profiling_heap(lispval arg)
{
  if (IsHeapProfilerRunning())
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval gperf_dump_heap(lispval arg)
{
  int running = IsHeapProfilerRunning();
  if (running) {
    HeapProfilerDump(CSTRING(arg));
    return FD_TRUE;}
  else return FD_FALSE;
}
#endif

#if HAVE_GPERFTOOLS_PROFILER_H
static lispval gperf_startstop(lispval arg)
{
  if (STRINGP(arg))
    ProfilerStart(CSTRING(arg));
  else ProfilerStop();
  return VOID;
}
static lispval gperf_flush(lispval arg)
{
  ProfilerFlush();
  return VOID;
}
#endif

static lispval malloc_stats_prim()
{
#if HAVE_MALLOC_STATS
  malloc_stats();
#else
  write(2,"No malloc_stats available\n",
        strlen("No malloc_stats available\n"));
#endif
  return VOID;
}

static lispval release_memory_prim(lispval arg)
{
#if HAVE_GPERFTOOLS_MALLOC_EXTENSION_C_H
  if (FIXNUMP(arg))
    MallocExtension_ReleaseToSystem(FIX2INT(arg));
  else if (VOIDP(arg))
    MallocExtension_ReleaseFreeMemory();
  else return fd_type_error("fixnum","release_memory_prim",arg);
  return FD_TRUE;
#else
  return FD_FALSE;
#endif
}

static lispval mallocd_sensor()
{
  size_t in_use=0, heap_size=0;
  MallocExtension_GetNumericProperty("generic.current_allocated_bytes",&in_use);
  return FD_INT(in_use);
}

static lispval heapsize_sensor()
{
  size_t heap_size=0;
  MallocExtension_GetNumericProperty("generic.heap_size",&heap_size);
  /* Other properties: */
  /* "tcmalloc.current_total_thread_cache_bytes" */
  /* "tcmalloc.pageheap_free_bytes" */
  /* "tcmalloc.pageheap_unmapped_bytes" */
  return FD_INT(heap_size);
}

static lispval mallocinfo_sensor()
{
  char buf[1024];
  MallocExtension_GetStats(buf,1024);
  return fd_make_string(NULL,-1,buf);
}

FD_EXPORT int fd_init_gperftools(void) FD_LIBINIT_FN;

static long long int gperftools_init = 0;

FD_EXPORT int fd_init_gperftools()
{
  if (gperftools_init) return 0;
  gperftools_init = u8_millitime();
  lispval gperftools_module = fd_new_cmodule("GPERFTOOLS",0,fd_init_gperftools);

  fd_add_sensor(fd_intern("MALLOCD"),mallocd_sensor());
  fd_add_sensor(fd_intern("HEAPSIZE"),heapsize_sensor());
  fd_add_sensor(fd_intern("MALLOCINFO"),mallocinfo_sensor());

#if HAVE_GPERFTOOLS_HEAP_PROFILER_H
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1("GPERF/HEAP/PROFILE!",gperf_heap_profile,0));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim0("GPERF/HEAP?",gperf_profiling_heap));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1("GPERF/DUMPHEAP",gperf_dump_heap,1));
#endif

#if HAVE_GPERFTOOLS_PROFILER_H
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1("GPERF/PROFILE!",gperf_startstop,0));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim1("GPERF/FLUSH",gperf_flush,1));
#endif

  fd_idefn1(fd_scheme_module,"RELEASE-MEMORY",release_memory_prim,0,
            "Releases memory back to the operating system",
            fd_fixnum_type,VOID);

  fd_idefn0(fd_scheme_module,"MALLOC-STATS",malloc_stats_prim,
            "Returns a string report of memory usage");

  fd_finish_module(gperftools_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
