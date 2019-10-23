/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/config.h"

#if HAVE_GPERFTOOLS_PROFILER_H
#include <gperftools/profiler.h>
#endif

#if HAVE_GPERFTOOLS_HEAP_PROFILER_H
#include <gperftools/heap-profiler.h>
#endif

#if HAVE_GPERFTOOLS_MALLOC_EXTENSION_C_H
#include <gperftools/malloc_extension_c.h>
#endif

#define KNO_SOURCE 1

#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/cprims.h"

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
DEFPRIM("GPERF/HEAP/PROFILE!",gperf_heap_profile,MAX_ARGS(1)|MIN_ARGS(0),
	"Activates the gperftools heap profiler");
static lispval gperf_heap_profile(lispval arg)
{
  int running = IsHeapProfilerRunning();
  if (FALSEP(arg)) {
    if (running) {
      HeapProfilerStop();
    return KNO_TRUE;}
    else return KNO_FALSE;}
  else if (running) return KNO_FALSE;
  else if (STRINGP(arg)) {
    HeapProfilerStart(CSTRING(arg));
    return KNO_TRUE;}
  else {
    HeapProfilerStart(u8_appid());
    return KNO_TRUE;}
}

DEFPRIM("GPERF/HEAP/PROFILING?",gperf_profiling_heap,MAX_ARGS(0)|MIN_ARGS(0),
	"`(GPERF/HEAP/PROFILING?)` Returns true if the gperftools heap "
	"profiler is running.");
static lispval gperf_profiling_heap()
{
  if (IsHeapProfilerRunning())
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM("GPERF/HEAP/DUMP!",gperf_dump_heap,MAX_ARGS(1)|MIN_ARGS(1),
	"`(GPERF/HEAP/DUMP! *file*)` dumps the current heap information to"
	"*file*, returning true. Returns false if the heap profiler is not "
	"running.");
static lispval gperf_dump_heap(lispval arg)
{
  int running = IsHeapProfilerRunning();
  if (running) {
    HeapProfilerDump(CSTRING(arg));
    return KNO_TRUE;}
  else return KNO_FALSE;
}
#endif

#if HAVE_GPERFTOOLS_PROFILER_H
DEFPRIM("GPERF/CPU/PROFILE!",gperf_startstop,MAX_ARGS(1)|MIN_ARGS(1),
	"`(GPERF/CPU/PROFILE! *file*)` starts CPU profiling to *file*, or "
	"stops CPU profiling if *file* is not provided.");
static lispval gperf_startstop(lispval arg)
{
  if (STRINGP(arg))
    ProfilerStart(CSTRING(arg));
  else ProfilerStop();
  return VOID;
}
DEFPRIM("GPERF/CPU/FLUSH!",gperf_flush,MAX_ARGS(1)|MIN_ARGS(1),
	"`(GPERF/CPU/FLUSH!)` flushes CPU profiling records to the "
	"designated file.");
static lispval gperf_flush()
{
  ProfilerFlush();
  return VOID;
}
#endif

DEFPRIM("GPERF/MALLOC/STATS!",malloc_stats_prim,MAX_ARGS(0)|MIN_ARGS(0),
	"`(GPERF/MALLOC/STATS)` writes malloc statistics to the stdout.");
static lispval malloc_stats_prim()
{
#if HAVE_MALLOC_STATS
  malloc_stats();
#else
  int rv = write(2,"No malloc_stats available\n",
                 strlen("No malloc_stats available\n"));
#endif
  return VOID;
}

DEFPRIM("GPERF/MALLOC/RELEASE!",release_memory_prim,MAX_ARGS(1)|MIN_ARGS(0),
	"`(GPERF/MALLOC/RELEASE!)` attemps to release memory back to the "
	"operating system.");
static lispval release_memory_prim(lispval arg)
{
#if HAVE_GPERFTOOLS_MALLOC_EXTENSION_C_H
  if (FIXNUMP(arg))
    MallocExtension_ReleaseToSystem(FIX2INT(arg));
  else if (VOIDP(arg))
    MallocExtension_ReleaseFreeMemory();
  else return kno_type_error("fixnum","release_memory_prim",arg);
  return KNO_TRUE;
#else
  return KNO_FALSE;
#endif
}

static lispval mallocd_sensor()
{
  size_t in_use=0, heap_size=0;
  MallocExtension_GetNumericProperty("generic.current_allocated_bytes",&in_use);
  return KNO_INT(in_use);
}

static lispval heapsize_sensor()
{
  size_t heap_size=0;
  MallocExtension_GetNumericProperty("generic.heap_size",&heap_size);
  /* Other properties: */
  /* "tcmalloc.current_total_thread_cache_bytes" */
  /* "tcmalloc.pageheap_free_bytes" */
  /* "tcmalloc.pageheap_unmapped_bytes" */
  return KNO_INT(heap_size);
}

static lispval mallocinfo_sensor()
{
  char buf[1024];
  MallocExtension_GetStats(buf,1024);
  return kno_make_string(NULL,-1,buf);
}

KNO_EXPORT int kno_init_gperftools(void) KNO_LIBINIT_FN;

static long long int gperftools_init = 0;

KNO_EXPORT int kno_init_gperftools()
{
  if (gperftools_init) return 0;
  gperftools_init = u8_millitime();
  lispval module = kno_new_cmodule("gperftools",0,kno_init_gperftools);

  kno_add_sensor(kno_intern("mallocd"),mallocd_sensor);
  kno_add_sensor(kno_intern("heapsize"),heapsize_sensor);
  kno_add_sensor(kno_intern("mallocinfo"),mallocinfo_sensor);

#if HAVE_GPERFTOOLS_HEAP_PROFILER_H
  KNO_LINK_CPRIM(gperf_heap_profile,1,module);
  KNO_LINK_CPRIM(gperf_profiling_heap,0,module);
  KNO_LINK_CPRIM(gperf_dump_heap,1,module);
#endif

#if HAVE_GPERFTOOLS_PROFILER_H
  KNO_LINK_CPRIM(gperf_startstop,1,module);
  KNO_LINK_CPRIM(gperf_flush,0,module);
#endif

  KNO_LINK_CPRIM(release_memory_prim,1,module);
  KNO_LINK_CPRIM(malloc_stats_prim,0,module);

  kno_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

