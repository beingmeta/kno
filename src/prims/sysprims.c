/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define FD_PROVIDE_FASTEVAL 1 */

#include "framerd/fdsource.h"

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

#if ((HAVE_SYS_UTSNAME_H)&&(HAVE_UNAME))
#include <sys/utsname.h>
#endif

fd_exception fd_MissingFeature=_("OS doesn't support operation");

/* Getting the current hostname */

static fdtype hostname_prim()
{
  return fd_lispstring(u8_gethostname());
}

/* There's not a good justification for putting this here other
   than that it has to do with getting stuff from the environment. */
static fdtype hostaddrs_prim(fdtype hostname)
{
  int addr_len = -1; unsigned int type = -1;
  char **addrs = u8_lookup_host(FD_STRDATA(hostname),&addr_len,&type);
  fdtype results = FD_EMPTY_CHOICE;
  int i = 0;
  if (addrs == NULL) {
    fd_clear_errors(1);
    return results;}
  else while (addrs[i]) {
    unsigned char *addr = addrs[i++]; fdtype string;
    struct U8_OUTPUT out; int j = 0; U8_INIT_OUTPUT(&out,16);
    while (j<addr_len) {
      u8_printf(&out,((j>0)?(".%d"):("%d")),(int)addr[j]);
      j++;}
    string = fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    FD_ADD_TO_CHOICE(results,string);}
  u8_free(addrs);
  return results;
}

/* GETENV primitive */

static fdtype getenv_prim(fdtype var)
{
  u8_string enval = u8_getenv(FD_STRDATA(var));
  if (enval == NULL) return FD_FALSE;
  else return fd_lispstring(enval);
}

/* LOAD AVERAGE */

static fdtype loadavg_prim()
{
  double loadavg;
  int nsamples = getloadavg(&loadavg,1);
  if (nsamples==1) return fd_make_flonum(loadavg);
  else return FD_FALSE;
}

static fdtype loadavgs_prim()
{
  double loadavg[3]; int nsamples = getloadavg(loadavg,3);
  if (nsamples==1)
    return fd_make_nvector(1,fd_make_flonum(loadavg[0]));
  else if (nsamples==2)
    return fd_make_nvector
      (2,fd_make_flonum(loadavg[0]),fd_make_flonum(loadavg[1]));
  else if (nsamples==3)
    return fd_make_nvector
      (3,fd_make_flonum(loadavg[0]),fd_make_flonum(loadavg[1]),
       fd_make_flonum(loadavg[2]));
  else return FD_FALSE;
}

/* RUSAGE */

static fdtype data_symbol, stack_symbol, shared_symbol, private_symbol;
static fdtype memusage_symbol, vmemusage_symbol, pagesize_symbol, rss_symbol;
static fdtype datakb_symbol, stackkb_symbol, sharedkb_symbol;
static fdtype rsskb_symbol, privatekb_symbol;
static fdtype utime_symbol, stime_symbol, clock_symbol;
static fdtype load_symbol, loadavg_symbol, pid_symbol, ppid_symbol;
static fdtype memusage_symbol, vmemusage_symbol, pagesize_symbol;
static fdtype n_cpus_symbol, max_cpus_symbol;
static fdtype physical_pages_symbol, available_pages_symbol;
static fdtype physical_memory_symbol, available_memory_symbol;
static fdtype physicalmb_symbol, availablemb_symbol;
static fdtype memload_symbol, vmemload_symbol, stacksize_symbol;
static fdtype nptrlocks_symbol, cpusage_symbol, tcpusage_symbol;
static fdtype mallocd_symbol, heap_symbol, mallocinfo_symbol;
static fdtype tcmallocinfo_symbol;

static int pagesize = -1;
static int get_n_cpus(void);
static int get_max_cpus(void);
static int get_pagesize(void);
static int get_physical_pages(void);
static int get_available_pages(void);
static long long get_physical_memory(void);
static long long get_available_memory(void);

/* We store the usage at startup because exec() calls will sometimes
   include time values running before exec() was called and we usually
   want only user and system time *after* exec was called. */
static struct rusage init_rusage;

static void add_intval(fdtype table,fdtype symbol,long long ival)
{
  fdtype iptr = FD_INT(ival);
  fd_add(table,symbol,iptr);
  if (FD_CONSP(iptr)) fd_decref(iptr);
}

static void add_flonum(fdtype table,fdtype symbol,double fval)
{
  fdtype flonum = fd_make_flonum(fval);
  fd_add(table,symbol,flonum);
  fd_decref(flonum);
}

static u8_string get_malloc_info()
{
  char *buf=NULL;
  size_t buflen=0;
  FILE *out;
  out=open_memstream(&buf,&buflen);
  malloc_info(0,out);
  fclose(out);
  return buf;
}

static fdtype rusage_prim(fdtype field)
{
  struct rusage r;
#if HAVE_MALLINFO
  struct mallinfo meminfo;
#endif
  int pagesize = get_pagesize();
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return FD_ERROR_VALUE;
  else if (FD_VOIDP(field)) {
    fdtype result = fd_empty_slotmap();
    pid_t pid = getpid(), ppid = getppid();
    ssize_t mem = u8_memusage(), vmem = u8_vmemusage();
    double memload = u8_memload(), vmemload = u8_vmemload();
    size_t n_cpus = get_n_cpus();
#if HAVE_GPERFTOOLS_MALLOC_EXTENSION_C_H
    /* This doesn't seem to work, but it should */
    {
      size_t in_use=0, heap_size=0;
      MallocExtension_GetNumericProperty
        ("generic.current_allocated_bytes",&in_use);
      MallocExtension_GetNumericProperty
        ("generic.heap_size",&heap_size);
      /* Other properties: */
      /* "tcmalloc.current_total_thread_cache_bytes" */
      /* "tcmalloc.pageheap_free_bytes" */
      /* "tcmalloc.pageheap_unmapped_bytes" */
      add_intval(result,mallocd_symbol,in_use);
      add_intval(result,heap_symbol,heap_size);}
#elif HAVE_MALLINFO
    if (sizeof(meminfo.arena)>=8) {
      meminfo=mallinfo();
      add_intval(result,mallocd_symbol,meminfo.arena);}
#endif

    add_intval(result,data_symbol,r.ru_idrss);
    add_intval(result,stack_symbol,r.ru_isrss);
    add_intval(result,shared_symbol,r.ru_ixrss);
    add_intval(result,rss_symbol,r.ru_maxrss);
    add_intval(result,datakb_symbol,((r.ru_idrss*pagesize)/1024));
    add_intval(result,stackkb_symbol,((r.ru_isrss*pagesize)/1024));
    add_intval(result,privatekb_symbol,
               (((r.ru_idrss+r.ru_isrss)*pagesize)/1024));
    add_intval(result,sharedkb_symbol,((r.ru_ixrss*pagesize)/1024));
    add_intval(result,rsskb_symbol,((r.ru_maxrss*pagesize)/1024));
    add_intval(result,memusage_symbol,mem);
    add_intval(result,vmemusage_symbol,vmem);
    add_flonum(result,memload_symbol,memload);
    add_flonum(result,vmemload_symbol,vmemload);
    add_intval(result,stacksize_symbol,u8_stack_size);
    add_intval(result,nptrlocks_symbol,FD_N_PTRLOCKS);
    add_intval(result,pid_symbol,pid);
    add_intval(result,ppid_symbol,ppid);
    { /* Load average(s) */
      double loadavg[3]; int nsamples = getloadavg(loadavg,3);
      if (nsamples>0) {
        fdtype lval = fd_make_flonum(loadavg[0]), lvec = FD_VOID;
        fd_store(result,load_symbol,lval);
        if (nsamples==1)
          lvec = fd_make_nvector(1,fd_make_flonum(loadavg[0]));
        else if (nsamples==2)
          lvec = fd_make_nvector(2,fd_make_flonum(loadavg[0]),
                               fd_make_flonum(loadavg[1]));
        else lvec = fd_make_nvector
               (3,fd_make_flonum(loadavg[0]),
                fd_make_flonum(loadavg[1]),
                fd_make_flonum(loadavg[2]));
        if (!(FD_VOIDP(lvec))) fd_store(result,loadavg_symbol,lvec);
        fd_decref(lval); fd_decref(lvec);}}
    { /* Elapsed time */
      double elapsed = u8_elapsed_time();
      double usecs = elapsed*1000000.0;
      double utime = u8_dbldifftime(r.ru_utime,init_rusage.ru_utime);
      double stime = u8_dbldifftime(r.ru_stime,init_rusage.ru_stime);
      double cpusage = ((utime+stime)*100)/usecs;
      double tcpusage = cpusage/n_cpus;
      add_flonum(result,clock_symbol,elapsed);
      add_flonum(result,cpusage_symbol,cpusage);
      add_flonum(result,tcpusage_symbol,tcpusage);}

    add_flonum(result,utime_symbol,
               (u8_dbldifftime(r.ru_utime,init_rusage.ru_utime))/1000000);
    add_flonum(result,stime_symbol,
               (u8_dbldifftime(r.ru_stime,init_rusage.ru_stime))/1000000);

    { /* SYSCONF information */
      int n_cpus = get_n_cpus(), max_cpus = get_max_cpus();
      int pagesize = get_pagesize();
      int physical_pages = get_physical_pages();
      int available_pages = get_available_pages();
      long long physical_memory = get_physical_memory();
      long long available_memory = get_available_memory();
      fd_add(result,n_cpus_symbol,FD_INT(n_cpus));
      fd_add(result,max_cpus_symbol,FD_INT(max_cpus));
      if (pagesize>0) fd_add(result,pagesize_symbol,FD_INT(pagesize));
      if (physical_pages>0)
        add_intval(result,physical_pages_symbol,physical_pages);
      if (available_pages>0)
        add_intval(result,available_pages_symbol,available_pages);
      if (physical_memory>0) {
        add_intval(result,physicalmb_symbol,physical_memory/(1024*1024));
        add_intval(result,physical_memory_symbol,physical_memory);}
      if (available_memory>0) {
        add_intval(result,availablemb_symbol,available_memory/(1024*1024));
        add_intval(result,available_memory_symbol,available_memory);}}

    return result;}
  else if (FD_EQ(field,cpusage_symbol)) {
    double elapsed = u8_elapsed_time()*1000000.0;
    double stime = u8_dbltime(r.ru_stime);
    double utime = u8_dbltime(r.ru_utime);
    double cpusage = (stime+utime)*100.0/elapsed;
    return fd_init_double(NULL,cpusage);}
  else if (FD_EQ(field,data_symbol))
    return FD_INT((r.ru_idrss*pagesize));
  else if (FD_EQ(field,clock_symbol))
    return fd_make_flonum(u8_elapsed_time());
  else if (FD_EQ(field,stack_symbol))
    return FD_INT((r.ru_isrss*pagesize));
  else if (FD_EQ(field,private_symbol))
    return FD_INT((r.ru_idrss+r.ru_isrss)*pagesize);
  else if (FD_EQ(field,shared_symbol))
    return FD_INT((r.ru_ixrss*pagesize));
  else if (FD_EQ(field,rss_symbol))
    return FD_INT((r.ru_maxrss*pagesize));
  else if (FD_EQ(field,datakb_symbol))
    return FD_INT((r.ru_idrss*pagesize)/1024);
  else if (FD_EQ(field,stackkb_symbol))
    return FD_INT((r.ru_isrss*pagesize)/1024);
  else if (FD_EQ(field,stacksize_symbol))
    return FD_INT(u8_stack_size);
  else if (FD_EQ(field,privatekb_symbol))
    return FD_INT(((r.ru_idrss+r.ru_isrss)*pagesize)/1024);
  else if (FD_EQ(field,sharedkb_symbol))
    return FD_INT((r.ru_ixrss*pagesize)/1024);
  else if (FD_EQ(field,rsskb_symbol))
    return FD_INT((r.ru_maxrss*pagesize)/1024);
  else if (FD_EQ(field,utime_symbol))
    return fd_make_flonum(u8_dbltime(r.ru_utime));
  else if (FD_EQ(field,stime_symbol))
    return fd_make_flonum(u8_dbltime(r.ru_stime));
  else if (FD_EQ(field,memusage_symbol))
    return FD_INT(u8_memusage());
  else if (FD_EQ(field,vmemusage_symbol))
    return FD_INT(u8_vmemusage());
  else if (FD_EQ(field,nptrlocks_symbol))
    return FD_INT(FD_N_PTRLOCKS);
  else if (FD_EQ(field,mallocinfo_symbol)) {
    u8_string info=get_malloc_info();
    if (info)
      return fd_init_string(NULL,-1,info);
    else return FD_EMPTY_CHOICE;}
#if HAVE_GPERFTOOLS_MALLOC_EXTENSION_C_H
  else if (FD_EQ(field,tcmallocinfo_symbol)) {
    char buf[1024];
    MallocExtension_GetStats(buf,1024);
    return fd_make_string(NULL,-1,buf);}
#endif
  else if (FD_EQ(field,load_symbol)) {
    double loadavg; int nsamples = getloadavg(&loadavg,1);
    if (nsamples>0) return fd_make_flonum(loadavg);
    else return FD_EMPTY_CHOICE;}
  else if (FD_EQ(field,loadavg_symbol)) {
    double loadavg[3]; int nsamples = getloadavg(loadavg,3);
    if (nsamples>0) {
      if (nsamples==1)
        return fd_make_nvector(1,fd_make_flonum(loadavg[0]));
      else if (nsamples==2)
        return fd_make_nvector(2,fd_make_flonum(loadavg[0]),
                               fd_make_flonum(loadavg[1]));
      else return fd_make_nvector
             (3,fd_make_flonum(loadavg[0]),fd_make_flonum(loadavg[1]),
              fd_make_flonum(loadavg[2]));}
    else if (FD_EQ(field,pid_symbol))
      return FD_INT((unsigned long)(getpid()));
    else if (FD_EQ(field,ppid_symbol))
      return FD_INT((unsigned long)(getppid()));
    else return FD_EMPTY_CHOICE;}
  else if (FD_EQ(field,n_cpus_symbol)) {
    int n_cpus = get_n_cpus();
    if (n_cpus>0) return FD_INT(n_cpus);
    else if (n_cpus==0) return FD_EMPTY_CHOICE;
    else {
      u8_graberr(-1,"rusage_prim/N_CPUS",NULL);
      return FD_ERROR_VALUE;}}
  else if (FD_EQ(field,max_cpus_symbol)) {
    int max_cpus = get_max_cpus();
    if (max_cpus>0) return FD_INT(max_cpus);
    else if (max_cpus==0) return FD_EMPTY_CHOICE;
    else {
      u8_graberr(-1,"rusage_prim/MAX_CPUS",NULL);
      return FD_ERROR_VALUE;}}
  else if (FD_EQ(field,pagesize_symbol)) {
    int pagesize = get_pagesize();
    if (pagesize>0) return FD_INT(pagesize);
    else if (pagesize==0) return FD_EMPTY_CHOICE;
    else {
      u8_graberr(-1,"rusage_prim/PAGESIZE",NULL);
      return FD_ERROR_VALUE;}}
  else if (FD_EQ(field,physical_pages_symbol)) {
    int physical_pages = get_physical_pages();
    if (physical_pages>0) return FD_INT(physical_pages);
    else if (physical_pages==0) return FD_EMPTY_CHOICE;
    else {
      u8_graberr(-1,"rusage_prim/PHYSICAL_PAGES",NULL);
      return FD_ERROR_VALUE;}}
  else if (FD_EQ(field,available_pages_symbol)) {
    int available_pages = get_available_pages();
    if (available_pages>0) return FD_INT(available_pages);
    else if (available_pages==0) return FD_EMPTY_CHOICE;
    else {
      u8_graberr(-1,"rusage_prim/AVAILABLE_PAGES",NULL);
      return FD_ERROR_VALUE;}}
  else if (FD_EQ(field,physical_memory_symbol)) {
    long long physical_memory = get_physical_memory();
    if (physical_memory>0) return FD_INT(physical_memory);
    else if (physical_memory==0) return FD_EMPTY_CHOICE;
    else {
      u8_graberr(-1,"rusage_prim/PHYSICAL_MEMORY",NULL);
      return FD_ERROR_VALUE;}}
  else if (FD_EQ(field,physicalmb_symbol)) {
    long long physical_memory = get_physical_memory();
    if (physical_memory>0) return FD_INT(physical_memory/(1024*1024));
    else if (physical_memory==0) return FD_EMPTY_CHOICE;
    else {
      u8_graberr(-1,"rusage_prim/PHYSICAL_MEMORY",NULL);
      return FD_ERROR_VALUE;}}
  else if (FD_EQ(field,available_memory_symbol)) {
    long long available_memory = get_available_memory();
    if (available_memory>0) return FD_INT(available_memory);
    else if (available_memory==0) return FD_EMPTY_CHOICE;
    else {
      u8_graberr(-1,"rusage_prim/AVAILABLE_MEMORY",NULL);
      return FD_ERROR_VALUE;}}
  else if (FD_EQ(field,availablemb_symbol)) {
    long long available_memory = get_available_memory();
    if (available_memory>0) return FD_INT(available_memory/(1024*1024));
    else if (available_memory==0) return FD_EMPTY_CHOICE;
    else {
      u8_graberr(-1,"rusage_prim/AVAILABLE_MEMORY",NULL);
      return FD_ERROR_VALUE;}}
  else return FD_EMPTY_CHOICE;
}

static int setprop(fdtype result,u8_string field,char *value)
{
  if ((value)&&(strcmp(value,"(none)"))) {
    fdtype slotid = fd_intern(field);
    u8_string svalue = u8_fromlibc(value);
    fdtype lvalue = fdstring(svalue);
    int rv = fd_store(result,slotid,lvalue);
    fd_decref(lvalue);
    u8_free(svalue);
    return rv;}
  else return 0;
}

static fdtype uname_prim()
{
#if ((HAVE_SYS_UTSNAME_H)&&(HAVE_UNAME))
  struct utsname sysinfo;
  int rv = uname(&sysinfo);
  if (rv==0) {
    fdtype result = fd_init_slotmap(NULL,0,NULL);
    setprop(result,"OSNAME",sysinfo.sysname);
    setprop(result,"NODENAME",sysinfo.nodename);
    setprop(result,"RELEASE",sysinfo.release);
    setprop(result,"VERSION",sysinfo.version);
    setprop(result,"MACHINE",sysinfo.machine);
    return result;}
  else {
    u8_graberr(errno,"uname_prim",NULL);
    return FD_ERROR_VALUE;}
#else
  return fd_init_slotmap(NULL,0,NULL);
#endif
}

static fdtype getpid_prim()
{
  pid_t pid = getpid();
  return FD_INT(((unsigned long)pid));
}
static fdtype getppid_prim()
{
  pid_t pid = getppid();
  return FD_INT(((unsigned long)pid));
}

static fdtype stacksize_prim()
{
  ssize_t size = u8_stacksize();
  if (size<0)
    return FD_ERROR_VALUE;
  else return FD_INT(size);
}

static fdtype threadid_prim()
{
  long long tid = u8_threadid();
  return FD_INT(tid);
}

static fdtype getprocstring_prim()
{
  unsigned char buf[128];
  unsigned char *pinfo = u8_procinfo(buf);
  return fdtype_string(pinfo);
}

static fdtype memusage_prim()
{
  ssize_t size = u8_memusage();
  return FD_INT(size);
}

static fdtype vmemusage_prim()
{
  ssize_t size = u8_vmemusage();
  return FD_INT(size);
}

static fdtype physmem_prim(fdtype total)
{
  ssize_t size = u8_physmem();
  return FD_INT(size);
}

static fdtype memload_prim()
{
  double load = u8_memload();
  return fd_make_flonum(load);
}

static fdtype vmemload_prim()
{
  double vload = u8_vmemload();
  return fd_make_flonum(vload);
}

static fdtype usertime_prim()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return FD_ERROR_VALUE;
  else {
    double msecs=
      (r.ru_utime.tv_sec*1000000.0+r.ru_utime.tv_usec*1.0)-
      (init_rusage.ru_utime.tv_sec*1000000.0+
       init_rusage.ru_utime.tv_usec*1.0);
    return fd_init_double(NULL,msecs);}
}

static fdtype systime_prim()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return FD_ERROR_VALUE;
  else {
    double msecs=
      (r.ru_stime.tv_sec*1000000.0+r.ru_stime.tv_usec*1.0)-
      (init_rusage.ru_stime.tv_sec*1000000.0+
       init_rusage.ru_stime.tv_usec*1.0);
    return fd_init_double(NULL,msecs);}
}

static fdtype cpusage_prim(fdtype arg)
{
  if (FD_VOIDP(arg))
    return rusage_prim(cpusage_symbol);
  else {
    struct rusage r;
    memset(&r,0,sizeof(r));
    if (u8_getrusage(RUSAGE_SELF,&r)<0)
      return FD_ERROR_VALUE;
    else {
      fdtype prelapsed = fd_get(arg,clock_symbol,FD_VOID);
      fdtype prestime = fd_get(arg,stime_symbol,FD_VOID);
      fdtype preutime = fd_get(arg,utime_symbol,FD_VOID);
      if ((FD_FLONUMP(prelapsed)) &&
          (FD_FLONUMP(prestime)) &&
          (FD_FLONUMP(preutime))) {
        double elapsed=
          (u8_elapsed_time()-FD_FLONUM(prelapsed))*1000000.0;
        double stime = (u8_dbltime(r.ru_stime)-FD_FLONUM(prestime));
        double utime = u8_dbltime(r.ru_utime)-FD_FLONUM(preutime);
        double cpusage = (stime+utime)*100.0/elapsed;
        return fd_init_double(NULL,cpusage);}
      else return fd_type_error(_("rusage"),"getcpusage",arg);}}
}

static int get_max_cpus()
{
  int retval = 0;
#if ((HAVE_SYSCONF)&&(defined(_SC_NPROCESSORS_CONF)))
  retval = sysconf(_SC_NPROCESSORS_CONF);
  if (retval>0) return retval;
  if (retval<0) fd_clear_errors(1);
  return 1;
#else
  return 1;
#endif
}

static int get_n_cpus()
{
  int retval = 0;
#if ((HAVE_SYSCONF)&&(defined(_SC_NPROCESSORS_ONLN)))
  retval = sysconf(_SC_NPROCESSORS_ONLN);
  if (retval>0) return retval;
  if (retval<0) fd_clear_errors(1);
  return 1;
#elif (HAVE_SYSCONF)
  return get_max_cpus();
#else
  return 1;
#endif
}

static int get_pagesize()
{
  int retval = 0;
  if (pagesize>=0) return pagesize;
#if (HAVE_SYSCONF)
#if (defined(_SC_PAGESIZE))
  retval = sysconf(_SC_PAGESIZE);
#elif (defined(_SC_PAGE_SIZE))
  retval = sysconf(_SC_PAGE_SIZE);
#else
  retval = 0;
#endif
#endif
  pagesize = retval;
  if (retval>0) return retval;
  if (retval<0) fd_clear_errors(1);
  return 0;
}

static int get_physical_pages()
{
#if ((HAVE_SYSCONF)&&(defined(_SC_PHYS_PAGES)))
  int retval = sysconf(_SC_PHYS_PAGES);
  if (retval>0) return retval;
  if (retval<0) fd_clear_errors(1);
#endif
  return 0;
}

static int get_available_pages()
{
#if ((HAVE_SYSCONF)&&(defined(_SC_AVPHYS_PAGES)))
  int retval = sysconf(_SC_AVPHYS_PAGES);
  if (retval>0) return retval;
  if (retval<0) fd_clear_errors(1);
#endif
  return 0;
}

static long long get_physical_memory()
{
  long long retval = 0;
  if (pagesize<0) pagesize = get_pagesize();
  if (pagesize==0) return 0;
#if ((HAVE_SYSCONF)&&(defined(_SC_PHYS_PAGES)))
  retval = sysconf(_SC_PHYS_PAGES);
  if (retval>0) return retval*pagesize;
  if (retval<0) fd_clear_errors(1);
#endif
  return 0;
}

static long long get_available_memory()
{
  long long retval = 0;
  if (pagesize<0) pagesize = get_pagesize();
  if (pagesize==0) return 0;
#if ((HAVE_SYSCONF)&&(defined(_SC_AVPHYS_PAGES)))
  retval = sysconf(_SC_AVPHYS_PAGES);
  if (retval>0) return retval*pagesize;
  if (retval<0) fd_clear_errors(1);
#endif
  return retval;
}

/* CALLTRACK SENSORS */

/* See src/dtype/apply.c for a description of calltrack, which is a
   profiling utility for higher level programs.
   These functions allow the tracking of various RUSAGE fields
   over program execution.
*/

#if FD_CALLTRACK_ENABLED
static double utime_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return 0.0;
  else return r.ru_utime.tv_sec*1000000.0+r.ru_utime.tv_usec*1.0;
}
static double stime_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return 0.0;
  else return r.ru_stime.tv_sec*1000000.0+r.ru_stime.tv_usec*1.0;
}
static long memusage_sensor()
{
  ssize_t usage = u8_memusage();
  return (long)usage;
}
static long vmemusage_sensor()
{
  ssize_t usage = u8_vmemusage();
  return (long)usage;
}
#if HAVE_STRUCT_RUSAGE_RU_INBLOCK
static long inblock_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return 0;
  else return r.ru_inblock;
}
static long outblock_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return 0;
  else return r.ru_oublock;
}
#endif
#if HAVE_STRUCT_RUSAGE_RU_MAJFLT
static long majflt_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return 0;
  else return r.ru_majflt;
}
static long nswaps_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return 0;
  else return r.ru_nswap;
}
#endif
#if HAVE_STRUCT_RUSAGE_RU_NVCSW
static long cxtswitch_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return 0;
  else return r.ru_nvcsw+r.ru_nivcsw;
}
static long vcxtswitch_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return 0;
  else return r.ru_nvcsw;
}
static long ivcxtswitch_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return 0;
  else return r.ru_nivcsw;
}
#endif
#endif

/* CALLTRACK INTERFACE */

/* This is the Scheme API for accessing CALLTRACK */

static fdtype calltrack_sensors()
{
#if FD_CALLTRACK_ENABLED
  return fd_calltrack_sensors();
#else
  return fd_init_vector(NULL,0,NULL);
#endif
}

static fdtype calltrack_sense(fdtype all)
{
#if FD_CALLTRACK_ENABLED
  if (FD_FALSEP(all))
    return fd_calltrack_sense(0);
  else return fd_calltrack_sense(1);
#else
  return fd_init_vector(NULL,0,NULL);
#endif
}

/* Corelimit config variable */

static fdtype corelimit_get(fdtype symbol,void *vptr)
{
  struct rlimit limit;
  int rv = getrlimit(RLIMIT_CORE,&limit);
  if (rv<0) {
    u8_graberr(errno,"corelimit_get",NULL);
    return FD_ERROR_VALUE;}
  else return FD_INT(limit.rlim_cur);
}

static int corelimit_set(fdtype symbol,fdtype value,void *vptr)
{
  struct rlimit limit; int rv;
  if (FD_FIXNUMP(value))
    limit.rlim_cur = limit.rlim_max = FD_FIX2INT(value);
  else if (FD_TRUEP(value))
    limit.rlim_cur = limit.rlim_max = RLIM_INFINITY;
  else if (FD_TYPEP(value,fd_bigint_type))
    limit.rlim_cur = limit.rlim_max = fd_bigint_to_long_long
      ((struct FD_BIGINT *)(value));
  else {
    fd_seterr(fd_TypeError,"corelimit",NULL,value);
    return -1;}
  rv = setrlimit(RLIMIT_CORE,&limit);
  if (rv<0) return rv;
  else return 1;
}

/* Google profiling tools */

#if HAVE_GPERFTOOLS_HEAP_PROFILER_H
static fdtype gperf_heap_profile(fdtype arg)
{
  int running = IsHeapProfilerRunning();
  if (FD_FALSEP(arg)) {
    if (running) {
      HeapProfilerStop();
    return FD_TRUE;}
    else return FD_FALSE;}
  else if (running) return FD_FALSE;
  else if (FD_STRINGP(arg)) {
    HeapProfilerStart(FD_STRDATA(arg));
    return FD_TRUE;}
  else {
    HeapProfilerStart(u8_appid());
    return FD_TRUE;}
}

static fdtype gperf_profiling_heap(fdtype arg)
{
  if (IsHeapProfilerRunning())
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype gperf_dump_heap(fdtype arg)
{
  int running = IsHeapProfilerRunning();
  if (running) {
    HeapProfilerDump(FD_STRDATA(arg));
    return FD_TRUE;}
  else return FD_FALSE;
}
#endif

#if HAVE_GPERFTOOLS_PROFILER_H
static fdtype gperf_startstop(fdtype arg)
{
  if (FD_STRINGP(arg))
    ProfilerStart(FD_STRDATA(arg));
  else ProfilerStop();
  return FD_VOID;
}
static fdtype gperf_flush(fdtype arg)
{
  ProfilerFlush();
  return FD_VOID;
}
#endif

static fdtype malloc_stats_prim()
{
  malloc_stats();
  return FD_VOID;
}

static fdtype release_memory_prim(fdtype arg)
{
#if HAVE_GPERFTOOLS_MALLOC_EXTENSION_C_H
  if (FD_FIXNUMP(arg))
    MallocExtension_ReleaseToSystem(FD_FIX2INT(arg));
  else if (FD_VOIDP(arg))
    MallocExtension_ReleaseFreeMemory();
  else return fd_type_error("fixnum","release_memory_prim",arg);
  return FD_TRUE;
#else
  return FD_FALSE;
#endif
}

/* Initialization */

FD_EXPORT void fd_init_sysprims_c()
{
  u8_register_source_file(_FILEINFO);

  u8_getrusage(RUSAGE_SELF,&init_rusage);

  data_symbol = fd_intern("DATA");
  datakb_symbol = fd_intern("DATAKB");
  stack_symbol = fd_intern("STACK");
  stackkb_symbol = fd_intern("STACKKB");
  shared_symbol = fd_intern("SHARED");
  sharedkb_symbol = fd_intern("SHAREDKB");
  private_symbol = fd_intern("PRIVATE");
  privatekb_symbol = fd_intern("PRIVATEKB");
  rss_symbol = fd_intern("RESIDENT");
  rsskb_symbol = fd_intern("RESIDENTKB");
  utime_symbol = fd_intern("UTIME");
  stime_symbol = fd_intern("STIME");
  clock_symbol = fd_intern("CLOCK");
  cpusage_symbol = fd_intern("CPUSAGE");
  pid_symbol = fd_intern("PID");
  ppid_symbol = fd_intern("PPID");
  memusage_symbol = fd_intern("MEMUSAGE");
  vmemusage_symbol = fd_intern("VMEMUSAGE");
  memload_symbol = fd_intern("MEMLOAD");
  vmemload_symbol = fd_intern("VMEMLOAD");
  stacksize_symbol = fd_intern("STACKSIZE");
  n_cpus_symbol = fd_intern("NCPUS");
  max_cpus_symbol = fd_intern("MAXCPUS");
  nptrlocks_symbol = fd_intern("NPTRLOCKS");
  pagesize_symbol = fd_intern("PAGESIZE");
  physical_pages_symbol = fd_intern("PHYSICAL-PAGES");
  available_pages_symbol = fd_intern("AVAILABLE-PAGES");
  physical_memory_symbol = fd_intern("PHYSICAL-MEMORY");
  available_memory_symbol = fd_intern("AVAILABLE-MEMORY");
  physicalmb_symbol = fd_intern("PHYSMB");
  availablemb_symbol = fd_intern("AVAILMB");
  cpusage_symbol = fd_intern("CPU%");
  tcpusage_symbol = fd_intern("CPU%/CPU");

  mallocd_symbol = fd_intern("MALLOCD");
  heap_symbol = fd_intern("HEAP");
  mallocinfo_symbol = fd_intern("MALLOCINFO");
  tcmallocinfo_symbol = fd_intern("TCMALLOCINFO");

  load_symbol = fd_intern("LOAD");
  loadavg_symbol = fd_intern("LOADAVG");

  fd_idefn(fd_scheme_module,fd_make_cprim0("GETHOSTNAME",hostname_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim1("HOSTADDRS",hostaddrs_prim,0));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("GETENV",getenv_prim,1,
                           fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim1("RUSAGE",rusage_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("MEMUSAGE",memusage_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("VMEMUSAGE",vmemusage_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PHYSMEM",physmem_prim,0));

  fd_idefn(fd_scheme_module,fd_make_cprim0("MEMLOAD",memload_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("VMEMLOAD",vmemload_prim));

  fd_idefn(fd_scheme_module,fd_make_cprim0("USERTIME",usertime_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("SYSTIME",systime_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CPUSAGE",cpusage_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("UNAME",uname_prim));

  fd_idefn(fd_scheme_module,fd_make_cprim0("GETLOAD",loadavg_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("LOADAVG",loadavgs_prim));

  fd_idefn(fd_scheme_module,fd_make_cprim0("GETPID",getpid_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("GETPPID",getppid_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("THREADID",threadid_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("STACKSIZE",stacksize_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("PROCSTRING",getprocstring_prim));

  fd_idefn0(fd_scheme_module,"MALLOC-STATS",malloc_stats_prim,
            "Returns a string report of memory usage");
  fd_idefn1(fd_scheme_module,"RELEASE-MEMORY",release_memory_prim,0,
            "Releases memory back to the operating system",
            fd_fixnum_type,FD_VOID);

  fd_idefn(fd_scheme_module,fd_make_cprim0("CT/SENSORS",calltrack_sensors));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CT/SENSE",calltrack_sense,0));

  fd_register_config
    ("CORELIMIT",_("Set core size limit"),
     corelimit_get,corelimit_set,NULL);

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


  /* Initialize utime and stime sensors */
#if FD_CALLTRACK_ENABLED
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("UTIME",1);
    cts->enabled = 0; cts->dblfcn = utime_sensor;}
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("STIME",1);
    cts->enabled = 0; cts->dblfcn = stime_sensor;}
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("MEMUSAGE",1);
    cts->enabled = 0; cts->intfcn = memusage_sensor;}
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("VMEMUSAGE",1);
    cts->enabled = 0; cts->intfcn = vmemusage_sensor;}
#if HAVE_STRUCT_RUSAGE_RU_INBLOCK
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("INBLOCK",1);
    cts->enabled = 0; cts->intfcn = inblock_sensor;}
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("OUTBLOCK",1);
    cts->enabled = 0; cts->intfcn = outblock_sensor;}
#endif
#if HAVE_STRUCT_RUSAGE_RU_MAJFLT
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("MAJFLT",1);
    cts->enabled = 0; cts->intfcn = majflt_sensor;}
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("NSWAPS",1);
    cts->enabled = 0; cts->intfcn = nswaps_sensor;}
#endif
#if HAVE_STRUCT_RUSAGE_RU_NVCSW
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("SWITCHES",1);
    cts->enabled = 0; cts->intfcn = cxtswitch_sensor;}
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("VSWITCHES",1);
    cts->enabled = 0; cts->intfcn = vcxtswitch_sensor;}
  {
    fd_calltrack_sensor cts = fd_get_calltrack_sensor("IVSWITCHES",1);
    cts->enabled = 0; cts->intfcn = ivcxtswitch_sensor;}
#endif
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
