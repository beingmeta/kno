/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define FD_PROVIDE_FASTEVAL 1 */

#include "framerd/fdsource.h"

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include "framerd/cprims.h"

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

#if ((HAVE_SYS_UTSNAME_H)&&(HAVE_UNAME))
#include <sys/utsname.h>
#endif

#if ((HAVE_SYS_SYSINFO_H)&&(HAVE_SYSINFO))
#include <sys/sysinfo.h>
#endif

u8_condition fd_MissingFeature=_("OS doesn't support operation");

/* Getting the current hostname */

DCLPRIM("GETHOSTNAME",hostname_prim,0,
        "Gets the assigned name for this computer")
static lispval hostname_prim()
{
  return fd_lispstring(u8_gethostname());
}

DCLPRIM1("HOSTADDRS",hostaddrs_prim,0,
         "Gets the addresses associated with a hostname "
         "using the ldns library",
         fd_string_type,FD_VOID)
static lispval hostaddrs_prim(lispval hostname)
{
  int addr_len = -1; unsigned int type = -1;
  char **addrs = u8_lookup_host(CSTRING(hostname),&addr_len,&type);
  lispval results = EMPTY;
  int i = 0;
  if (addrs == NULL) {
    fd_clear_errors(1);
    return results;}
  else while (addrs[i]) {
      unsigned char *addr = addrs[i++]; lispval string;
      struct U8_OUTPUT out; int j = 0; U8_INIT_OUTPUT(&out,16);
      while (j<addr_len) {
        u8_printf(&out,((j>0)?(".%d"):("%d")),(int)addr[j]);
        j++;}
      string = fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
      CHOICE_ADD(results,string);}
  u8_free(addrs);
  return results;
}

/* GETENV primitive */

DCLPRIM1("GETENV",getenv_prim,0,
         "Gets the value of *envvar* in the environment of "
         "the current process. Returns a string or #f if "
         "the environment variable is not defined.",
         fd_string_type,FD_VOID)
static lispval getenv_prim(lispval var)
{
  u8_string enval = u8_getenv(CSTRING(var));
  if (enval == NULL) return FD_FALSE;
  else return fd_lispstring(enval);
}

static lispval getenv_macro(lispval expr,fd_lexenv env,fd_stack ptr)
{
  lispval var = fd_get_arg(expr,1);
  if ( (FD_STRINGP(var)) || (FD_SYMBOLP(var)) ) {
    u8_string enval = (FD_SYMBOLP(var)) ?
      (u8_getenv(FD_SYMBOL_NAME(var))) :
      (u8_getenv(CSTRING(var)));
    if (enval == NULL)
      return FD_FALSE;
    else return fd_lispstring(enval);}
  else return fd_err(fd_TypeError,"getenv_macro","string or symbol",var);
}

/* LOAD AVERAGE */

DCLPRIM("GETLOAD",loadavg_prim,0,
        "Gets the current host's load average.")
static lispval loadavg_prim()
{
  double loadavg;
  int nsamples = getloadavg(&loadavg,1);
  if (nsamples==1) return fd_make_flonum(loadavg);
  else return FD_FALSE;
}

DCLPRIM("LOADAVG",loadavgs_prim,0,
        "Gets the load averages for the current host.")
static lispval loadavgs_prim()
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

static lispval data_symbol, stack_symbol, shared_symbol, private_symbol;
static lispval memusage_symbol, vmemusage_symbol, pagesize_symbol, rss_symbol;
static lispval datakb_symbol, stackkb_symbol, sharedkb_symbol;
static lispval rsskb_symbol, privatekb_symbol;
static lispval utime_symbol, stime_symbol, clock_symbol;
static lispval load_symbol, loadavg_symbol, pid_symbol, ppid_symbol;
static lispval memusage_symbol, vmemusage_symbol, pagesize_symbol;
static lispval n_cpus_symbol, max_cpus_symbol;
static lispval physical_pages_symbol, available_pages_symbol;
static lispval physical_memory_symbol, available_memory_symbol;
static lispval physicalmb_symbol, availablemb_symbol;
static lispval memload_symbol, vmemload_symbol, stacksize_symbol;
static lispval nptrlocks_symbol, cpusage_symbol, tcpusage_symbol;
static lispval mallocd_symbol, heap_symbol, mallocinfo_symbol;
static lispval uptime_symbol, total_swap_symbol, swap_symbol, total_ram_symbol;
static lispval free_swap_symbol, free_ram_symbol, nprocs_symbol;
static lispval max_vmem_symbol;

static lispval tcmallocinfo_symbol;

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

static void add_intval(lispval table,lispval symbol,long long ival)
{
  lispval iptr = FD_INT(ival);
  fd_add(table,symbol,iptr);
  if (CONSP(iptr)) fd_decref(iptr);
}

static void add_flonum(lispval table,lispval symbol,double fval)
{
  lispval flonum = fd_make_flonum(fval);
  fd_add(table,symbol,flonum);
  fd_decref(flonum);
}

static u8_string get_malloc_info()
{
#if HAVE_OPEN_MEMSTREAM && HAVE_MALLOC_INFO
  char *buf=NULL;
  size_t buflen=0;
  FILE *out;
  out=open_memstream(&buf,&buflen);
  malloc_info(0,out);
  fclose(out);
  return buf;
#elif HAVE_MSTATS
  struct mstats stats=mstats();
  size_t used=stats.bytes_used, total=stats.bytes_total;
  double pct=(100.0*used)/(total*1.0);
  return u8_mkstring("%lld/%lld (%0.2f%) used/total",
                     used,total,pct);
#else
  return u8_strdup("none");
#endif
}

DCLPRIM("RUSAGE",rusage_prim,MAX_ARGS(1)|MIN_ARGS(0),
        "`(RUSAGE [*field*])` returns information about the "
        "current process. *field*, if provided, indicates the "
        "value to return. Otherwise, a slotmap of possible "
        "values is returned.")
static lispval rusage_prim(lispval field)
{
  struct rusage r;
  int pagesize = get_pagesize();
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return FD_ERROR;
  else if (VOIDP(field)) {
    lispval result = fd_empty_slotmap();
    pid_t pid = getpid(), ppid = getppid();
    ssize_t mem = u8_memusage(), vmem = u8_vmemusage();
    double memload = u8_memload(), vmemload = u8_vmemload();
    size_t n_cpus = get_n_cpus();
#if HAVE_MSTATS
    struct mstats stats=mstats();
    add_intval(result,mallocd_symbol,stats.bytes_used);
    add_intval(result,heap_symbol,stats.bytes_total);
#elif HAVE_MALLINFO
    struct mallinfo meminfo;
    if (sizeof(meminfo.arena)>=8) {
      meminfo=mallinfo();
      add_intval(result,mallocd_symbol,meminfo.arena);}
#endif

#if ((HAVE_SYS_SYSINFO_H)&&(HAVE_SYSINFO))
    struct sysinfo sinfo;
    if (sysinfo(&sinfo)>=0) {
      unsigned int mem_unit=sinfo.mem_unit;
      unsigned long long total_swap=sinfo.totalswap*mem_unit;
      unsigned long long free_swap=sinfo.freeswap*mem_unit;
      unsigned long long total_ram=sinfo.totalram*mem_unit;
      unsigned long long free_ram=sinfo.freeram*mem_unit;
      add_intval(result,uptime_symbol,sinfo.uptime);
      add_intval(result,nprocs_symbol,sinfo.procs);
      add_intval(result,total_swap_symbol,total_swap);
      add_intval(result,free_swap_symbol,free_swap);
      add_intval(result,swap_symbol,total_swap-free_swap);
      add_intval(result,max_vmem_symbol,total_swap+total_ram);
      add_intval(result,free_ram_symbol,free_ram);
      add_intval(result,total_ram_symbol,total_ram);}
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
        lispval lval = fd_make_flonum(loadavg[0]), lvec = VOID;
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
        if (!(VOIDP(lvec))) fd_store(result,loadavg_symbol,lvec);
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

    return fd_read_sensors(result);}

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
    else return EMPTY;}
  else if (FD_EQ(field,load_symbol)) {
    double loadavg; int nsamples = getloadavg(&loadavg,1);
    if (nsamples>0) return fd_make_flonum(loadavg);
    else return EMPTY;}
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
    else return EMPTY;}
  else if (FD_EQ(field,n_cpus_symbol)) {
    int n_cpus = get_n_cpus();
    if (n_cpus>0) return FD_INT(n_cpus);
    else if (n_cpus==0) return EMPTY;
    else {
      u8_graberrno("rusage_prim/N_CPUS",NULL);
      return FD_ERROR;}}
  else if (FD_EQ(field,max_cpus_symbol)) {
    int max_cpus = get_max_cpus();
    if (max_cpus>0) return FD_INT(max_cpus);
    else if (max_cpus==0) return EMPTY;
    else {
      u8_graberrno("rusage_prim/MAX_CPUS",NULL);
      return FD_ERROR;}}
  else if (FD_EQ(field,pagesize_symbol)) {
    int pagesize = get_pagesize();
    if (pagesize>0) return FD_INT(pagesize);
    else if (pagesize==0) return EMPTY;
    else {
      u8_graberrno("rusage_prim/PAGESIZE",NULL);
      return FD_ERROR;}}
  else if (FD_EQ(field,physical_pages_symbol)) {
    int physical_pages = get_physical_pages();
    if (physical_pages>0) return FD_INT(physical_pages);
    else if (physical_pages==0) return EMPTY;
    else {
      u8_graberrno("rusage_prim/PHYSICAL_PAGES",NULL);
      return FD_ERROR;}}
  else if (FD_EQ(field,available_pages_symbol)) {
    int available_pages = get_available_pages();
    if (available_pages>0) return FD_INT(available_pages);
    else if (available_pages==0) return EMPTY;
    else {
      u8_graberrno("rusage_prim/AVAILABLE_PAGES",NULL);
      return FD_ERROR;}}
  else if (FD_EQ(field,physical_memory_symbol)) {
    long long physical_memory = get_physical_memory();
    if (physical_memory>0) return FD_INT(physical_memory);
    else if (physical_memory==0) return EMPTY;
    else {
      u8_graberrno("rusage_prim/PHYSICAL_MEMORY",NULL);
      return FD_ERROR;}}
  else if (FD_EQ(field,physicalmb_symbol)) {
    long long physical_memory = get_physical_memory();
    if (physical_memory>0) return FD_INT(physical_memory/(1024*1024));
    else if (physical_memory==0) return EMPTY;
    else {
      u8_graberrno("rusage_prim/PHYSICAL_MEMORY",NULL);
      return FD_ERROR;}}
  else if (FD_EQ(field,available_memory_symbol)) {
    long long available_memory = get_available_memory();
    if (available_memory>0) return FD_INT(available_memory);
    else if (available_memory==0) return EMPTY;
    else {
      u8_graberrno("rusage_prim/AVAILABLE_MEMORY",NULL);
      return FD_ERROR;}}
  else if (FD_EQ(field,availablemb_symbol)) {
    long long available_memory = get_available_memory();
    if (available_memory>0) return FD_INT(available_memory/(1024*1024));
    else if (available_memory==0) return EMPTY;
    else {
      u8_graberrno("rusage_prim/AVAILABLE_MEMORY",NULL);
      return FD_ERROR;}}
  else {
    lispval val=fd_read_sensor(field);
    if (VOIDP(val))
      return EMPTY;
    else return val;}
}

static int setprop(lispval result,u8_string field,char *value)
{
  if ((value)&&(strcmp(value,"(none)"))) {
    lispval slotid = fd_intern(field);
    u8_string svalue = u8_fromlibc(value);
    lispval lvalue = fdstring(svalue);
    int rv = fd_store(result,slotid,lvalue);
    fd_decref(lvalue);
    u8_free(svalue);
    return rv;}
  else return 0;
}

DCLPRIM("UNAME",uname_prim,0,
        "Returns a slotmap describing the hosting OS.")
static lispval uname_prim()
{
#if ((HAVE_SYS_UTSNAME_H)&&(HAVE_UNAME))
  struct utsname sysinfo;
  int rv = uname(&sysinfo);
  if (rv==0) {
    lispval result = fd_init_slotmap(NULL,0,NULL);
    setprop(result,"OSNAME",sysinfo.sysname);
    setprop(result,"NODENAME",sysinfo.nodename);
    setprop(result,"RELEASE",sysinfo.release);
    setprop(result,"VERSION",sysinfo.version);
    setprop(result,"MACHINE",sysinfo.machine);
    return result;}
  else {
    u8_graberr(errno,"uname_prim",NULL);
    return FD_ERROR;}
#else
  return fd_init_slotmap(NULL,0,NULL);
#endif
}

DCLPRIM("GETPID",getpid_prim,0,
        "Gets the PID (process ID) for the current process")
static lispval getpid_prim()
{
  pid_t pid = getpid();
  return FD_INT(((unsigned long)pid));
}
DCLPRIM("GETPPID",getppid_prim,0,
        "Gets the PID (process ID) for the parent of the "
        "current process")
static lispval getppid_prim()
{
  pid_t pid = getppid();
  return FD_INT(((unsigned long)pid));
}

DCLPRIM("STACKSIZE",stacksize_prim,0,
        "Gets the stack size for the current thread")
static lispval stacksize_prim()
{
  ssize_t size = u8_stacksize();
  if (size<0)
    return FD_ERROR;
  else return FD_INT(size);
}

DCLPRIM("THREADID",threadid_prim,0,
        "Gets the numeric identifier for the current thread.")
static lispval threadid_prim()
{
  long long tid = u8_threadid();
  return FD_INT(tid);
}

DCLPRIM("PROCSTRING",getprocstring_prim,0,
        "Gets a string identifying the current thread id and process id")
static lispval getprocstring_prim()
{
  unsigned char buf[128];
  unsigned char *pinfo = u8_procinfo(buf);
  return lispval_string(pinfo);
}

DCLPRIM("MEMUSAGE",memusage_prim,0,
        "Gets the memory usage by the current process.")
static lispval memusage_prim()
{
  ssize_t size = u8_memusage();
  return FD_INT(size);
}

DCLPRIM("VMEMUSAGE",vmemusage_prim,0,
        "Gets the virtual memory usage by the current process.")
static lispval vmemusage_prim()
{
  ssize_t size = u8_vmemusage();
  return FD_INT(size);
}

DCLPRIM("PHYSMEM",physmem_prim,0,
        "Gets the physical memory available on the host.")
static lispval physmem_prim()
{
  ssize_t size = u8_physmem();
  return FD_INT(size);
}

DCLPRIM("MEMLOAD",memload_prim,0,
        "Gets the memory load for the current process.")
static lispval memload_prim()
{
  double load = u8_memload();
  return fd_make_flonum(load);
}

DCLPRIM("MEMLOAD",vmemload_prim,0,
        "Gets the virtual memory load for the current process.")
static lispval vmemload_prim()
{
  double vload = u8_vmemload();
  return fd_make_flonum(vload);
}

DCLPRIM("USERTIME",usertime_prim,0,
        "Gets the total user-space run time for the current process.")
static lispval usertime_prim()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return FD_ERROR;
  else {
    double msecs=
      (r.ru_utime.tv_sec*1000000.0+r.ru_utime.tv_usec*1.0)-
      (init_rusage.ru_utime.tv_sec*1000000.0+
       init_rusage.ru_utime.tv_usec*1.0);
    return fd_init_double(NULL,msecs);}
}

DCLPRIM("SYSTIME",systime_prim,0,
        "Gets the total system run time for the current process.")
static lispval systime_prim()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return FD_ERROR;
  else {
    double msecs=
      (r.ru_stime.tv_sec*1000000.0+r.ru_stime.tv_usec*1.0)-
      (init_rusage.ru_stime.tv_sec*1000000.0+
       init_rusage.ru_stime.tv_usec*1.0);
    return fd_init_double(NULL,msecs);}
}

DCLPRIM("CPUSAGE",cpusage_prim,MAX_ARGS(1)|MIN_ARGS(0),
        "Provides relative CPU usage information. With no "
        "argument, this returns usage since the process started, "
        "as a slotmap. The result of this call can be passed "
        "as an argument to a later call to get relative timing "
        "information")
static lispval cpusage_prim(lispval arg)
{
  if (VOIDP(arg))
    return rusage_prim(cpusage_symbol);
  else {
    struct rusage r;
    memset(&r,0,sizeof(r));
    if (u8_getrusage(RUSAGE_SELF,&r)<0)
      return FD_ERROR;
    else {
      lispval prelapsed = fd_get(arg,clock_symbol,VOID);
      lispval prestime = fd_get(arg,stime_symbol,VOID);
      lispval preutime = fd_get(arg,utime_symbol,VOID);
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

/* Initialization */

FD_EXPORT void fd_init_procprims_c(void);

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

  uptime_symbol=fd_intern("UPTIME");
  nprocs_symbol=fd_intern("NPROCS");
  free_swap_symbol=fd_intern("FREESWAP");
  total_swap_symbol=fd_intern("TOTALSWAP");
  swap_symbol=fd_intern("SWAP");
  free_ram_symbol=fd_intern("FREERAM");
  total_ram_symbol=fd_intern("TOTALRAM");
  max_vmem_symbol=fd_intern("MAXVMEM");

  DECL_PRIM(hostname_prim,0,fd_scheme_module);
  DECL_PRIM(hostaddrs_prim,1,fd_scheme_module);
  DECL_PRIM(getenv_prim,1,fd_scheme_module);

  fd_idefn(fd_scheme_module,fd_make_cprim1("RUSAGE",rusage_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CPUSAGE",cpusage_prim,0));

  DECL_PRIM(rusage_prim,1,fd_scheme_module);
  DECL_PRIM(cpusage_prim,1,fd_scheme_module);

  DECL_PRIM(memusage_prim,0,fd_scheme_module);

  DECL_PRIM(memusage_prim,0,fd_scheme_module);
  DECL_PRIM(vmemusage_prim,0,fd_scheme_module);
  DECL_PRIM(memload_prim,0,fd_scheme_module);
  DECL_PRIM(vmemload_prim,0,fd_scheme_module);
  DECL_PRIM(usertime_prim,0,fd_scheme_module);
  DECL_PRIM(systime_prim,0,fd_scheme_module);
  DECL_PRIM(physmem_prim,0,fd_scheme_module);

  DECL_PRIM(loadavg_prim,0,fd_scheme_module);
  DECL_PRIM(loadavgs_prim,0,fd_scheme_module);

  DECL_PRIM(uname_prim,0,fd_scheme_module);

  DECL_PRIM(getpid_prim,0,fd_scheme_module);
  DECL_PRIM(getppid_prim,0,fd_scheme_module);
  DECL_PRIM(threadid_prim,0,fd_scheme_module);
  DECL_PRIM(stacksize_prim,0,fd_scheme_module);

  fd_def_evalfn(fd_scheme_module,"#ENV",
                "#:ENV\"HOME\" or #:ENV:HOME\n"
                "evaluates to an environment variable",
                getenv_macro);

  fd_init_procprims_c();

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
