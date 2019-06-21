/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define KNO_PROVIDE_FASTEVAL 1 */

#include "kno/knosource.h"

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

#if ((HAVE_SYS_UTSNAME_H)&&(HAVE_UNAME))
#include <sys/utsname.h>
#endif

#if ((HAVE_SYS_SYSINFO_H)&&(HAVE_SYSINFO))
#include <sys/sysinfo.h>
#endif

u8_condition kno_MissingFeature=_("OS doesn't support operation");

/* Getting the current hostname */

DCLPRIM("GETHOSTNAME",hostname_prim,0,
        "Gets the assigned name for this computer")
static lispval hostname_prim()
{
  return kno_lispstring(u8_gethostname());
}

DCLPRIM1("HOSTADDRS",hostaddrs_prim,0,
         "Gets the addresses associated with a hostname "
         "using the ldns library",
         kno_string_type,KNO_VOID)
static lispval hostaddrs_prim(lispval hostname)
{
  int addr_len = -1; unsigned int type = -1;
  char **addrs = u8_lookup_host(CSTRING(hostname),&addr_len,&type);
  lispval results = EMPTY;
  int i = 0;
  if (addrs == NULL) {
    kno_clear_errors(1);
    return results;}
  else while (addrs[i]) {
      unsigned char *addr = addrs[i++]; lispval string;
      struct U8_OUTPUT out; int j = 0; U8_INIT_OUTPUT(&out,16);
      while (j<addr_len) {
        u8_printf(&out,((j>0)?(".%d"):("%d")),(int)addr[j]);
        j++;}
      string = kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
      CHOICE_ADD(results,string);}
  u8_free(addrs);
  return results;
}

/* GETENV primitive */

DCLPRIM1("GETENV",getenv_prim,0,
         "Gets the value of *envvar* in the environment of "
         "the current process. Returns a string or #f if "
         "the environment variable is not defined.",
         kno_string_type,KNO_VOID)
static lispval getenv_prim(lispval var)
{
  u8_string enval = u8_getenv(CSTRING(var));
  if (enval == NULL)
    return KNO_FALSE;
  else return kno_lispstring(enval);
}

static lispval getenv_macro(lispval expr,kno_lexenv env,kno_stack ptr)
{
  lispval var = kno_get_arg(expr,1);
  if ( (KNO_STRINGP(var)) || (KNO_SYMBOLP(var)) ) {
    u8_string enval = (KNO_SYMBOLP(var)) ?
      (u8_getenv(KNO_SYMBOL_NAME(var))) :
      (u8_getenv(CSTRING(var)));
    if (enval == NULL)
      return KNO_FALSE;
    else return kno_lispstring(enval);}
  else return kno_err(kno_TypeError,"getenv_macro","string or symbol",var);
}

/* LOAD AVERAGE */

DCLPRIM("GETLOAD",loadavg_prim,0,
        "Gets the current host's load average.")
static lispval loadavg_prim()
{
  double loadavg;
  int nsamples = getloadavg(&loadavg,1);
  if (nsamples==1)
    return kno_make_flonum(loadavg);
  else return KNO_FALSE;
}

DCLPRIM("LOADAVG",loadavgs_prim,0,
        "Gets the load averages for the current host.")
static lispval loadavgs_prim()
{
  double loadavg[3]; int nsamples = getloadavg(loadavg,3);
  if (nsamples==1)
    return kno_make_nvector(1,kno_make_flonum(loadavg[0]));
  else if (nsamples==2)
    return kno_make_nvector
      (2,kno_make_flonum(loadavg[0]),kno_make_flonum(loadavg[1]));
  else if (nsamples==3)
    return kno_make_nvector
      (3,kno_make_flonum(loadavg[0]),kno_make_flonum(loadavg[1]),
       kno_make_flonum(loadavg[2]));
  else return KNO_FALSE;
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
  lispval iptr = KNO_INT(ival);
  kno_add(table,symbol,iptr);
  if (CONSP(iptr)) kno_decref(iptr);
}

static void add_flonum(lispval table,lispval symbol,double fval)
{
  lispval flonum = kno_make_flonum(fval);
  kno_add(table,symbol,flonum);
  kno_decref(flonum);
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
    return KNO_ERROR;
  else if (VOIDP(field)) {
    lispval result = kno_empty_slotmap();
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
    add_intval(result,nptrlocks_symbol,KNO_N_PTRLOCKS);
    add_intval(result,pid_symbol,pid);
    add_intval(result,ppid_symbol,ppid);
    { /* Load average(s) */
      double loadavg[3]; int nsamples = getloadavg(loadavg,3);
      if (nsamples>0) {
        lispval lval = kno_make_flonum(loadavg[0]), lvec = VOID;
        kno_store(result,load_symbol,lval);
        if (nsamples==1)
          lvec = kno_make_nvector(1,kno_make_flonum(loadavg[0]));
        else if (nsamples==2)
          lvec = kno_make_nvector(2,kno_make_flonum(loadavg[0]),
                                  kno_make_flonum(loadavg[1]));
        else lvec = kno_make_nvector
               (3,kno_make_flonum(loadavg[0]),
                kno_make_flonum(loadavg[1]),
                kno_make_flonum(loadavg[2]));
        if (!(VOIDP(lvec)))
	  kno_store(result,loadavg_symbol,lvec);
        kno_decref(lval); kno_decref(lvec);}}
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
      kno_add(result,n_cpus_symbol,KNO_INT(n_cpus));
      kno_add(result,max_cpus_symbol,KNO_INT(max_cpus));
      if (pagesize>0)
	kno_add(result,pagesize_symbol,KNO_INT(pagesize));
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

    return kno_read_sensors(result);}

  else if (KNO_EQ(field,cpusage_symbol)) {
    double elapsed = u8_elapsed_time()*1000000.0;
    double stime = u8_dbltime(r.ru_stime);
    double utime = u8_dbltime(r.ru_utime);
    double cpusage = (stime+utime)*100.0/elapsed;
    return kno_init_double(NULL,cpusage);}
  else if (KNO_EQ(field,data_symbol))
    return KNO_INT((r.ru_idrss*pagesize));
  else if (KNO_EQ(field,clock_symbol))
    return kno_make_flonum(u8_elapsed_time());
  else if (KNO_EQ(field,stack_symbol))
    return KNO_INT((r.ru_isrss*pagesize));
  else if (KNO_EQ(field,private_symbol))
    return KNO_INT((r.ru_idrss+r.ru_isrss)*pagesize);
  else if (KNO_EQ(field,shared_symbol))
    return KNO_INT((r.ru_ixrss*pagesize));
  else if (KNO_EQ(field,rss_symbol))
    return KNO_INT((r.ru_maxrss*pagesize));
  else if (KNO_EQ(field,datakb_symbol))
    return KNO_INT((r.ru_idrss*pagesize)/1024);
  else if (KNO_EQ(field,stackkb_symbol))
    return KNO_INT((r.ru_isrss*pagesize)/1024);
  else if (KNO_EQ(field,stacksize_symbol))
    return KNO_INT(u8_stack_size);
  else if (KNO_EQ(field,privatekb_symbol))
    return KNO_INT(((r.ru_idrss+r.ru_isrss)*pagesize)/1024);
  else if (KNO_EQ(field,sharedkb_symbol))
    return KNO_INT((r.ru_ixrss*pagesize)/1024);
  else if (KNO_EQ(field,rsskb_symbol))
    return KNO_INT((r.ru_maxrss*pagesize)/1024);
  else if (KNO_EQ(field,utime_symbol))
    return kno_make_flonum(u8_dbltime(r.ru_utime));
  else if (KNO_EQ(field,stime_symbol))
    return kno_make_flonum(u8_dbltime(r.ru_stime));
  else if (KNO_EQ(field,memusage_symbol))
    return KNO_INT(u8_memusage());
  else if (KNO_EQ(field,vmemusage_symbol))
    return KNO_INT(u8_vmemusage());
  else if (KNO_EQ(field,nptrlocks_symbol))
    return KNO_INT(KNO_N_PTRLOCKS);
  else if (KNO_EQ(field,mallocinfo_symbol)) {
    u8_string info=get_malloc_info();
    if (info)
      return kno_init_string(NULL,-1,info);
    else return EMPTY;}
  else if (KNO_EQ(field,load_symbol)) {
    double loadavg; int nsamples = getloadavg(&loadavg,1);
    if (nsamples>0)
      return kno_make_flonum(loadavg);
    else return EMPTY;}
  else if (KNO_EQ(field,loadavg_symbol)) {
    double loadavg[3]; int nsamples = getloadavg(loadavg,3);
    if (nsamples>0) {
      if (nsamples==1)
        return kno_make_nvector(1,kno_make_flonum(loadavg[0]));
      else if (nsamples==2)
        return kno_make_nvector(2,kno_make_flonum(loadavg[0]),
                                kno_make_flonum(loadavg[1]));
      else return kno_make_nvector
             (3,kno_make_flonum(loadavg[0]),kno_make_flonum(loadavg[1]),
              kno_make_flonum(loadavg[2]));}
    else if (KNO_EQ(field,pid_symbol))
      return KNO_INT((unsigned long)(getpid()));
    else if (KNO_EQ(field,ppid_symbol))
      return KNO_INT((unsigned long)(getppid()));
    else return EMPTY;}
  else if (KNO_EQ(field,n_cpus_symbol)) {
    int n_cpus = get_n_cpus();
    if (n_cpus>0)
      return KNO_INT(n_cpus);
    else if (n_cpus==0)
      return EMPTY;
    else {
      u8_graberrno("rusage_prim/N_CPUS",NULL);
      return KNO_ERROR;}}
  else if (KNO_EQ(field,max_cpus_symbol)) {
    int max_cpus = get_max_cpus();
    if (max_cpus>0)
      return KNO_INT(max_cpus);
    else if (max_cpus==0)
      return EMPTY;
    else {
      u8_graberrno("rusage_prim/MAX_CPUS",NULL);
      return KNO_ERROR;}}
  else if (KNO_EQ(field,pagesize_symbol)) {
    int pagesize = get_pagesize();
    if (pagesize>0)
      return KNO_INT(pagesize);
    else if (pagesize==0)
      return EMPTY;
    else {
      u8_graberrno("rusage_prim/PAGESIZE",NULL);
      return KNO_ERROR;}}
  else if (KNO_EQ(field,physical_pages_symbol)) {
    int physical_pages = get_physical_pages();
    if (physical_pages>0)
      return KNO_INT(physical_pages);
    else if (physical_pages==0)
      return EMPTY;
    else {
      u8_graberrno("rusage_prim/PHYSICAL_PAGES",NULL);
      return KNO_ERROR;}}
  else if (KNO_EQ(field,available_pages_symbol)) {
    int available_pages = get_available_pages();
    if (available_pages>0)
      return KNO_INT(available_pages);
    else if (available_pages==0)
      return EMPTY;
    else {
      u8_graberrno("rusage_prim/AVAILABLE_PAGES",NULL);
      return KNO_ERROR;}}
  else if (KNO_EQ(field,physical_memory_symbol)) {
    long long physical_memory = get_physical_memory();
    if (physical_memory>0)
      return KNO_INT(physical_memory);
    else if (physical_memory==0)
      return EMPTY;
    else {
      u8_graberrno("rusage_prim/PHYSICAL_MEMORY",NULL);
      return KNO_ERROR;}}
  else if (KNO_EQ(field,physicalmb_symbol)) {
    long long physical_memory = get_physical_memory();
    if (physical_memory>0)
      return KNO_INT(physical_memory/(1024*1024));
    else if (physical_memory==0)
      return EMPTY;
    else {
      u8_graberrno("rusage_prim/PHYSICAL_MEMORY",NULL);
      return KNO_ERROR;}}
  else if (KNO_EQ(field,available_memory_symbol)) {
    long long available_memory = get_available_memory();
    if (available_memory>0)
      return KNO_INT(available_memory);
    else if (available_memory==0)
      return EMPTY;
    else {
      u8_graberrno("rusage_prim/AVAILABLE_MEMORY",NULL);
      return KNO_ERROR;}}
  else if (KNO_EQ(field,availablemb_symbol)) {
    long long available_memory = get_available_memory();
    if (available_memory>0)
      return KNO_INT(available_memory/(1024*1024));
    else if (available_memory==0)
      return EMPTY;
    else {
      u8_graberrno("rusage_prim/AVAILABLE_MEMORY",NULL);
      return KNO_ERROR;}}
  else {
    lispval val=kno_read_sensor(field);
    if (VOIDP(val))
      return EMPTY;
    else return val;}
}

static int setprop(lispval result,u8_string field,char *value)
{
  if ((value)&&(strcmp(value,"(none)"))) {
    lispval slotid = kno_intern(field);
    u8_string svalue = u8_fromlibc(value);
    lispval lvalue = knostring(svalue);
    int rv = kno_store(result,slotid,lvalue);
    kno_decref(lvalue);
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
    lispval result = kno_init_slotmap(NULL,0,NULL);
    setprop(result,"osname",sysinfo.sysname);
    setprop(result,"nodename",sysinfo.nodename);
    setprop(result,"release",sysinfo.release);
    setprop(result,"version",sysinfo.version);
    setprop(result,"machine",sysinfo.machine);
    return result;}
  else {
    u8_graberr(errno,"uname_prim",NULL);
    return KNO_ERROR;}
#else
  return kno_init_slotmap(NULL,0,NULL);
#endif
}

DCLPRIM("GETPID",getpid_prim,0,
        "Gets the PID (process ID) for the current process")
static lispval getpid_prim()
{
  pid_t pid = getpid();
  return KNO_INT(((unsigned long)pid));
}
DCLPRIM("GETPPID",getppid_prim,0,
        "Gets the PID (process ID) for the parent of the "
        "current process")
static lispval getppid_prim()
{
  pid_t pid = getppid();
  return KNO_INT(((unsigned long)pid));
}

DCLPRIM("STACKSIZE",stacksize_prim,0,
        "Gets the stack size for the current thread")
static lispval stacksize_prim()
{
  ssize_t size = u8_stacksize();
  if (size<0)
    return KNO_ERROR;
  else return KNO_INT(size);
}

DCLPRIM("THREADID",threadid_prim,0,
        "Gets the numeric identifier for the current thread.")
static lispval threadid_prim()
{
  long long tid = u8_threadid();
  return KNO_INT(tid);
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
  return KNO_INT(size);
}

DCLPRIM("VMEMUSAGE",vmemusage_prim,0,
        "Gets the virtual memory usage by the current process.")
static lispval vmemusage_prim()
{
  ssize_t size = u8_vmemusage();
  return KNO_INT(size);
}

DCLPRIM("PHYSMEM",physmem_prim,0,
        "Gets the physical memory available on the host.")
static lispval physmem_prim()
{
  ssize_t size = u8_physmem();
  return KNO_INT(size);
}

DCLPRIM("MEMLOAD",memload_prim,0,
        "Gets the memory load for the current process.")
static lispval memload_prim()
{
  double load = u8_memload();
  return kno_make_flonum(load);
}

DCLPRIM("VMEMLOAD",vmemload_prim,0,
        "Gets the virtual memory load for the current process.")
static lispval vmemload_prim()
{
  double vload = u8_vmemload();
  return kno_make_flonum(vload);
}

DCLPRIM("USERTIME",usertime_prim,0,
        "Gets the total user-space run time for the current process.")
static lispval usertime_prim()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return KNO_ERROR;
  else {
    double msecs=
      (r.ru_utime.tv_sec*1000000.0+r.ru_utime.tv_usec*1.0)-
      (init_rusage.ru_utime.tv_sec*1000000.0+
       init_rusage.ru_utime.tv_usec*1.0);
    return kno_init_double(NULL,msecs);}
}

DCLPRIM("SYSTIME",systime_prim,0,
        "Gets the total system run time for the current process.")
static lispval systime_prim()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0)
    return KNO_ERROR;
  else {
    double msecs=
      (r.ru_stime.tv_sec*1000000.0+r.ru_stime.tv_usec*1.0)-
      (init_rusage.ru_stime.tv_sec*1000000.0+
       init_rusage.ru_stime.tv_usec*1.0);
    return kno_init_double(NULL,msecs);}
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
      return KNO_ERROR;
    else {
      lispval prelapsed = kno_get(arg,clock_symbol,VOID);
      lispval prestime = kno_get(arg,stime_symbol,VOID);
      lispval preutime = kno_get(arg,utime_symbol,VOID);
      if ((KNO_FLONUMP(prelapsed)) &&
          (KNO_FLONUMP(prestime)) &&
          (KNO_FLONUMP(preutime))) {
        double elapsed=
          (u8_elapsed_time()-KNO_FLONUM(prelapsed))*1000000.0;
        double stime = (u8_dbltime(r.ru_stime)-KNO_FLONUM(prestime));
        double utime = u8_dbltime(r.ru_utime)-KNO_FLONUM(preutime);
        double cpusage = (stime+utime)*100.0/elapsed;
	kno_decref(prelapsed); kno_decref(prestime); kno_decref(preutime);
       return kno_init_double(NULL,cpusage);}
      else {
	kno_decref(prelapsed); kno_decref(prestime); kno_decref(preutime);
	return kno_type_error(_("rusage"),"getcpusage",arg);}}}
}

static int get_max_cpus()
{
  int retval = 0;
#if ((HAVE_SYSCONF)&&(defined(_SC_NPROCESSORS_CONF)))
  retval = sysconf(_SC_NPROCESSORS_CONF);
  if (retval>0)
    return retval;
  if (retval<0)
    kno_clear_errors(1);
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
  if (retval>0)
    return retval;
  if (retval<0)
    kno_clear_errors(1);
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
  if (pagesize>=0)
    return pagesize;
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
  if (retval>0)
    return retval;
  if (retval<0)
    kno_clear_errors(1);
  return 0;
}

static int get_physical_pages()
{
#if ((HAVE_SYSCONF)&&(defined(_SC_PHYS_PAGES)))
  int retval = sysconf(_SC_PHYS_PAGES);
  if (retval>0)
    return retval;
  if (retval<0)
    kno_clear_errors(1);
#endif
  return 0;
}

static int get_available_pages()
{
#if ((HAVE_SYSCONF)&&(defined(_SC_AVPHYS_PAGES)))
  int retval = sysconf(_SC_AVPHYS_PAGES);
  if (retval>0)
    return retval;
  if (retval<0)
    kno_clear_errors(1);
#endif
  return 0;
}

static long long get_physical_memory()
{
  long long retval = 0;
  if (pagesize<0) pagesize = get_pagesize();
  if (pagesize==0)
    return 0;
#if ((HAVE_SYSCONF)&&(defined(_SC_PHYS_PAGES)))
  retval = sysconf(_SC_PHYS_PAGES);
  if (retval>0)
    return retval*pagesize;
  if (retval<0)
    kno_clear_errors(1);
#endif
  return 0;
}

static long long get_available_memory()
{
  long long retval = 0;
  if (pagesize<0) pagesize = get_pagesize();
  if (pagesize==0)
    return 0;
#if ((HAVE_SYSCONF)&&(defined(_SC_AVPHYS_PAGES)))
  retval = sysconf(_SC_AVPHYS_PAGES);
  if (retval>0)
    return retval*pagesize;
  if (retval<0)
    kno_clear_errors(1);
#endif
  return retval;
}

/* Initialization */

KNO_EXPORT void kno_init_procprims_c(void);

KNO_EXPORT void kno_init_sysprims_c()
{
  u8_register_source_file(_FILEINFO);

  u8_getrusage(RUSAGE_SELF,&init_rusage);

  data_symbol = kno_intern("data");
  datakb_symbol = kno_intern("datakb");
  stack_symbol = kno_intern("stack");
  stackkb_symbol = kno_intern("stackkb");
  shared_symbol = kno_intern("shared");
  sharedkb_symbol = kno_intern("sharedkb");
  private_symbol = kno_intern("private");
  privatekb_symbol = kno_intern("privatekb");
  rss_symbol = kno_intern("resident");
  rsskb_symbol = kno_intern("residentkb");
  utime_symbol = kno_intern("utime");
  stime_symbol = kno_intern("stime");
  clock_symbol = kno_intern("clock");
  cpusage_symbol = kno_intern("cpusage");
  pid_symbol = kno_intern("pid");
  ppid_symbol = kno_intern("ppid");
  memusage_symbol = kno_intern("memusage");
  vmemusage_symbol = kno_intern("vmemusage");
  memload_symbol = kno_intern("memload");
  vmemload_symbol = kno_intern("vmemload");
  stacksize_symbol = kno_intern("stacksize");
  n_cpus_symbol = kno_intern("ncpus");
  max_cpus_symbol = kno_intern("maxcpus");
  nptrlocks_symbol = kno_intern("nptrlocks");
  pagesize_symbol = kno_intern("pagesize");
  physical_pages_symbol = kno_intern("physical-pages");
  available_pages_symbol = kno_intern("available-pages");
  physical_memory_symbol = kno_intern("physical-memory");
  available_memory_symbol = kno_intern("available-memory");
  physicalmb_symbol = kno_intern("physmb");
  availablemb_symbol = kno_intern("availmb");
  cpusage_symbol = kno_intern("cpu%");
  tcpusage_symbol = kno_intern("cpu%/cpu");

  mallocd_symbol = kno_intern("mallocd");
  heap_symbol = kno_intern("heap");
  mallocinfo_symbol = kno_intern("mallocinfo");
  tcmallocinfo_symbol = kno_intern("tcmallocinfo");

  load_symbol = kno_intern("load");
  loadavg_symbol = kno_intern("loadavg");

  uptime_symbol=kno_intern("uptime");
  nprocs_symbol=kno_intern("nprocs");
  free_swap_symbol=kno_intern("freeswap");
  total_swap_symbol=kno_intern("totalswap");
  swap_symbol=kno_intern("swap");
  free_ram_symbol=kno_intern("freeram");
  total_ram_symbol=kno_intern("totalram");
  max_vmem_symbol=kno_intern("maxvmem");

  DECL_PRIM(hostname_prim,0,kno_sys_module);
  DECL_PRIM(hostaddrs_prim,1,kno_sys_module);
  DECL_PRIM(getenv_prim,1,kno_sys_module);

  kno_idefn(kno_sys_module,kno_make_cprim1("RUSAGE",rusage_prim,0));
  kno_idefn(kno_sys_module,kno_make_cprim1("CPUSAGE",cpusage_prim,0));

  DECL_PRIM(rusage_prim,1,kno_sys_module);
  DECL_PRIM(cpusage_prim,1,kno_sys_module);

  DECL_PRIM(memusage_prim,0,kno_sys_module);

  DECL_PRIM(memusage_prim,0,kno_sys_module);
  DECL_PRIM(vmemusage_prim,0,kno_sys_module);
  DECL_PRIM(memload_prim,0,kno_sys_module);
  DECL_PRIM(vmemload_prim,0,kno_sys_module);
  DECL_PRIM(usertime_prim,0,kno_sys_module);
  DECL_PRIM(systime_prim,0,kno_sys_module);
  DECL_PRIM(physmem_prim,0,kno_sys_module);

  DECL_PRIM(loadavg_prim,0,kno_sys_module);
  DECL_PRIM(loadavgs_prim,0,kno_sys_module);

  DECL_PRIM(uname_prim,0,kno_sys_module);

  DECL_PRIM(getpid_prim,0,kno_sys_module);
  DECL_PRIM(getppid_prim,0,kno_sys_module);
  DECL_PRIM(threadid_prim,0,kno_sys_module);
  DECL_PRIM(getprocstring_prim,0,kno_sys_module);
  DECL_PRIM(stacksize_prim,0,kno_sys_module);

  kno_def_evalfn(kno_sys_module,"#ENV",
                 "#:ENV\"HOME\" or #:ENV:HOME\n"
                 "evaluates to an environment variable",
                 getenv_macro);

  kno_init_procprims_c();

}
