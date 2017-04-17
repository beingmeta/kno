/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/apply.h"

#include <libu8/u8signals.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <signal.h>
#include <sys/types.h>
#include <pwd.h>

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#if FD_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

/* Option objects */

static fd_exception WeirdOption=_("Weird option specification");

FD_EXPORT fdtype fd_getopt(fdtype opts,fdtype key,fdtype dflt)
{
  if (FD_VOIDP(opts))
    return fd_incref(dflt);
  else if (FD_EMPTY_CHOICEP(opts))
    return fd_incref(dflt);
  else if ((FD_CHOICEP(opts)) || (FD_PRECHOICEP(opts))) {
    FD_DO_CHOICES(opt,opts) {
      fdtype value = fd_getopt(opt,key,FD_VOID);
      if (!(FD_VOIDP(value))) {
        FD_STOP_DO_CHOICES; return value;}}
    return fd_incref(dflt);}
  else if (FD_QCHOICEP(opts))
    return fd_getopt(FD_XQCHOICE(opts)->qchoiceval,key,dflt);
  else while (!(FD_VOIDP(opts))) {
      if (FD_PAIRP(opts)) {
        fdtype car = FD_CAR(opts);
        if (FD_SYMBOLP(car)) {
          if (FD_EQ(key,car)) return FD_TRUE;}
        else if (FD_PAIRP(car)) {
          if (FD_EQ(FD_CAR(car),key))
            return fd_incref(FD_CDR(car));
          else {
            fdtype value = fd_getopt(car,key,FD_VOID);
            if (!(FD_VOIDP(value))) return value;}}
        else if (FD_TABLEP(car)) {
          fdtype value = fd_get(car,key,FD_VOID);
          if (!(FD_VOIDP(value))) return value;}
        else if ((FD_FALSEP(car))||(FD_EMPTY_LISTP(car))) {}
        else return fd_err(WeirdOption,"fd_getopt",NULL,car);
        opts = FD_CDR(opts);}
      else if (FD_SYMBOLP(opts))
        if (FD_EQ(key,opts)) return FD_TRUE;
        else return fd_incref(dflt);
      else if (FD_TABLEP(opts))
        return fd_get(opts,key,dflt);
      else if ((FD_EMPTY_LISTP(opts))||(FD_FALSEP(opts)))
        return fd_incref(dflt);
      else return fd_err(WeirdOption,"fd_getopt",NULL,opts);}
  return fd_incref(dflt);
}

static int boolopt(fdtype opts,fdtype key)
{
  while (!(FD_VOIDP(opts))) {
    if (FD_PAIRP(opts)) {
      fdtype car = FD_CAR(opts);
      if (FD_SYMBOLP(car)) {
        if (FD_EQ(key,car)) return 1;}
      else if (FD_PAIRP(car)) {
        if (FD_EQ(FD_CAR(car),key)) {
          if (FD_FALSEP(FD_CDR(car))) return 0;
          else return 1;}}
      else if (FD_FALSEP(car)) {}
      else if (FD_TABLEP(car)) {
        fdtype value = fd_get(car,key,FD_VOID);
        if (FD_FALSEP(value)) return 0;
        else if (!(FD_VOIDP(value))) {
          fd_decref(value); return 1;}}
      else return fd_err(WeirdOption,"fd_getopt",NULL,car);
      opts = FD_CDR(opts);}
    else if (FD_SYMBOLP(opts))
      if (FD_EQ(key,opts)) return 1;
      else return 0;
    else if (FD_TABLEP(opts)) {
      fdtype value = fd_get(opts,key,FD_VOID);
      if (FD_FALSEP(value)) return 0;
      else if (FD_VOIDP(value)) return 0;
      else return 1;}
    else if ((FD_EMPTY_LISTP(opts))||(FD_FALSEP(opts)))
      return 0;
    else return fd_err(WeirdOption,"fd_getopt",NULL,opts);}
  return 0;
}

FD_EXPORT int fd_testopt(fdtype opts,fdtype key,fdtype val)
{
  if (FD_VOIDP(opts)) return 0;
  else if ((FD_CHOICEP(opts)) || (FD_PRECHOICEP(opts))) {
    FD_DO_CHOICES(opt,opts)
      if (fd_testopt(opt,key,val)) {
        FD_STOP_DO_CHOICES; return 1;}
    return 0;}
  else if (FD_VOIDP(val))
    return boolopt(opts,key);
  else if (FD_QCHOICEP(opts))
    return fd_testopt(FD_XQCHOICE(opts)->qchoiceval,key,val);
  else if (FD_EMPTY_CHOICEP(opts))
    return 0;
  else while (!(FD_VOIDP(opts))) {
         if (FD_PAIRP(opts)) {
           fdtype car = FD_CAR(opts);
           if (FD_SYMBOLP(car)) {
             if ((FD_EQ(key,car)) && (FD_TRUEP(val)))
               return 1;}
           else if (FD_PAIRP(car)) {
             if (FD_EQ(FD_CAR(car),key)) {
               if (FD_EQUAL(val,FD_CDR(car)))
                 return 1;
               else return 0;}}
           else if (FD_TABLEP(car)) {
             int tv = fd_test(car,key,val);
             if (tv) return tv;}
           else if (FD_FALSEP(car)) {}
           else return fd_err(WeirdOption,"fd_getopt",NULL,car);
           opts = FD_CDR(opts);}
         else if (FD_SYMBOLP(opts))
           if (FD_EQ(key,opts))
             if (FD_TRUEP(val)) return 1;
             else return 0;
           else return 0;
         else if (FD_TABLEP(opts))
           return fd_test(opts,key,val);
         else if ((FD_EMPTY_LISTP(opts))||(FD_FALSEP(opts)))
           return 0;
         else return fd_err(WeirdOption,"fd_getopt",NULL,opts);}
  return 0;
}

FD_EXPORT
long long
fd_fixopt(fdtype opts,u8_string name,int dflt)
{
  fdtype val=fd_getopt(opts,fd_intern(name),FD_VOID);
  if (FD_VOIDP(val))
    return dflt;
  else if (FD_FIXNUMP(val))
    return FD_FIX2INT(val);
  else {
    fd_decref(val);
    return dflt;}
}



void fd_init_getopt_c()
{
  u8_register_source_file(_FILEINFO);
}
