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
  if (VOIDP(opts))
    return fd_incref(dflt);
  else if (EMPTYP(opts))
    return fd_incref(dflt);
  else if ((CHOICEP(opts)) || (PRECHOICEP(opts))) {
    DO_CHOICES(opt,opts) {
      fdtype value = fd_getopt(opt,key,VOID);
      if (!(VOIDP(value))) {
        FD_STOP_DO_CHOICES; return value;}}
    return fd_incref(dflt);}
  else if (QCHOICEP(opts))
    return fd_getopt(FD_XQCHOICE(opts)->qchoiceval,key,dflt);
  else while (!(VOIDP(opts))) {
      if (PAIRP(opts)) {
	fdtype car = FD_CAR(opts);
	if (SYMBOLP(car)) {
	  if (FD_EQ(key,car))
	    return FD_TRUE;
	  else {}}
	else if (PAIRP(car)) {
	  if (FD_EQ(FD_CAR(car),key))
	    return fd_incref(FD_CDR(car));
	  else {
	    fdtype value = fd_getopt(car,key,VOID);
	    if (!(VOIDP(value)))
	      return value;}}
	else if (TABLEP(car)) {
	  fdtype value = fd_get(car,key,VOID);
	  if (!(VOIDP(value)))
	    return value;}
	else if ((FALSEP(car))||(NILP(car))) {}
	else return fd_err(WeirdOption,"fd_getopt",NULL,car);
	opts = FD_CDR(opts);}
      else if (SYMBOLP(opts))
        if (FD_EQ(key,opts))
	  return FD_TRUE;
        else return fd_incref(dflt);
      else if (TABLEP(opts))
        return fd_get(opts,key,dflt);
      else if ((NILP(opts))||(FALSEP(opts)))
        return fd_incref(dflt);
      else return fd_err(WeirdOption,"fd_getopt",NULL,opts);}
  return fd_incref(dflt);
}

static int boolopt(fdtype opts,fdtype key)
{
  while (!(VOIDP(opts))) {
    if (PAIRP(opts)) {
      fdtype car = FD_CAR(opts);
      if (SYMBOLP(car)) {
        if (FD_EQ(key,car)) return 1;}
      else if (PAIRP(car)) {
        if (FD_EQ(FD_CAR(car),key)) {
          if (FALSEP(FD_CDR(car))) return 0;
          else return 1;}}
      else if (FALSEP(car)) {}
      else if (TABLEP(car)) {
        fdtype value = fd_get(car,key,VOID);
        if (FALSEP(value)) return 0;
        else if (!(VOIDP(value))) {
          fd_decref(value); return 1;}}
      else return fd_err(WeirdOption,"fd_getopt",NULL,car);
      opts = FD_CDR(opts);}
    else if (SYMBOLP(opts))
      if (FD_EQ(key,opts)) return 1;
      else return 0;
    else if (TABLEP(opts)) {
      fdtype value = fd_get(opts,key,VOID);
      if (FALSEP(value)) return 0;
      else if (VOIDP(value)) return 0;
      else return 1;}
    else if ((NILP(opts))||(FALSEP(opts)))
      return 0;
    else return fd_err(WeirdOption,"fd_getopt",NULL,opts);}
  return 0;
}

FD_EXPORT int fd_testopt(fdtype opts,fdtype key,fdtype val)
{
  if (VOIDP(opts)) return 0;
  else if ((CHOICEP(opts)) || (PRECHOICEP(opts))) {
    DO_CHOICES(opt,opts)
      if (fd_testopt(opt,key,val)) {
        FD_STOP_DO_CHOICES; return 1;}
    return 0;}
  else if (VOIDP(val))
    return boolopt(opts,key);
  else if (QCHOICEP(opts))
    return fd_testopt(FD_XQCHOICE(opts)->qchoiceval,key,val);
  else if (EMPTYP(opts))
    return 0;
  else while (!(VOIDP(opts))) {
         if (PAIRP(opts)) {
           fdtype car = FD_CAR(opts);
           if (SYMBOLP(car)) {
             if ((FD_EQ(key,car)) && (FD_TRUEP(val)))
               return 1;}
           else if (PAIRP(car)) {
             if (FD_EQ(FD_CAR(car),key)) {
               if (FD_EQUAL(val,FD_CDR(car)))
                 return 1;
               else return 0;}}
           else if (TABLEP(car)) {
             int tv = fd_test(car,key,val);
             if (tv) return tv;}
           else if (FALSEP(car)) {}
           else return fd_err(WeirdOption,"fd_getopt",NULL,car);
           opts = FD_CDR(opts);}
         else if (SYMBOLP(opts))
           if (FD_EQ(key,opts))
             if (FD_TRUEP(val)) return 1;
             else return 0;
           else return 0;
         else if (TABLEP(opts))
           return fd_test(opts,key,val);
         else if ((NILP(opts))||(FALSEP(opts)))
           return 0;
         else return fd_err(WeirdOption,"fd_getopt",NULL,opts);}
  return 0;
}

FD_EXPORT
long long
fd_fixopt(fdtype opts,u8_string name,int dflt)
{
  fdtype val=fd_getopt(opts,fd_intern(name),VOID);
  if (VOIDP(val))
    return dflt;
  else if (FIXNUMP(val))
    return FIX2INT(val);
  else {
    fd_decref(val);
    return dflt;}
}



void fd_init_getopt_c()
{
  u8_register_source_file(_FILEINFO);
}
