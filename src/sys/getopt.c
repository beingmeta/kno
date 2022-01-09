/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"

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
#if KNO_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

/* Option objects */

static u8_condition WeirdOption=_("Weird option specification");

KNO_EXPORT lispval kno_getopt(lispval opts,lispval key,lispval dflt)
{
  if ( (FALSEP(opts)) || (NILP(opts)) || (EMPTYP(opts)) || (VOIDP(opts)) )
    return kno_incref(dflt);
  else if (SLOTMAPP(opts))
    return kno_slotmap_get((kno_slotmap)opts,key,dflt);
  else if (SCHEMAPP(opts))
    return kno_schemap_get((kno_schemap)opts,key,dflt);
  else if ((CHOICEP(opts)) || (PRECHOICEP(opts))) {
    DO_CHOICES(opt,opts) {
      lispval value = kno_getopt(opt,key,VOID);
      if (!(VOIDP(value))) {
        KNO_STOP_DO_CHOICES;
        return value;}}
    return kno_incref(dflt);}
  else if (QCHOICEP(opts))
    return kno_getopt(KNO_XQCHOICE(opts)->qchoiceval,key,dflt);
  else {
    while (!(VOIDP(opts))) {
      if (PAIRP(opts)) {
        lispval car = KNO_CAR(opts), value = KNO_VOID;
	if (SYMBOLP(car)) {
          if (KNO_EQ(key,car))
            return KNO_TRUE;
          else {}}
        else if (PAIRP(car)) {
          if (KNO_EQ(KNO_CAR(car),key))
            value = kno_incref(KNO_CDR(car));
          else value = kno_getopt(car,key,VOID);}
        else if (TABLEP(car))
          value = kno_get(car,key,VOID);
        else if ((FALSEP(car))||(NILP(car))) {}
        else return kno_err(WeirdOption,"kno_getopt",NULL,car);
        if (KNO_VOIDP(value)) {}
        else if (value == KNO_DEFAULT_VALUE) {}
        else return value;
        opts = KNO_CDR(opts);}
      else if (SYMBOLP(opts))
        if (KNO_EQ(key,opts))
          return KNO_TRUE;
        else return kno_incref(dflt);
      else if (TABLEP(opts))
        return kno_get(opts,key,dflt);
      else if ((NILP(opts))||(FALSEP(opts)))
        return kno_incref(dflt);
      else return kno_err(WeirdOption,"kno_getopt",NULL,opts);}}
  return kno_incref(dflt);
}

static int boolopt(lispval opts,lispval key)
{
  while (!(VOIDP(opts))) {
    if (PAIRP(opts)) {
      lispval car = KNO_CAR(opts);
      if (SYMBOLP(car)) {
        if (KNO_EQ(key,car)) return 1;}
      else if (PAIRP(car)) {
        if (KNO_EQ(KNO_CAR(car),key)) {
          if (FALSEP(KNO_CDR(car))) return 0;
          else return 1;}}
      else if (FALSEP(car)) {}
      else if (TABLEP(car)) {
        lispval value = kno_get(car,key,VOID);
        if (FALSEP(value))
          return 0;
        else if (!(VOIDP(value))) {
          kno_decref(value);
          return 1;}}
      else return kno_err(WeirdOption,"kno_getopt",NULL,car);
      opts = KNO_CDR(opts);}
    else if (SYMBOLP(opts))
      if (KNO_EQ(key,opts))
        return 1;
      else return 0;
    else if (TABLEP(opts)) {
      lispval value = kno_get(opts,key,VOID);
      if (FALSEP(value))
        return 0;
      else if (VOIDP(value))
        return 0;
      else {
        kno_decref(value);
        return 1;}}
    else if ((NILP(opts))||(FALSEP(opts)))
      return 0;
    else return kno_err(WeirdOption,"kno_getopt",NULL,opts);}
  return 0;
}

KNO_EXPORT int kno_testopt(lispval opts,lispval key,lispval val)
{
  if (VOIDP(opts)) return 0;
  else if ((CHOICEP(opts)) || (PRECHOICEP(opts))) {
    DO_CHOICES(opt,opts) {
      if (kno_testopt(opt,key,val))
	return 1;}
    return 0;}
  else if (VOIDP(val))
    return boolopt(opts,key);
  else if (QCHOICEP(opts))
    return kno_testopt(KNO_XQCHOICE(opts)->qchoiceval,key,val);
  else if (EMPTYP(opts))
    return 0;
  else while (!(VOIDP(opts))) {
      if (PAIRP(opts)) {
        lispval car = KNO_CAR(opts);
        if (SYMBOLP(car)) {
          if ((KNO_EQ(key,car)) && (KNO_TRUEP(val)))
            return 1;}
        else if (PAIRP(car)) {
          if (KNO_EQ(KNO_CAR(car),key)) {
            if (KNO_EQUAL(val,KNO_CDR(car)))
              return 1;
            else return 0;}}
        else if (TABLEP(car)) {
          int tv = kno_test(car,key,val);
          if (tv) return tv;}
        else if (FALSEP(car)) {}
        else return kno_err(WeirdOption,"kno_getopt",NULL,car);
        opts = KNO_CDR(opts);}
      else if (SYMBOLP(opts))
        if (KNO_EQ(key,opts))
          if (KNO_TRUEP(val)) return 1;
          else return 0;
        else return 0;
      else if (TABLEP(opts))
        return kno_test(opts,key,val);
      else if ((NILP(opts))||(FALSEP(opts)))
        return 0;
      else return kno_err(WeirdOption,"kno_getopt",NULL,opts);}
  return 0;
}

KNO_EXPORT long long kno_getfixopt(lispval opts,u8_string name,long long dflt)
{
  lispval val=kno_getopt(opts,kno_intern(name),VOID);
  if (VOIDP(val))
    return dflt;
  else if (FIXNUMP(val))
    return FIX2INT(val);
  else if (KNO_BIGINTP(val))  {
    struct KNO_BIGINT *bi=(kno_bigint)val;
    if (kno_modest_bigintp(bi)) {
      long long llval=kno_bigint2int64(bi);
      return llval;}
    else {
      kno_decref(val);
      return dflt;}}
  else {
    kno_decref(val);
    return dflt;}
}

KNO_EXPORT lispval kno_merge_opts(lispval head,lispval tail)
{
  if ( (KNO_FALSEP(head)) || (KNO_VOIDP(head)) || (KNO_NILP(head)) )
    return kno_incref(tail);
  else if ( (KNO_FALSEP(tail)) || (KNO_VOIDP(tail)) || (KNO_NILP(tail)) )
    return kno_incref(head);
  else if ( (KNO_PAIRP(head)) && ( (KNO_CDR(head)) == tail) )
    return kno_incref(head);
  else if ( (KNO_PAIRP(tail)) && ( (KNO_CAR(tail)) == head ) )
    return kno_incref(tail);
  else {
    lispval combined = kno_make_pair(head,tail);
    return combined;}
}

void kno_init_getopt_c()
{
  u8_register_source_file(_FILEINFO);
}

