/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_PROFILES_H
#define KNO_PROFILES_H 1
#ifndef KNO_PROFILES_H_INFO
#define KNO_PROFILES_H_INFO "include/kno/profiles.h"
#endif

#if HAVE_STDATOMIC_H
typedef struct KNO_PROFILE {
  u8_string prof_label;
  _Atomic long long prof_calls;
  _Atomic long long prof_items;
  _Atomic long long prof_nsecs;
  _Atomic long long prof_nsecs_user;
  _Atomic long long prof_nsecs_system;
  _Atomic long long prof_n_waits;
  _Atomic long long prof_n_pauses;
  _Atomic long long prof_n_faults;} *kno_profile;
#else
typedef struct KNO_PROFILE {
  u8_string prof_label;
  long long prof_calls;
  long long prof_items;
  long long prof_nsecs;
  long long prof_nsecs_user;
  long long prof_nsecs_system;
  long long prof_n_waits;
  long long prof_n_pauses;
  long long prof_n_faults;
  u8_mutex prof_lock;} *kno_profile;
#endif

#if HAVE_STDATOMIC_H
U8_MAYBE_UNUSED static void kno_profile_record
(struct KNO_PROFILE *p,long long items,
 long long nsecs,long long nsecs_user,long long nsecs_system,
 long long n_waits,long long n_pauses,long long n_faults)
{
  if (items) atomic_fetch_add(&(p->prof_items),items);
  atomic_fetch_add(&(p->prof_calls),1);
  atomic_fetch_add(&(p->prof_nsecs),nsecs);
#if KNO_EXTENDED_PROFILING
  atomic_fetch_add(&(p->prof_nsecs_user),nsecs_user);
  atomic_fetch_add(&(p->prof_nsecs_system),nsecs_system);
  atomic_fetch_add(&(p->prof_n_waits),n_waits);
  atomic_fetch_add(&(p->prof_n_pauses),n_pauses);
  atomic_fetch_add(&(p->prof_n_faults),n_faults);
#endif
}
#else
static void kno_profile_record
(struct KNO_PROFILE *p,long long items,
 long long nsecs,long long nsecs_user,long long nsecs_system,
 long long n_waits,long long n_pauses,long long n_faults)
{
  u8_lock_mutex(&(p->prof_lock));
  if (items) p->prof_items += items;
  p->prof_calls++;
  p->prof_nsecs += nsecs;
#if KNO_EXTENDED_PROFILING
  p->prof_nsecs_user += nsecs_user;
  p->prof_nsecs_system += nsecs_system;
  p->prof_n_waits += n_waits;
  p->prof_n_pauses += n_pauses;
  p->prof_n_nfaults += n_faults;
#endif
  u8_lock_mutex(&(p->prof_lock));
}
#endif

static U8_MAYBE_UNUSED struct KNO_PROFILE *kno_make_profile(u8_string name)
{
  struct KNO_PROFILE *result = u8_alloc(struct KNO_PROFILE);
  result->prof_label = (name) ? (u8_strdup(name)) : (NULL);
#if HAVE_STDATOMIC_H
  result->prof_calls        = ATOMIC_VAR_INIT(0);
  result->prof_items        = ATOMIC_VAR_INIT(0);
  result->prof_nsecs        = ATOMIC_VAR_INIT(0);
#if KNO_EXTENDED_PROFILING
  result->prof_nsecs_user   = ATOMIC_VAR_INIT(0);
  result->prof_nsecs_system = ATOMIC_VAR_INIT(0);
  result->prof_n_waits      = ATOMIC_VAR_INIT(0);
  result->prof_n_pauses      = ATOMIC_VAR_INIT(0);
  result->prof_n_faults     = ATOMIC_VAR_INIT(0);
#endif
#else
  u8_init_mutex(&(result->prof_lock));
#endif
  return result;
}

#endif /* KNO_PROFILES_H */
