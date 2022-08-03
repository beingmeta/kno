/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_PROFILES_H
#define KNO_PROFILES_H 1
#ifndef KNO_PROFILES_H_INFO
#define KNO_PROFILES_H_INFO "include/kno/profiles.h"
#endif

#include <sys/resource.h>

#if HAVE_STDATOMIC_H
typedef struct KNO_PROFILE {
  u8_string prof_label;
  int prof_disabled;
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
  int prof_disabled;
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

KNO_EXPORT void kno_profile_record
(struct KNO_PROFILE *p,long long items,
 long long nsecs,long long nsecs_user,long long nsecs_system,
 long long n_waits,long long n_pauses,long long n_faults,
 int calls);

KNO_EXPORT struct KNO_PROFILE *kno_make_profile(u8_string name);

KNO_EXPORT void kno_profile_update(struct KNO_PROFILE *p,
				   struct rusage *before,
				   struct timespec *start,
				   int calls);
KNO_EXPORT void kno_profile_start(struct KNO_PROFILE *p,
                                  struct rusage *before,
				  struct timespec *start);

#endif /* KNO_PROFILES_H */
