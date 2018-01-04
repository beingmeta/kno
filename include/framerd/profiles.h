/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_PROFILES_H
#define FRAMERD_PROFILES_H 1
#ifndef FRAMERD_PROFILES_H_INFO
#define FRAMERD_PROFILES_H_INFO "include/framerd/profiles.h"
#endif

#if HAVE_STDATOMIC_H
typedef struct FD_PROFILE {
  u8_string prof_label;
  _Atomic long long prof_calls;
  _Atomic long long prof_nsecs;
  _Atomic long long prof_items;} *fd_profile;
#else
typedef struct FD_PROFILE {
  u8_string prof_label;
  long long prof_calls;
  long long prof_nsecs;
  long long prof_items;
  u8_mutex prof_lock;} *fd_profile;
#endif

#if HAVE_STDATOMIC_H
U8_MAYBE_UNUSED static void fd_profile_call
(struct FD_PROFILE *p,long long nsecs,long long items)
{
  if (items) atomic_fetch_add(&(p->prof_items),items);
  atomic_fetch_add(&(p->prof_nsecs),nsecs);
  atomic_fetch_add(&(p->prof_calls),1);
}
#else
static void fd_profile_call
(struct FD_PROFILE *p,long long nsecs,long long items)
{
  u8_lock_mutex(&(p->prof_lock));
  if (items) p->prof_items += items;
  p->prof_nsecs += nsecs;
  p->prof_calls++;
}
#endif

static struct FD_PROFILE *fd_make_profile(u8_string name)
{
  struct FD_PROFILE *result = u8_alloc(struct FD_PROFILE);
  result->prof_label = (name) ? (u8_strdup(name)) : (NULL);
#if HAVE_STDATOMIC_H
  result->prof_calls = ATOMIC_VAR_INIT(0);
  result->prof_items = ATOMIC_VAR_INIT(0);
  result->prof_nsecs = ATOMIC_VAR_INIT(0);
#else
  u8_init_mutex(&(result->prof_lock));
#endif
  return result;
}

#endif /* FRAMERD_PROFILES_H */
