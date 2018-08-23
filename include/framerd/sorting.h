/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_SORTING_H
#define FRAMERD_SORTING_H 1
#ifndef FRAMERD_SORTING_H_INFO
#define FRAMERD_SORTING_H_INFO "include/framerd/sorting.h"
#endif

#include <libu8/u8strcmp.h>

#ifndef HAVE_U8_STRCMP
static int _u8_strcmp(u8_string s1,u8_string s2,int cmpflags)
{
  u8_string scan1 = s1, scan2 = s2;
  while ( (*scan1) && (*scan2) ) {
    if ( (*scan1 < 0x80 ) && (*scan2 < 0x80 ) ) {
      int c1, c2;
      if (cmpflags & U8_STRCMP_CI) {
	c1 = u8_tolower(*scan1);
	c2 = u8_tolower(*scan2);}
      else {
	c1 = *scan1;
	c2 = *scan2;}
      if (c1 < c2)
	return -1;
      else if (c1 > c2)
	return 1;
      else {}
      scan1++;
      scan2++;}
    else {
      int c1 = u8_sgetc(&scan1);
      int c2 = u8_sgetc(&scan2);
      if (cmpflags & U8_STRCMP_CI) {
	c1 = u8_tolower(c1);
	c2 = u8_tolower(c2);}
      if (c1<c2)
	return -1;
      else if (c1>c2)
	return 1;
      else {}}}
  if (*scan1)
    return 1;
  else if (*scan2)
    return -1;
  else return 0;
}

#define u8_strcmp _u8_strcmp
#endif



typedef int (*fd_sortfn)(const void *x,const void *y);

struct FD_SORT_ENTRY {
  lispval sortval, sortkey;};

U8_MAYBE_UNUSED static int _fd_sort_helper(const void *vx,const void *vy)
{
  const struct FD_SORT_ENTRY *sx = (struct FD_SORT_ENTRY *)vx;
  const struct FD_SORT_ENTRY *sy = (struct FD_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey) return 0;
  else {
    fd_ptr_type xtype = FD_PTR_TYPE(sx->sortkey);
    fd_ptr_type ytype = FD_PTR_TYPE(sy->sortkey);
    if (xtype == ytype)
      if (FD_OIDP(sx->sortkey)) {
        FD_OID xaddr = FD_OID_ADDR(sx->sortkey);
        FD_OID yaddr = FD_OID_ADDR(sy->sortkey);
        return FD_OID_COMPARE(xaddr,yaddr);}
      else if (FD_FIXNUMP(sx->sortkey)) {
        long long xval = FD_FIX2INT(sx->sortkey);
        long long yval = FD_FIX2INT(sy->sortkey);
        if (xval<yval) return -1; else return 1;}
      else return FD_FULL_COMPARE(sx->sortkey,sy->sortkey);
    else if ((xtype == fd_fixnum_type) || (xtype == fd_bigint_type) ||
             (xtype == fd_flonum_type) || (xtype == fd_rational_type) ||
             (xtype == fd_complex_type)) {
      lispval x = sx->sortkey, y = sy->sortkey;
      if ( ((xtype == fd_fixnum_type) && (xtype == fd_bigint_type)) ||
           ((xtype == fd_bigint_type) && (xtype == fd_fixnum_type)) ) {
        int xneg = ((xtype == fd_fixnum_type) ? (FD_FIX2INT(x)<0) :
                    (fd_bigint_negativep((fd_bigint)x)));
        int yneg = ((ytype == fd_fixnum_type) ? (FD_FIX2INT(y)<0) :
                    (fd_bigint_negativep((fd_bigint)y)));
        if ((xneg == 0) && (yneg == 0)) {
          if (xtype == fd_fixnum_type)
            return -1;
          else return 1;}
        else if ((xneg) && (yneg)){
          if (xtype == fd_fixnum_type)
            return 1;
          else return -11;}
        else if (xneg)
          return -1;
        else return 1;}
      else if ((ytype == fd_fixnum_type) || (ytype == fd_bigint_type) ||
               (ytype == fd_flonum_type) || (ytype == fd_rational_type) ||
               (ytype == fd_complex_type)) {
        int cmp = fd_numcompare(sx->sortkey,sy->sortkey);
        if (cmp) return cmp;
        else if (xtype<ytype) return -1;
        else return 1;}
      else return -1;}
    else if ((ytype == fd_fixnum_type) || (ytype == fd_bigint_type) ||
             (ytype == fd_flonum_type) || (ytype == fd_rational_type) ||
             (ytype == fd_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

U8_MAYBE_UNUSED static int _fd_pointer_sort_helper(const void *vx,const void *vy)
{
  const struct FD_SORT_ENTRY *sx = (struct FD_SORT_ENTRY *)vx;
  const struct FD_SORT_ENTRY *sy = (struct FD_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey)
    return 0;
  else if (sx->sortkey < sy->sortkey)
    return -1;
  else return 1;
}

U8_MAYBE_UNUSED static int _fd_collate_helper(const void *vx,const void *vy)
{
  const struct FD_SORT_ENTRY *sx = (struct FD_SORT_ENTRY *)vx;
  const struct FD_SORT_ENTRY *sy = (struct FD_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey) return 0;
  else {
    fd_ptr_type xtype = FD_PTR_TYPE(sx->sortkey);
    fd_ptr_type ytype = FD_PTR_TYPE(sy->sortkey);
    if (xtype == ytype)
      if (FD_OIDP(sx->sortkey)) {
        FD_OID xaddr = FD_OID_ADDR(sx->sortkey);
        FD_OID yaddr = FD_OID_ADDR(sy->sortkey);
        return FD_OID_COMPARE(xaddr,yaddr);}
      else if (FD_FIXNUMP(sx->sortkey)) {
        long long xval = FD_FIX2INT(sx->sortkey);
        long long yval = FD_FIX2INT(sy->sortkey);
        if (xval<yval) return -1; else return 1;}
      else if (xtype == fd_string_type)
        return (strcoll(FD_CSTRING(sx->sortkey),
                        FD_CSTRING(sy->sortkey)));
      else if (xtype == fd_symbol_type)
        return (strcoll(FD_XSYMBOL_NAME(sx->sortkey),
                        FD_XSYMBOL_NAME(sy->sortkey)));
      else return FD_FULL_COMPARE(sx->sortkey,sy->sortkey);
    else if ((xtype == fd_fixnum_type) || (xtype == fd_bigint_type) ||
              (xtype == fd_flonum_type) || (xtype == fd_rational_type) ||
              (xtype == fd_complex_type))
      if ((ytype == fd_fixnum_type) || (ytype == fd_bigint_type) ||
          (ytype == fd_flonum_type) || (ytype == fd_rational_type) ||
          (ytype == fd_complex_type)) {
        int cmp = fd_numcompare(sx->sortkey,sy->sortkey);
        if (cmp) return cmp;
        else if (xtype<ytype) return -1;
        else return 1;}
      else return -1;
    else if ((ytype == fd_fixnum_type) || (ytype == fd_bigint_type) ||
             (ytype == fd_flonum_type) || (ytype == fd_rational_type) ||
             (ytype == fd_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

U8_MAYBE_UNUSED static int _fd_lexsort_helper(const void *vx,const void *vy)
{
  const struct FD_SORT_ENTRY *sx = (struct FD_SORT_ENTRY *)vx;
  const struct FD_SORT_ENTRY *sy = (struct FD_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey) return 0;
  else {
    fd_ptr_type xtype = FD_PTR_TYPE(sx->sortkey);
    fd_ptr_type ytype = FD_PTR_TYPE(sy->sortkey);
    if (xtype == ytype)
      if (FD_OIDP(sx->sortkey)) {
        FD_OID xaddr = FD_OID_ADDR(sx->sortkey);
        FD_OID yaddr = FD_OID_ADDR(sy->sortkey);
        return FD_OID_COMPARE(xaddr,yaddr);}
      else if (FD_FIXNUMP(sx->sortkey)) {
        long long xval = FD_FIX2INT(sx->sortkey);
        long long yval = FD_FIX2INT(sy->sortkey);
        if (xval<yval) return -1; else return 1;}
      else if (xtype == fd_string_type)
        return (u8_strcmp(FD_CSTRING(sx->sortkey),
                          FD_CSTRING(sy->sortkey),0));
      else if (xtype == fd_symbol_type)
        return (u8_strcmp(FD_XSYMBOL_NAME(sx->sortkey),
                          FD_XSYMBOL_NAME(sy->sortkey),0));
      else return FD_FULL_COMPARE(sx->sortkey,sy->sortkey);
    else if ((xtype == fd_fixnum_type) || (xtype == fd_bigint_type) ||
              (xtype == fd_flonum_type) || (xtype == fd_rational_type) ||
              (xtype == fd_complex_type))
      if ((ytype == fd_fixnum_type) || (ytype == fd_bigint_type) ||
          (ytype == fd_flonum_type) || (ytype == fd_rational_type) ||
          (ytype == fd_complex_type)) {
        int cmp = fd_numcompare(sx->sortkey,sy->sortkey);
        if (cmp) return cmp;
        else if (xtype<ytype) return -1;
        else return 1;}
      else return -1;
    else if ((ytype == fd_fixnum_type) || (ytype == fd_bigint_type) ||
             (ytype == fd_flonum_type) || (ytype == fd_rational_type) ||
             (ytype == fd_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

U8_MAYBE_UNUSED static int _fd_lexsort_ci_helper(const void *vx,const void *vy)
{
  const struct FD_SORT_ENTRY *sx = (struct FD_SORT_ENTRY *)vx;
  const struct FD_SORT_ENTRY *sy = (struct FD_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey) return 0;
  else {
    fd_ptr_type xtype = FD_PTR_TYPE(sx->sortkey);
    fd_ptr_type ytype = FD_PTR_TYPE(sy->sortkey);
    if (xtype == ytype)
      if (FD_OIDP(sx->sortkey)) {
        FD_OID xaddr = FD_OID_ADDR(sx->sortkey);
        FD_OID yaddr = FD_OID_ADDR(sy->sortkey);
        return FD_OID_COMPARE(xaddr,yaddr);}
      else if (FD_FIXNUMP(sx->sortkey)) {
        long long xval = FD_FIX2INT(sx->sortkey);
        long long yval = FD_FIX2INT(sy->sortkey);
        if (xval<yval) return -1; else return 1;}
      else if (xtype == fd_string_type)
        return (u8_strcmp(FD_CSTRING(sx->sortkey),
                          FD_CSTRING(sy->sortkey),
                          U8_STRCMP_CI));
      else if (xtype == fd_symbol_type)
        return (u8_strcmp(FD_XSYMBOL_NAME(sx->sortkey),
                          FD_XSYMBOL_NAME(sy->sortkey),
                          U8_STRCMP_CI));
      else return FD_FULL_COMPARE(sx->sortkey,sy->sortkey);
    else if ((xtype == fd_fixnum_type) || (xtype == fd_bigint_type) ||
              (xtype == fd_flonum_type) || (xtype == fd_rational_type) ||
              (xtype == fd_complex_type))
      if ((ytype == fd_fixnum_type) || (ytype == fd_bigint_type) ||
          (ytype == fd_flonum_type) || (ytype == fd_rational_type) ||
          (ytype == fd_complex_type)) {
        int cmp = fd_numcompare(sx->sortkey,sy->sortkey);
        if (cmp) return cmp;
        else if (xtype<ytype) return -1;
        else return 1;}
      else return -1;
    else if ((ytype == fd_fixnum_type) || (ytype == fd_bigint_type) ||
             (ytype == fd_flonum_type) || (ytype == fd_rational_type) ||
             (ytype == fd_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

U8_MAYBE_UNUSED static lispval _fd_apply_keyfn(lispval x,lispval keyfn)
{
  if ((FD_VOIDP(keyfn)) ||
      (FD_EMPTY_CHOICEP(keyfn)) ||
      (FD_FALSEP(keyfn)) ||
      (FD_DEFAULTP(keyfn)))
    return fd_incref(x);
  else if (FD_OIDP(keyfn)) return fd_frame_get(x,keyfn);
  else if (FD_TABLEP(keyfn)) return fd_get(keyfn,x,FD_EMPTY_CHOICE);
  else if (FD_APPLICABLEP(keyfn)) {
    lispval result = fd_apply(keyfn,1,&x);
    return fd_finish_call(result);}
  else if (FD_VECTORP(keyfn)) {
    int i = 0, len = FD_VECTOR_LENGTH(keyfn);
    lispval *keyfns = FD_VECTOR_DATA(keyfn);
    lispval *vecdata = u8_alloc_n(len,lispval);
    while (i<len) {vecdata[i]=_fd_apply_keyfn(x,keyfns[i]); i++;}
    return fd_wrap_vector(len,vecdata);}
  else if ((FD_OIDP(x)) && (FD_SYMBOLP(keyfn)))
    return fd_frame_get(x,keyfn);
  else if (FD_TABLEP(x))
    return fd_get(x,keyfn,FD_EMPTY_CHOICE);
  else return FD_EMPTY_CHOICE;
}

#endif /* FRAMERD_SORTING_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
