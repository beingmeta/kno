/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_SORTING_H
#define KNO_SORTING_H 1
#ifndef KNO_SORTING_H_INFO
#define KNO_SORTING_H_INFO "include/kno/sorting.h"
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



typedef int (*kno_sortfn)(const void *x,const void *y);

struct KNO_SORT_ENTRY {
  lispval sortval, sortkey;};

U8_MAYBE_UNUSED static int _kno_sort_helper(const void *vx,const void *vy)
{
  const struct KNO_SORT_ENTRY *sx = (struct KNO_SORT_ENTRY *)vx;
  const struct KNO_SORT_ENTRY *sy = (struct KNO_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey) return 0;
  else {
    kno_ptr_type xtype = KNO_PTR_TYPE(sx->sortkey);
    kno_ptr_type ytype = KNO_PTR_TYPE(sy->sortkey);
    if (xtype == ytype)
      if (KNO_OIDP(sx->sortkey)) {
        KNO_OID xaddr = KNO_OID_ADDR(sx->sortkey);
        KNO_OID yaddr = KNO_OID_ADDR(sy->sortkey);
        return KNO_OID_COMPARE(xaddr,yaddr);}
      else if (KNO_FIXNUMP(sx->sortkey)) {
        long long xval = KNO_FIX2INT(sx->sortkey);
        long long yval = KNO_FIX2INT(sy->sortkey);
        if (xval<yval) return -1; else return 1;}
      else return KNO_FULL_COMPARE(sx->sortkey,sy->sortkey);
    else if ((xtype == kno_fixnum_type) || (xtype == kno_bigint_type) ||
             (xtype == kno_flonum_type) || (xtype == kno_rational_type) ||
             (xtype == kno_complex_type)) {
      lispval x = sx->sortkey, y = sy->sortkey;
      if ( ((xtype == kno_fixnum_type) && (xtype == kno_bigint_type)) ||
           ((xtype == kno_bigint_type) && (xtype == kno_fixnum_type)) ) {
        int xneg = ((xtype == kno_fixnum_type) ? (KNO_FIX2INT(x)<0) :
                    (kno_bigint_negativep((kno_bigint)x)));
        int yneg = ((ytype == kno_fixnum_type) ? (KNO_FIX2INT(y)<0) :
                    (kno_bigint_negativep((kno_bigint)y)));
        if ((xneg == 0) && (yneg == 0)) {
          if (xtype == kno_fixnum_type)
            return -1;
          else return 1;}
        else if ((xneg) && (yneg)){
          if (xtype == kno_fixnum_type)
            return 1;
          else return -11;}
        else if (xneg)
          return -1;
        else return 1;}
      else if ((ytype == kno_fixnum_type) || (ytype == kno_bigint_type) ||
               (ytype == kno_flonum_type) || (ytype == kno_rational_type) ||
               (ytype == kno_complex_type)) {
        int cmp = kno_numcompare(sx->sortkey,sy->sortkey);
        if (cmp) return cmp;
        else if (xtype<ytype) return -1;
        else return 1;}
      else return -1;}
    else if ((ytype == kno_fixnum_type) || (ytype == kno_bigint_type) ||
             (ytype == kno_flonum_type) || (ytype == kno_rational_type) ||
             (ytype == kno_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

U8_MAYBE_UNUSED static int _kno_pointer_sort_helper(const void *vx,const void *vy)
{
  const struct KNO_SORT_ENTRY *sx = (struct KNO_SORT_ENTRY *)vx;
  const struct KNO_SORT_ENTRY *sy = (struct KNO_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey)
    return 0;
  else if (sx->sortkey < sy->sortkey)
    return -1;
  else return 1;
}

U8_MAYBE_UNUSED static int _kno_collate_helper(const void *vx,const void *vy)
{
  const struct KNO_SORT_ENTRY *sx = (struct KNO_SORT_ENTRY *)vx;
  const struct KNO_SORT_ENTRY *sy = (struct KNO_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey) return 0;
  else {
    kno_ptr_type xtype = KNO_PTR_TYPE(sx->sortkey);
    kno_ptr_type ytype = KNO_PTR_TYPE(sy->sortkey);
    if (xtype == ytype)
      if (KNO_OIDP(sx->sortkey)) {
        KNO_OID xaddr = KNO_OID_ADDR(sx->sortkey);
        KNO_OID yaddr = KNO_OID_ADDR(sy->sortkey);
        return KNO_OID_COMPARE(xaddr,yaddr);}
      else if (KNO_FIXNUMP(sx->sortkey)) {
        long long xval = KNO_FIX2INT(sx->sortkey);
        long long yval = KNO_FIX2INT(sy->sortkey);
        if (xval<yval) return -1; else return 1;}
      else if (xtype == kno_string_type)
        return (strcoll(KNO_CSTRING(sx->sortkey),
                        KNO_CSTRING(sy->sortkey)));
      else if (xtype == kno_symbol_type)
        return (strcoll(KNO_XSYMBOL_NAME(sx->sortkey),
                        KNO_XSYMBOL_NAME(sy->sortkey)));
      else return KNO_FULL_COMPARE(sx->sortkey,sy->sortkey);
    else if ((xtype == kno_fixnum_type) || (xtype == kno_bigint_type) ||
              (xtype == kno_flonum_type) || (xtype == kno_rational_type) ||
              (xtype == kno_complex_type))
      if ((ytype == kno_fixnum_type) || (ytype == kno_bigint_type) ||
          (ytype == kno_flonum_type) || (ytype == kno_rational_type) ||
          (ytype == kno_complex_type)) {
        int cmp = kno_numcompare(sx->sortkey,sy->sortkey);
        if (cmp) return cmp;
        else if (xtype<ytype) return -1;
        else return 1;}
      else return -1;
    else if ((ytype == kno_fixnum_type) || (ytype == kno_bigint_type) ||
             (ytype == kno_flonum_type) || (ytype == kno_rational_type) ||
             (ytype == kno_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

U8_MAYBE_UNUSED static int _kno_lexsort_helper(const void *vx,const void *vy)
{
  const struct KNO_SORT_ENTRY *sx = (struct KNO_SORT_ENTRY *)vx;
  const struct KNO_SORT_ENTRY *sy = (struct KNO_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey) return 0;
  else {
    kno_ptr_type xtype = KNO_PTR_TYPE(sx->sortkey);
    kno_ptr_type ytype = KNO_PTR_TYPE(sy->sortkey);
    if (xtype == ytype)
      if (KNO_OIDP(sx->sortkey)) {
        KNO_OID xaddr = KNO_OID_ADDR(sx->sortkey);
        KNO_OID yaddr = KNO_OID_ADDR(sy->sortkey);
        return KNO_OID_COMPARE(xaddr,yaddr);}
      else if (KNO_FIXNUMP(sx->sortkey)) {
        long long xval = KNO_FIX2INT(sx->sortkey);
        long long yval = KNO_FIX2INT(sy->sortkey);
        if (xval<yval) return -1; else return 1;}
      else if (xtype == kno_string_type)
        return (u8_strcmp(KNO_CSTRING(sx->sortkey),
                          KNO_CSTRING(sy->sortkey),0));
      else if (xtype == kno_symbol_type)
        return (u8_strcmp(KNO_XSYMBOL_NAME(sx->sortkey),
                          KNO_XSYMBOL_NAME(sy->sortkey),0));
      else return KNO_FULL_COMPARE(sx->sortkey,sy->sortkey);
    else if ((xtype == kno_fixnum_type) || (xtype == kno_bigint_type) ||
              (xtype == kno_flonum_type) || (xtype == kno_rational_type) ||
              (xtype == kno_complex_type))
      if ((ytype == kno_fixnum_type) || (ytype == kno_bigint_type) ||
          (ytype == kno_flonum_type) || (ytype == kno_rational_type) ||
          (ytype == kno_complex_type)) {
        int cmp = kno_numcompare(sx->sortkey,sy->sortkey);
        if (cmp) return cmp;
        else if (xtype<ytype) return -1;
        else return 1;}
      else return -1;
    else if ((ytype == kno_fixnum_type) || (ytype == kno_bigint_type) ||
             (ytype == kno_flonum_type) || (ytype == kno_rational_type) ||
             (ytype == kno_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

U8_MAYBE_UNUSED static int _kno_lexsort_ci_helper(const void *vx,const void *vy)
{
  const struct KNO_SORT_ENTRY *sx = (struct KNO_SORT_ENTRY *)vx;
  const struct KNO_SORT_ENTRY *sy = (struct KNO_SORT_ENTRY *)vy;
  if (sx->sortkey == sy->sortkey) return 0;
  else {
    kno_ptr_type xtype = KNO_PTR_TYPE(sx->sortkey);
    kno_ptr_type ytype = KNO_PTR_TYPE(sy->sortkey);
    if (xtype == ytype)
      if (KNO_OIDP(sx->sortkey)) {
        KNO_OID xaddr = KNO_OID_ADDR(sx->sortkey);
        KNO_OID yaddr = KNO_OID_ADDR(sy->sortkey);
        return KNO_OID_COMPARE(xaddr,yaddr);}
      else if (KNO_FIXNUMP(sx->sortkey)) {
        long long xval = KNO_FIX2INT(sx->sortkey);
        long long yval = KNO_FIX2INT(sy->sortkey);
        if (xval<yval) return -1; else return 1;}
      else if (xtype == kno_string_type)
        return (u8_strcmp(KNO_CSTRING(sx->sortkey),
                          KNO_CSTRING(sy->sortkey),
                          U8_STRCMP_CI));
      else if (xtype == kno_symbol_type)
        return (u8_strcmp(KNO_XSYMBOL_NAME(sx->sortkey),
                          KNO_XSYMBOL_NAME(sy->sortkey),
                          U8_STRCMP_CI));
      else return KNO_FULL_COMPARE(sx->sortkey,sy->sortkey);
    else if ((xtype == kno_fixnum_type) || (xtype == kno_bigint_type) ||
              (xtype == kno_flonum_type) || (xtype == kno_rational_type) ||
              (xtype == kno_complex_type))
      if ((ytype == kno_fixnum_type) || (ytype == kno_bigint_type) ||
          (ytype == kno_flonum_type) || (ytype == kno_rational_type) ||
          (ytype == kno_complex_type)) {
        int cmp = kno_numcompare(sx->sortkey,sy->sortkey);
        if (cmp) return cmp;
        else if (xtype<ytype) return -1;
        else return 1;}
      else return -1;
    else if ((ytype == kno_fixnum_type) || (ytype == kno_bigint_type) ||
             (ytype == kno_flonum_type) || (ytype == kno_rational_type) ||
             (ytype == kno_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

U8_MAYBE_UNUSED static lispval _kno_apply_keyfn(lispval x,lispval keyfn)
{
  if ((KNO_VOIDP(keyfn)) ||
      (KNO_EMPTY_CHOICEP(keyfn)) ||
      (KNO_FALSEP(keyfn)) ||
      (KNO_DEFAULTP(keyfn)))
    return kno_incref(x);
  else if (KNO_OIDP(keyfn)) return kno_frame_get(x,keyfn);
  else if (KNO_TABLEP(keyfn)) return kno_get(keyfn,x,KNO_EMPTY_CHOICE);
  else if (KNO_APPLICABLEP(keyfn)) {
    lispval result = kno_apply(keyfn,1,&x);
    return kno_finish_call(result);}
  else if (KNO_VECTORP(keyfn)) {
    int i = 0, len = KNO_VECTOR_LENGTH(keyfn);
    lispval *keyfns = KNO_VECTOR_DATA(keyfn);
    lispval *vecdata = u8_alloc_n(len,lispval);
    while (i<len) {vecdata[i]=_kno_apply_keyfn(x,keyfns[i]); i++;}
    return kno_wrap_vector(len,vecdata);}
  else if ((KNO_OIDP(x)) && (KNO_SYMBOLP(keyfn)))
    return kno_frame_get(x,keyfn);
  else if (KNO_TABLEP(x))
    return kno_get(x,keyfn,KNO_EMPTY_CHOICE);
  else return KNO_EMPTY_CHOICE;
}

#endif /* KNO_SORTING_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
