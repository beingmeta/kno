/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/cons.h"

static int string_compare(u8_string s1,u8_string s2,int ci);

KNO_EXPORT
/* lispval_compare:
   Arguments: two dtype pointers
   Returns: 1, 0, or -1 (an int)
   Returns a function corresponding to a generic sort of two dtype pointers. */
int lispval_compare(lispval x,lispval y,kno_compare_flags flags)
{
  int quick = (flags == KNO_COMPARE_QUICK);
  int compare_atomic = (!((flags&KNO_COMPARE_CODES)));
  int compare_lengths = (!((flags&KNO_COMPARE_RECURSIVE)));
  int natural_sort = (flags&KNO_COMPARE_NATSORT);
  int lexical = (flags&KNO_COMPARE_ALPHABETICAL);
  int nocase = (flags&KNO_COMPARE_CI);

  /* This is just defined for this function */
#define DOCOMPARE(x,y)                                          \
  (((quick)&&(x == y)) ? (0) : (LISP_COMPARE(x,y,flags)))

  if (x == y) return 0;
  else if ((OIDP(x))&&(OIDP(y))) {
    if (compare_atomic) {
      if (x<y) return -1; else if (x>y) return 1; else return 0;}
    else if ((KNO_OID_BASE_ID(x)) == (KNO_OID_BASE_ID(y))) {
      unsigned int ox = KNO_OID_BASE_OFFSET(x);
      unsigned int oy = KNO_OID_BASE_OFFSET(y);
      /* The zero case is just x == y above */
      if (ox>oy) return 1;
      else return -1;}
    else {
      KNO_OID xaddr = KNO_OID_ADDR(x), yaddr = KNO_OID_ADDR(y);
      return KNO_OID_COMPARE(xaddr,yaddr);}}
  else if ((natural_sort)&&(SYMBOLP(x))&&(SYMBOLP(y))) {
    u8_string xname = SYM_NAME(x), yname = SYM_NAME(y);
    if ((compare_lengths)&&(!(lexical))) {
      size_t xlen = strlen(xname), ylen = strlen(yname);
      if (xlen>ylen) return 1;
      else if (xlen<ylen) return -1;
      else return string_compare(xname,yname,nocase);}
    else return string_compare(xname,yname,nocase);}
  else if ((compare_atomic) && (ATOMICP(x)))
    if (ATOMICP(y)) {
      if (x>y) return 1; else if (x<y) return -1; else return 0;}
    else return -1;
  else if ((compare_atomic) && (ATOMICP(y))) return 1;
  else if ((FIXNUMP(x)) && (FIXNUMP(y))) {
    long long xval = FIX2INT(x), yval = FIX2INT(y);
    /* The == case is handled by the x == y above. */
    if (xval>yval) return 1; else return -1;}
  else {
    kno_lisp_type xtype = KNO_TYPEOF(x);
    kno_lisp_type ytype = KNO_TYPEOF(y);
    if (KNO_NUMBER_TYPEP(xtype))
      if (KNO_NUMBER_TYPEP(ytype))
        return kno_numcompare(x,y);
      else return -1;
    else if (KNO_NUMBER_TYPEP(ytype))
      return 1;
    else if ((PRECHOICEP(x))&&(PRECHOICEP(y))) {
      lispval sx = kno_make_simple_choice(x);
      lispval sy = kno_make_simple_choice(y);
      int retval = DOCOMPARE(sx,sy);
      kno_decref(sx); kno_decref(sy);
      return retval;}
    else if (PRECHOICEP(x)) {
      lispval sx = kno_make_simple_choice(x);
      int retval = DOCOMPARE(sx,y);
      kno_decref(sx);
      return retval;}
    else if (PRECHOICEP(y)) {
      lispval sy = kno_make_simple_choice(y);
      int retval = DOCOMPARE(x,sy);
      kno_decref(sy);
      return retval;}
    else if (xtype>ytype) return 1;
    else if (xtype<ytype) return -1;
    else if (CONSP(x))
      switch (xtype) {
      case kno_pair_type: {
        int car_cmp = DOCOMPARE(KNO_CAR(x),KNO_CAR(y));
        if (car_cmp == 0) return (DOCOMPARE(KNO_CDR(x),KNO_CDR(y)));
        else return car_cmp;}
      case kno_string_type: {
        int xlen = STRLEN(x), ylen = STRLEN(y);
        if ((compare_lengths)&&(!(lexical))) {
          if (xlen>ylen) return 1; else if (xlen<ylen) return -1;
          else return string_compare(CSTRING(x),CSTRING(y),nocase);}
        else return string_compare(CSTRING(x),CSTRING(y),nocase);}
      case kno_packet_type: case kno_secret_type: {
        int xlen = KNO_PACKET_LENGTH(x), ylen = KNO_PACKET_LENGTH(y);
        if ((quick)||(compare_lengths)) {
          if (xlen>ylen) return 1; else if (xlen<ylen) return -1;}
        return memcmp(KNO_PACKET_DATA(x),KNO_PACKET_DATA(y),xlen);}
      case kno_vector_type: {
        int i = 0, xlen = VEC_LEN(x), ylen = VEC_LEN(y), lim;
        lispval *xdata = VEC_DATA(x), *ydata = VEC_DATA(y);
        if (quick) {
          if (xlen>ylen) return 1; else if (xlen<ylen) return -1;}
        if (xlen<ylen) lim = xlen; else lim = ylen;
        while (i < lim) {
          int cmp = DOCOMPARE(xdata[i],ydata[i]);
          if (cmp) return cmp; else i++;}
        if (xlen>ylen) return 1;
        else if (xlen<ylen) return -1;
        else return 0;}
      case kno_choice_type: {
        struct KNO_CHOICE *xc = kno_consptr(struct KNO_CHOICE *,x,kno_choice_type);
        struct KNO_CHOICE *yc = kno_consptr(struct KNO_CHOICE *,y,kno_choice_type);
        size_t xlen = KNO_XCHOICE_SIZE(xc), ylen = KNO_XCHOICE_SIZE(yc);
        if ((compare_lengths) && (xlen>ylen))
          return 1;
        else if ((compare_lengths) && (xlen<ylen))
          return -1;
        else {
          int cmp;
          const lispval *xscan, *yscan, *xlim;
          lispval _xnatsorted[17], *xnatsorted=_xnatsorted;
          lispval _ynatsorted[17], *ynatsorted=_ynatsorted;
          if (natural_sort) {
            xnatsorted = kno_natsort_choice(xc,_xnatsorted,17);
            ynatsorted = kno_natsort_choice(yc,_ynatsorted,17);
            xscan = (const lispval *)xnatsorted;
            yscan = (const lispval *)ynatsorted;}
          else {
            xscan = KNO_XCHOICE_DATA(xc);
            yscan = KNO_XCHOICE_DATA(yc);}
          if (ylen<xlen) xlim = xscan+ylen;
          else xlim = xscan+xlen;
          while (xscan<xlim) {
            cmp = DOCOMPARE(*xscan,*yscan);
            if (cmp) break;
            xscan++;
            yscan++;}
          if (xnatsorted!=_xnatsorted) u8_free(xnatsorted);
          if (ynatsorted!=_ynatsorted) u8_free(ynatsorted);
          if (xscan<xlim) return cmp;
          else if (xlen>ylen) return 1;
          else if (xlen<ylen) return -1;
          else return 0;}}
      default: {
        kno_lisp_type ctype = KNO_CONS_TYPE(KNO_CONS_DATA(x));
        if (kno_comparators[ctype])
          return kno_comparators[ctype](x,y,flags);
        else if (x>y) return 1;
        else if (x<y) return -1;
        else return 0;}}
    else if (x<y) return -1;
    else return 1;}

#undef DOCOMPARE
}

static int string_compare(u8_string s1,u8_string s2,int ci)
{
  if (ci) {
    int rv = strcasecmp(s1,s2);
    if (rv) return rv;
    else return strcmp(s1,s2);}
  else return strcmp(s1,s2);
}

/* Sorting DTYPEs based on a set of comparison flags */

KNO_FASTOP void do_swap(lispval *a,lispval *b)
{
  lispval tmp = *a;
  *a = *b;
  *b = tmp;
}

KNO_EXPORT
/* lispval_sort:
   Arguments: a vector of dtypes, a length, and a comparison flag value
   Returns: 1, 0, or -1 (an int)
   Returns a function corresponding to a generic sort of two dtype pointers. */
void lispval_sort(lispval *v,size_t n,kno_compare_flags flags)
{
  size_t i, j, ln, rn;
  while (n > 1) {
    do_swap(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (LISP_COMPARE(v[j],v[0],flags)>0);
      do ++i; while (i < j && (LISP_COMPARE(v[i],v[0],flags)<0));
      if (i >= j) break; else {}
      do_swap(&v[i], &v[j]);}
    do_swap(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      lispval_sort(v, ln, flags); v += j; n = rn;}
    else {lispval_sort(v + j, rn, flags); n = ln;}}
}

static int compare_compounds(lispval x,lispval y,kno_compare_flags flags)
{
  struct KNO_COMPOUND *xc = kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type);
  struct KNO_COMPOUND *yc = kno_consptr(struct KNO_COMPOUND *,y,kno_compound_type);
  lispval xtag = xc->typetag, ytag = yc->typetag;
  int cmp;
  if (xc == yc) return 0;
  else if ((xc->compound_isopaque) || (yc->compound_isopaque))
    if (xc>yc) return 1; else return -1;
  else if ((cmp = (LISP_COMPARE(xtag,ytag,flags))))
    return cmp;
  else if (xc->compound_length<yc->compound_length) return -1;
  else if (xc->compound_length>yc->compound_length) return 1;
  else {
    int i = 0, len = xc->compound_length;
    lispval *xdata = &(xc->compound_0), *ydata = &(yc->compound_0);
    while (i<len)
      if ((cmp = (LISP_COMPARE(xdata[i],ydata[i],flags)))==0)
        i++;
      else return cmp;
    return 0;}
}

static int compare_timestamps(lispval x,lispval y,kno_compare_flags flags)
{
  struct KNO_TIMESTAMP *xtm=
    kno_consptr(struct KNO_TIMESTAMP *,x,kno_timestamp_type);
  struct KNO_TIMESTAMP *ytm=
    kno_consptr(struct KNO_TIMESTAMP *,y,kno_timestamp_type);
  double diff = u8_xtime_diff(&(xtm->u8xtimeval),&(ytm->u8xtimeval));
  if (diff<0.0) return -1;
  else if (diff == 0.0) return 0;
  else return 1;
}

static int compare_uuids(lispval x,lispval y,kno_compare_flags flags)
{
  struct KNO_UUID *xuuid = kno_consptr(struct KNO_UUID *,x,kno_uuid_type);
  struct KNO_UUID *yuuid = kno_consptr(struct KNO_UUID *,y,kno_uuid_type);
  return memcmp(xuuid->uuid16,yuuid->uuid16,16);
}

static int compare_regex(lispval x,lispval y,kno_compare_flags flags)
{
  struct KNO_REGEX *xrx = kno_consptr(struct KNO_REGEX *,x,kno_regex_type);
  struct KNO_REGEX *yrx = kno_consptr(struct KNO_REGEX *,y,kno_regex_type);
  int scmp = strcmp(xrx->rxsrc,yrx->rxsrc);
  if (scmp) return scmp;
  if (xrx->rxflags == yrx->rxflags)
    return 0;
  else if (xrx->rxflags < yrx->rxflags)
    return -1;
  else return 1;
}

lispval compare_quick, compare_recursive, compare_elts, compare_natural,
  compare_natsort, compare_numeric, compare_lexical, compare_alphabetical,
  compare_ci, compare_ic, compare_caseinsensitive, compare_case_insensitive,
  compare_nocase, compare_full;

KNO_EXPORT kno_compare_flags kno_get_compare_flags(lispval spec)
{
  if ((VOIDP(spec))||(FALSEP(spec)))
    return KNO_COMPARE_CODES;
  else if (KNO_TRUEP(spec))
    return KNO_COMPARE_FULL;
  else if (TABLEP(spec)) {
    kno_compare_flags flags = 0;
    if (kno_testopt(spec,compare_full,VOID))
      flags = KNO_COMPARE_FULL;
    else if (kno_testopt(spec,compare_quick,VOID))
      flags = 0;
    else {}
    if ( (kno_testopt(spec,compare_recursive,VOID)) ||
         (kno_testopt(spec,compare_elts,VOID)) )
      flags |= KNO_COMPARE_RECURSIVE;
    if ( (kno_testopt(spec,compare_natural,VOID)) ||
         (kno_testopt(spec,compare_natsort,VOID)) )
      flags |= KNO_COMPARE_NATSORT;
    if ( (kno_testopt(spec,compare_numeric,VOID)) )
      flags |= KNO_COMPARE_NUMERIC;
    if ( (kno_testopt(spec,compare_lexical,VOID)) ||
         (kno_testopt(spec,compare_alphabetical,VOID)))
      flags |= KNO_COMPARE_ALPHABETICAL;
    if ( (kno_testopt(spec,compare_ci,VOID)) ||
         (kno_testopt(spec,compare_ic,VOID)) ||
         (kno_testopt(spec,compare_nocase,VOID)) ||
         (kno_testopt(spec,compare_caseinsensitive,VOID)) ||
         (kno_testopt(spec,compare_case_insensitive,VOID)) )
      flags |= KNO_COMPARE_CI;
    return flags;}
  else return KNO_COMPARE_QUICK;
}

void kno_init_compare_c()
{
  u8_register_source_file(_FILEINFO);

  kno_comparators[kno_compound_type]=compare_compounds;
  kno_comparators[kno_timestamp_type]=compare_timestamps;
  kno_comparators[kno_uuid_type]=compare_uuids;
  kno_comparators[kno_regex_type]=compare_regex;

  compare_quick = kno_intern("quick");
  compare_recursive = kno_intern("recursive");
  compare_elts = kno_intern("elts");
  compare_natural = kno_intern("natural");
  compare_natsort = kno_intern("natsort");
  compare_numeric = kno_intern("numeric");
  compare_lexical = kno_intern("lexical");
  compare_alphabetical = kno_intern("alphabetical");
  compare_ci = kno_intern("ci");
  compare_ic = kno_intern("ic");
  compare_caseinsensitive = kno_intern("caseinsensitive");
  compare_case_insensitive = kno_intern("case-insensitive");
  compare_nocase = kno_intern("nocase");
  compare_full = kno_intern("full");
}

