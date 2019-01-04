/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/cons.h"

static int string_compare(u8_string s1,u8_string s2,int ci);

FD_EXPORT
/* lispval_compare:
    Arguments: two dtype pointers
    Returns: 1, 0, or -1 (an int)
  Returns a function corresponding to a generic sort of two dtype pointers. */
int lispval_compare(lispval x,lispval y,fd_compare_flags flags)
{
  int quick = (flags == FD_COMPARE_QUICK);
  int compare_atomic = (!((flags&FD_COMPARE_CODES)));
  int compare_lengths = (!((flags&FD_COMPARE_RECURSIVE)));
  int natural_sort = (flags&FD_COMPARE_NATSORT);
  int lexical = (flags&FD_COMPARE_ALPHABETICAL);
  int nocase = (flags&FD_COMPARE_CI);

  /* This is just defined for this function */
#define DOCOMPARE(x,y) \
  (((quick)&&(x == y)) ? (0) : (LISP_COMPARE(x,y,flags)))

  if (x == y) return 0;
  else if ((OIDP(x))&&(OIDP(y))) {
    if (compare_atomic) {
      if (x<y) return -1; else if (x>y) return 1; else return 0;}
    else if ((FD_OID_BASE_ID(x)) == (FD_OID_BASE_ID(y))) {
      unsigned int ox = FD_OID_BASE_OFFSET(x);
      unsigned int oy = FD_OID_BASE_OFFSET(y);
      /* The zero case is just x == y above */
      if (ox>oy) return 1;
      else return -1;}
    else {
      FD_OID xaddr = FD_OID_ADDR(x), yaddr = FD_OID_ADDR(y);
      return FD_OID_COMPARE(xaddr,yaddr);}}
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
    fd_ptr_type xtype = FD_PTR_TYPE(x);
    fd_ptr_type ytype = FD_PTR_TYPE(y);
    if (FD_NUMBER_TYPEP(xtype))
      if (FD_NUMBER_TYPEP(ytype))
        return fd_numcompare(x,y);
      else return -1;
    else if (FD_NUMBER_TYPEP(ytype))
      return 1;
    else if ((PRECHOICEP(x))&&(PRECHOICEP(y))) {
      lispval sx = fd_make_simple_choice(x);
      lispval sy = fd_make_simple_choice(y);
      int retval = DOCOMPARE(sx,sy);
      fd_decref(sx); fd_decref(sy);
      return retval;}
    else if (PRECHOICEP(x)) {
      lispval sx = fd_make_simple_choice(x);
      int retval = DOCOMPARE(sx,y);
      fd_decref(sx);
      return retval;}
    else if (PRECHOICEP(y)) {
      lispval sy = fd_make_simple_choice(y);
      int retval = DOCOMPARE(x,sy);
      fd_decref(sy);
      return retval;}
    else if (xtype>ytype) return 1;
    else if (xtype<ytype) return -1;
    else if (CONSP(x))
      switch (xtype) {
      case fd_pair_type: {
        int car_cmp = DOCOMPARE(FD_CAR(x),FD_CAR(y));
        if (car_cmp == 0) return (DOCOMPARE(FD_CDR(x),FD_CDR(y)));
        else return car_cmp;}
      case fd_string_type: {
        int xlen = STRLEN(x), ylen = STRLEN(y);
        if ((compare_lengths)&&(!(lexical))) {
          if (xlen>ylen) return 1; else if (xlen<ylen) return -1;
          else return string_compare(CSTRING(x),CSTRING(y),nocase);}
        else return string_compare(CSTRING(x),CSTRING(y),nocase);}
      case fd_packet_type: case fd_secret_type: {
        int xlen = FD_PACKET_LENGTH(x), ylen = FD_PACKET_LENGTH(y);
        if ((quick)||(compare_lengths)) {
          if (xlen>ylen) return 1; else if (xlen<ylen) return -1;}
        return memcmp(FD_PACKET_DATA(x),FD_PACKET_DATA(y),xlen);}
      case fd_vector_type: case fd_code_type: {
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
      case fd_choice_type: {
        struct FD_CHOICE *xc = fd_consptr(struct FD_CHOICE *,x,fd_choice_type);
        struct FD_CHOICE *yc = fd_consptr(struct FD_CHOICE *,y,fd_choice_type);
        size_t xlen = FD_XCHOICE_SIZE(xc), ylen = FD_XCHOICE_SIZE(yc);
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
            xnatsorted = fd_natsort_choice(xc,_xnatsorted,17);
            ynatsorted = fd_natsort_choice(yc,_ynatsorted,17);
            xscan = (const lispval *)xnatsorted;
            yscan = (const lispval *)ynatsorted;}
          else {
            xscan = FD_XCHOICE_DATA(xc);
            yscan = FD_XCHOICE_DATA(yc);}
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
        fd_ptr_type ctype = FD_CONS_TYPE(FD_CONS_DATA(x));
        if (fd_comparators[ctype])
          return fd_comparators[ctype](x,y,flags);
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

FD_FASTOP void do_swap(lispval *a,lispval *b)
{
  lispval tmp = *a;
  *a = *b;
  *b = tmp;
}

FD_EXPORT
/* lispval_sort:
    Arguments: a vector of dtypes, a length, and a comparison flag value
    Returns: 1, 0, or -1 (an int)
  Returns a function corresponding to a generic sort of two dtype pointers. */
void lispval_sort(lispval *v,size_t n,fd_compare_flags flags)
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

static int compare_compounds(lispval x,lispval y,fd_compare_flags flags)
{
  struct FD_COMPOUND *xc = fd_consptr(struct FD_COMPOUND *,x,fd_compound_type);
  struct FD_COMPOUND *yc = fd_consptr(struct FD_COMPOUND *,y,fd_compound_type);
  lispval xtag = xc->compound_typetag, ytag = yc->compound_typetag;
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

static int compare_timestamps(lispval x,lispval y,fd_compare_flags flags)
{
  struct FD_TIMESTAMP *xtm=
    fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
  struct FD_TIMESTAMP *ytm=
    fd_consptr(struct FD_TIMESTAMP *,y,fd_timestamp_type);
  double diff = u8_xtime_diff(&(xtm->u8xtimeval),&(ytm->u8xtimeval));
  if (diff<0.0) return -1;
  else if (diff == 0.0) return 0;
  else return 1;
}

static int compare_uuids(lispval x,lispval y,fd_compare_flags flags)
{
  struct FD_UUID *xuuid = fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  struct FD_UUID *yuuid = fd_consptr(struct FD_UUID *,y,fd_uuid_type);
  return memcmp(xuuid->uuid16,yuuid->uuid16,16);
}

lispval compare_quick, compare_recursive, compare_elts, compare_natural,
  compare_natsort, compare_numeric, compare_lexical, compare_alphabetical,
  compare_ci, compare_ic, compare_caseinsensitive, compare_case_insensitive,
  compare_nocase, compare_full;

FD_EXPORT fd_compare_flags fd_get_compare_flags(lispval spec)
{
  if ((VOIDP(spec))||(FALSEP(spec)))
    return FD_COMPARE_CODES;
  else if (FD_TRUEP(spec))
    return FD_COMPARE_FULL;
  else if (TABLEP(spec)) {
    fd_compare_flags flags = 0;
    if (fd_testopt(spec,compare_full,VOID))
      flags = FD_COMPARE_FULL;
    else if (fd_testopt(spec,compare_quick,VOID))
      flags = 0;
    else {}
    if ( (fd_testopt(spec,compare_recursive,VOID)) ||
         (fd_testopt(spec,compare_elts,VOID)) )
      flags |= FD_COMPARE_RECURSIVE;
    if ( (fd_testopt(spec,compare_natural,VOID)) ||
         (fd_testopt(spec,compare_natsort,VOID)) )
      flags |= FD_COMPARE_NATSORT;
    if ( (fd_testopt(spec,compare_numeric,VOID)) )
      flags |= FD_COMPARE_NUMERIC;
    if ( (fd_testopt(spec,compare_lexical,VOID)) ||
         (fd_testopt(spec,compare_alphabetical,VOID)))
      flags |= FD_COMPARE_ALPHABETICAL;
    if ( (fd_testopt(spec,compare_ci,VOID)) ||
         (fd_testopt(spec,compare_ic,VOID)) ||
         (fd_testopt(spec,compare_nocase,VOID)) ||
         (fd_testopt(spec,compare_caseinsensitive,VOID)) ||
         (fd_testopt(spec,compare_case_insensitive,VOID)) )
      flags |= FD_COMPARE_CI;
    return flags;}
  else return FD_COMPARE_QUICK;
}

void fd_init_compare_c()
{
  u8_register_source_file(_FILEINFO);

  fd_comparators[fd_compound_type]=compare_compounds;
  fd_comparators[fd_timestamp_type]=compare_timestamps;
  fd_comparators[fd_uuid_type]=compare_uuids;

  compare_quick = fd_intern("QUICK");
  compare_recursive = fd_intern("RECURSIVE");
  compare_elts = fd_intern("ELTS");
  compare_natural = fd_intern("NATURAL");
  compare_natsort = fd_intern("NATSORT");
  compare_numeric = fd_intern("NUMERIC");
  compare_lexical = fd_intern("LEXICAL");
  compare_alphabetical = fd_intern("ALPHABETICAL");
  compare_ci = fd_intern("CI");
  compare_ic = fd_intern("IC");
  compare_caseinsensitive = fd_intern("CASEINSENSITIVE");
  compare_case_insensitive = fd_intern("CASE-INSENSITIVE");
  compare_nocase = fd_intern("NOCASE");
  compare_full = fd_intern("FULL");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
