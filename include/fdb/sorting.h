/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2010 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_SORTING_H
#define FDB_SORTING_H 1
#define FDB_SORTING_H_VERSION "$Id$"

struct FD_SORT_ENTRY {
  fdtype value, key;};

static int _fd_sort_helper(const void *vx,const void *vy)
{
  const struct FD_SORT_ENTRY *sx=(struct FD_SORT_ENTRY *)vx;
  const struct FD_SORT_ENTRY *sy=(struct FD_SORT_ENTRY *)vy;
  if (sx->key==sy->key) return 0;
  else {
    fd_ptr_type xtype=FD_PTR_TYPE(sx->key), ytype=FD_PTR_TYPE(sy->key);
    if (xtype==ytype)
      if (FD_OIDP(sx->key)) {
	FD_OID xaddr=FD_OID_ADDR(sx->key);
	FD_OID yaddr=FD_OID_ADDR(sy->key);
	return FD_OID_COMPARE(xaddr,yaddr);}
      else if (FD_FIXNUMP(sx->key)) {
	int xval=FD_FIX2INT(sx->key);
	int yval=FD_FIX2INT(sy->key);
	if (xval<yval) return -1; else return 1;}
      else return FDTYPE_COMPARE(sx->key,sy->key);
    else if ((xtype==fd_fixnum_type) || (xtype==fd_bigint_type) ||
	      (xtype==fd_double_type) || (xtype==fd_rational_type) ||
	      (xtype==fd_complex_type))
      if ((ytype==fd_fixnum_type) || (ytype==fd_bigint_type) ||
	  (ytype==fd_double_type) || (ytype==fd_rational_type) ||
	  (ytype==fd_complex_type)) {
	int cmp=fd_numcompare(sx->key,sy->key);
	if (cmp) return cmp;
	else if (xtype<ytype) return -1;
	else return 1;}
      else return -1;
    else if ((ytype==fd_fixnum_type) || (ytype==fd_bigint_type) ||
	     (ytype==fd_double_type) || (ytype==fd_rational_type) ||
	     (ytype==fd_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

static int _fd_lexsort_helper(const void *vx,const void *vy)
{
  const struct FD_SORT_ENTRY *sx=(struct FD_SORT_ENTRY *)vx;
  const struct FD_SORT_ENTRY *sy=(struct FD_SORT_ENTRY *)vy;
  if (sx->key==sy->key) return 0;
  else {
    fd_ptr_type xtype=FD_PTR_TYPE(sx->key), ytype=FD_PTR_TYPE(sy->key);
    if (xtype==ytype)
      if (FD_OIDP(sx->key)) {
	FD_OID xaddr=FD_OID_ADDR(sx->key);
	FD_OID yaddr=FD_OID_ADDR(sy->key);
	return FD_OID_COMPARE(xaddr,yaddr);}
      else if (FD_FIXNUMP(sx->key)) {
	int xval=FD_FIX2INT(sx->key);
	int yval=FD_FIX2INT(sy->key);
	if (xval<yval) return -1; else return 1;}
      else if (xtype==fd_string_type) 
	return (strcoll(FD_STRDATA(sx->key),FD_STRDATA(sy->key)));
      else if (xtype==fd_symbol_type)
	return (strcoll(FD_XSYMBOL_NAME(sx->key),FD_XSYMBOL_NAME(sy->key)));
      else return FDTYPE_COMPARE(sx->key,sy->key);
    else if ((xtype==fd_fixnum_type) || (xtype==fd_bigint_type) ||
	      (xtype==fd_double_type) || (xtype==fd_rational_type) ||
	      (xtype==fd_complex_type))
      if ((ytype==fd_fixnum_type) || (ytype==fd_bigint_type) ||
	  (ytype==fd_double_type) || (ytype==fd_rational_type) ||
	  (ytype==fd_complex_type)) {
	int cmp=fd_numcompare(sx->key,sy->key);
	if (cmp) return cmp;
	else if (xtype<ytype) return -1;
	else return 1;}
      else return -1;
    else if ((ytype==fd_fixnum_type) || (ytype==fd_bigint_type) ||
	     (ytype==fd_double_type) || (ytype==fd_rational_type) ||
	     (ytype==fd_complex_type))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    /* Never reached */
    else return 0;}
}

static fdtype _fd_apply_keyfn(fdtype x,fdtype keyfn)
{
  if ((FD_VOIDP(keyfn)) || (FD_EMPTY_CHOICEP(keyfn)))
    return fd_incref(x);
  else if (FD_OIDP(keyfn)) return fd_frame_get(x,keyfn);
  else if (FD_TABLEP(keyfn)) return fd_get(keyfn,x,FD_EMPTY_CHOICE);
  else if (FD_APPLICABLEP(keyfn)) {
    fd_ptr_type keytype=FD_PRIM_TYPE(keyfn);
    fdtype result=fd_applyfns[keytype](keyfn,1,&x);
    return fd_finish_call(result);}
  else if (FD_VECTORP(keyfn)) {
    int i=0, len=FD_VECTOR_LENGTH(keyfn);
    fdtype *keyfns=FD_VECTOR_DATA(keyfn);
    fdtype *vecdata=u8_alloc_n(len,fdtype);
    while (i<len) {vecdata[i]=_fd_apply_keyfn(x,keyfns[i]); i++;}
    return fd_init_vector(NULL,len,vecdata);}
  else if ((FD_OIDP(x)) && (FD_SYMBOLP(keyfn)))
    return fd_frame_get(x,keyfn);
  else if (FD_TABLEP(x))
    return fd_get(x,keyfn,FD_EMPTY_CHOICE);
  else return FD_EMPTY_CHOICE;
}

#endif /* FDB_SORTING_H */
