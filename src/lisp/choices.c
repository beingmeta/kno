/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#include "framerd/fdsource.h"
#include "framerd/dtype.h"

#define MYSTERIOUS_MODULUS 256001281
#define MYSTERIOUS_MULTIPLIER 2654435769U
#define FD_HASHSET_THRESHOLD 200000

ssize_t fd_choicemerge_threshold=FD_CHOICEMERGE_THRESHOLD;

static lispval normalize_choice(lispval x,int free_prechoice);

#define lock_prechoice(ach) u8_lock_mutex(&((ach)->prechoice_lock))
#define unlock_prechoice(ach) u8_unlock_mutex(&((ach)->prechoice_lock))

/* Basic operations on PRECHOICES */

static void recycle_prechoice(struct FD_RAW_CONS *c)
{
  struct FD_PRECHOICE *ch = (struct FD_PRECHOICE *)c;
  if (ch->prechoice_data) {
    const lispval *read = ch->prechoice_data, *lim = ch->prechoice_write;
    if ((ch->prechoice_atomic==0) || (ch->prechoice_nested))
      while (read < lim) {
        lispval v = *read++;
        fd_decref(v);}
    if (ch->prechoice_mallocd) {
      fd_free_choice(ch->prechoice_choicedata);
      ch->prechoice_choicedata = NULL;
      ch->prechoice_mallocd = 0;}}
  fd_decref(ch->prechoice_normalized);
  u8_destroy_mutex(&(ch->prechoice_lock));
  if (!(FD_STATIC_CONSP(ch))) u8_free(ch);
}

static void recycle_prechoice_wrapper(struct FD_PRECHOICE *ch)
{
  fd_decref(ch->prechoice_normalized);
  u8_destroy_mutex(&(ch->prechoice_lock));
  if (!(FD_STATIC_CONSP(ch))) u8_free(ch);
}
static ssize_t write_prechoice_dtype(struct FD_OUTBUF *s,lispval x)
{
  lispval sc = fd_make_simple_choice(x);
  ssize_t n_bytes = fd_write_dtype(s,sc);
  fd_decref(sc);
  return n_bytes;
}

static lispval copy_prechoice(lispval x,int flags)
{
  if (FD_PRECHOICEP(x))
    return normalize_choice(x,0);
  else return fd_copier(x,flags);
}

static int unparse_prechoice(struct U8_OUTPUT *s,lispval x)
{
  lispval sc = fd_make_simple_choice(x);
  fd_unparse(s,sc);
  fd_decref(sc);
  return 1;
}

static int compare_prechoice(lispval x,lispval y,fd_compare_flags flags)
{
  lispval sx = fd_make_simple_choice(x), sy = fd_make_simple_choice(y);
  int compare = LISP_COMPARE(sx,sy,flags);
  fd_decref(sx); fd_decref(sy);
  return compare;
}

/* Sorting and compressing choices */

/* We separate the cases of sorting atomic and composite objects,
   since we can do a direct pointer comparison on atomic objects. */

FD_FASTOP void swap(lispval *a,lispval *b)
{
  lispval t;
  t = *a;
  *a = *b;
  *b = t;
}

static int atomic_sortedp(lispval *v,int n)
{
  int i = 0, lim = n-1;
  while (i<lim)
    if (v[i]<v[i+1]) i++; else return 0;
  return 1;
}

static void atomic_sort(lispval *v,int n)
{
  unsigned i, j, ln, rn;
  if (atomic_sortedp(v,n)) return;
  while (n > 1) {
    swap(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (v[j] > v[0]);
      do ++i; while (i < j && v[i] < v[0]);
      if (i >= j) break; else {}
      swap(&v[i], &v[j]);}
    swap(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      atomic_sort(v, ln); v += j; n = rn;}
    else {atomic_sort(v + j, rn); n = ln;}}
}

static void cons_sort(lispval *v,int n)
{
  unsigned i, j, ln, rn;
  while (n > 1) {
    swap(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (cons_compare(v[j],v[0])>0);
      do ++i; while (i < j && (cons_compare(v[i],v[0])<0));
      if (i >= j) break; else {}
      swap(&v[i], &v[j]);}
    swap(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      cons_sort(v, ln); v += j; n = rn;}
    else {cons_sort(v + j, rn); n = ln;}}
}

/* Compressing a choice removes duplicate elements from a sorted vector.
   Duplicate elements are decref'd.  The function returns the number of
   unique items left in the vector.  */
static int compress_choice(lispval *v,int n,int atomicp)
{
  lispval *write = v, *scan = v, *limit = v+n, pt;
  if (n == 0) return n;
  pt = *scan++;
  if (EMPTYP(pt)) {
    while ( (EMPTYP(pt)) && (scan<limit) )
      pt=*scan++;
    if (!(EMPTYP(pt))) *write++=pt;}
  else *write++=pt;
  /* We separate out the atomic and cons cases because we can do
     pointer comparisons for the atomic cases. */
  if (atomicp)
    while (scan < limit) {
      lispval elt = *scan;
      if (FD_EMPTYP(elt)) {
        scan++;}
      else if (pt== elt) {
        scan++;
        while ((scan<limit) && (pt== *scan)) scan++;}
      else *write++=pt = *scan++;}
  else while (scan < limit) {
      lispval elt = *scan;
      if (FD_EMPTYP(elt)) {
        scan++;}
      else if (cons_compare(pt,elt)==0) {
        fd_decref(*scan); scan++;
        while ((scan<limit) && (cons_compare(pt,*scan)==0)) {
          fd_decref(*scan); scan++;}}
      else *write++=pt = *scan++;}
  return write-v;
}

/* This does a simple binary search of a sorted choice vector,
   looking for a particular element. Once more, we separate out the
   atomic case because it just requires pointer comparisons.  */
static int choice_containsp(lispval x,struct FD_CHOICE *choice)
{
  int size = FD_XCHOICE_SIZE(choice);
  const lispval *bottom = FD_XCHOICE_DATA(choice), *top = bottom+(size-1);
  if (ATOMICP(x)) {
    while (top>=bottom) {
      const lispval *middle = bottom+(top-bottom)/2;
      if (x == *middle) return 1;
      else if (CONSP(*middle)) top = middle-1;
      else if (x < *middle) top = middle-1;
      else bottom = middle+1;}
    return 0;}
  else {
    while (top>=bottom) {
        const lispval *middle = bottom+(top-bottom)/2;
        int comparison = cons_compare(x,*middle);
        if (comparison == 0) return 1;
        else if (comparison<0) top = middle-1;
        else bottom = middle+1;}
      return 0;}
}

FD_EXPORT
/* fd_choice_containsp:
     Arguments: two dtype pointers
     Returns: 1 or 0
 This returns 1 if the first argument is in the choice represented by
 the second argument.  Note that if the second argument isn't a choice,
 this is the same as LISP_EQUAL. */
int fd_choice_containsp(lispval key,lispval x)
{
  if (CHOICEP(x))
    return choice_containsp(key,(struct FD_CHOICE *)x);
  else if (PRECHOICEP(x)) {
    int flag = 0;
    lispval sv = fd_make_simple_choice(x);
    if (CHOICEP(sv))
      flag = choice_containsp(key,(struct FD_CHOICE *)sv);
    else if (LISP_EQUAL(key,sv)) flag = 1;
    fd_decref(sv);
    return flag;}
  else if (LISP_EQUAL(x,key)) return 1;
  else return 0;
}

FD_EXPORT
struct FD_CHOICE *fd_cleanup_choice(struct FD_CHOICE *ch,unsigned int flags)
{
  if (ch == NULL) {
    u8_log(LOGCRIT,"fd_cleanup_choice",
           "The argument to fd_cleanup_choice is NULL");
    fd_seterr2("choice arg is NULL","fd_make_choice");
    return NULL;}
  else {
    int atomicp = 1; int n = ch->choice_size;
    lispval *base = &(ch->choice_0), *scan = base, *limit = scan+n;
    FD_SET_CONS_TYPE(ch,fd_choice_type);
    if (flags&FD_CHOICE_ISATOMIC) atomicp = 1;
    else if (flags&FD_CHOICE_ISCONSES) atomicp = 0;
    else while (scan<limit) {
        if (ATOMICP(*scan)) scan++; else {atomicp = 0; break;}}
    /* Now sort and compress it if requested */
    if (flags&FD_CHOICE_DOSORT) {
      if (atomicp) atomic_sort((lispval *)base,n);
      else cons_sort((lispval *)base,n);}
    if (flags&FD_CHOICE_COMPRESS)
      ch->choice_size = compress_choice((lispval *)base,n,atomicp);
    else ch->choice_size = n;
    return ch;}
}

FD_EXPORT
lispval fd_init_choice
  (struct FD_CHOICE *ch,int n,const lispval *data,int flags)
{
  int atomicp = 1, newlen = n;
  const lispval *base, *scan, *limit;
  if (PRED_FALSE((n==0) && (flags&FD_CHOICE_REALLOC))) {
    if ( (data) && (flags&FD_CHOICE_FREEDATA) ) u8_free(data);
    if (ch) fd_free_choice(ch);
    return EMPTY;}
  else if ( (n==1) &&  (flags&FD_CHOICE_REALLOC) ) {
    lispval elt = (data!=NULL) ? (data[0]) :
      (ch!=NULL) ? ((FD_XCHOICE_DATA(ch))[0]) :
      (FD_NULL);
    if (ch) fd_free_choice(ch);
    if ((data) && (flags&FD_CHOICE_FREEDATA)) u8_free(data);
    if (elt == FD_NULL) {
      fd_seterr2(_("BadInitData"),"fd_init_choice");
      return FD_ERROR;}
    else {
      return elt;}}
  else if (ch == NULL) {
    ch = fd_alloc_choice(n);
    if (ch == NULL) {
      u8_graberrno("fd_init_choice",NULL);
      if ((data) && (flags&FD_CHOICE_FREEDATA)) u8_free(data);
      return FD_ERROR;}
    if (data)
      memcpy((lispval *)FD_XCHOICE_DATA(ch),data,sizeof(lispval)*n);
    else {
      lispval *write = &(ch->choice_0), *writelim = write+n;
      while (write<writelim) *write++=VOID;}}
  else if ((data) && (data!=FD_XCHOICE_DATA(ch)))
    memcpy((lispval *)FD_XCHOICE_DATA(ch),data,sizeof(lispval)*n);
  else {}
  /* Free the original data vector if requested. */
  if ((data) && (flags&FD_CHOICE_FREEDATA)) {
    u8_free((lispval *)data);}
  /* Copy the data unless its yours. */
  base = FD_XCHOICE_DATA(ch); scan = base; limit = scan+n;
  /* Determine if the choice is atomic. */
  if (flags&FD_CHOICE_ISATOMIC) atomicp = 1;
  else if (flags&FD_CHOICE_ISCONSES) atomicp = 0;
  else while (scan<limit)
    if (ATOMICP(*scan)) scan++; else {atomicp = 0; break;}
  if ( (flags&FD_CHOICE_INCREF) && (! atomicp ) )
    fd_incref_vec(((lispval *)(FD_XCHOICE_DATA(ch))),n);
  /* Now sort and compress it if requested */
  if (flags&FD_CHOICE_DOSORT) {
    if (atomicp) atomic_sort((lispval *)base,n);
    else cons_sort((lispval *)base,n);
    newlen = compress_choice((lispval *)base,n,atomicp);}
  else if (flags&FD_CHOICE_COMPRESS)
    newlen = compress_choice((lispval *)base,n,atomicp);
  else newlen = n;
  if (newlen == 0) {
    if (flags&FD_CHOICE_REALLOC) fd_free_choice(ch);
    return FD_EMPTY;}
  else if ((newlen==1) && (flags&FD_CHOICE_REALLOC)) {
    lispval v = base[0];
    fd_free_choice(ch);
    return v;}
  else if ((flags&FD_CHOICE_REALLOC) && (newlen<(n/2)))
    ch = u8_big_realloc(ch,sizeof(struct FD_CHOICE)+((newlen-1)*sizeof(lispval)));
  else {}
  if (ch) {
    FD_INIT_XCHOICE(ch,newlen,atomicp);
    return LISP_CONS(ch);}
  else {
    u8_graberrno("fd_init_choice",NULL);
    return FD_ERROR;}
}

FD_EXPORT
/* fd_make_prechoice:
     Arguments: two dtype pointers
     Returns: a dtype pointer
   Allocates and initializes an FD_PRECHOICE to include the two arguments.
   If the elements are themselves PRECHOICEs, they are normalized into
   simple choices.
*/
lispval fd_make_prechoice(lispval x,lispval y)
{
  struct FD_PRECHOICE *ch = u8_alloc(struct FD_PRECHOICE);
  lispval nx = fd_simplify_choice(x), ny = fd_simplify_choice(y);
  FD_INIT_FRESH_CONS(ch,fd_prechoice_type);
  ch->prechoice_choicedata = fd_alloc_choice(64);
  ch->prechoice_write = ch->prechoice_data =
    (lispval *)FD_XCHOICE_DATA(ch->prechoice_choicedata);
  ch->prechoice_limit = ch->prechoice_data+64; ch->prechoice_normalized = VOID;
  ch->prechoice_mallocd = 1;
  ch->prechoice_nested = 0;
  ch->prechoice_muddled = 0;
  ch->prechoice_size = FD_CHOICE_SIZE(nx)+FD_CHOICE_SIZE(ny);
  if (CHOICEP(nx)) ch->prechoice_nested++;
  if (CHOICEP(ny)) ch->prechoice_nested++;
  if (ch->prechoice_nested) {
    ch->prechoice_write[0]=nx;
    ch->prechoice_write[1]=ny;
    ch->prechoice_muddled = 1;}
  else if (cons_compare(nx,ny)<1) {
    ch->prechoice_write[0]=nx;
    ch->prechoice_write[1]=ny;}
  else {
    ch->prechoice_write[0]=ny;
    ch->prechoice_write[1]=nx;}
  ch->prechoice_atomic = 1;
  if (CONSP(nx)) {
    if ((CHOICEP(nx)) && (FD_ATOMIC_CHOICEP(nx))) {}
    else ch->prechoice_atomic = 0;}
  if (CONSP(ny)) {
    if ((CHOICEP(ny)) && (FD_ATOMIC_CHOICEP(ny))) {}
    else ch->prechoice_atomic = 0;}
  ch->prechoice_write = ch->prechoice_write+2;
  ch->prechoice_uselock = 1;
  u8_init_mutex(&(ch->prechoice_lock));
  return LISP_CONS(ch);
}

FD_EXPORT
/* fd_init_prechoice:
     Arguments: a pointer to an FD_PRECHOICE structure, a size, and a flag (int)
     Returns: a dtype pointer
   Initializes an FD_PRECHOICE with an initial vector of slots.
   If the flag argument is non-zero, the mutex on the FD_PRECHOICE will
   be initialized and used in all operations.  If the pointer argument
   is NULL, an FD_PRECHOICE structure is allocated. */
lispval fd_init_prechoice(struct FD_PRECHOICE *ch,int lim,int uselock)
{
  if (ch == NULL) ch = u8_alloc(struct FD_PRECHOICE);
  FD_INIT_FRESH_CONS(ch,fd_prechoice_type);
  ch->prechoice_choicedata = fd_alloc_choice(lim);
  ch->prechoice_write = ch->prechoice_data =
    (lispval *)FD_XCHOICE_DATA(ch->prechoice_choicedata);
  ch->prechoice_limit = ch->prechoice_data+lim; ch->prechoice_size = 0;
  ch->prechoice_nested = 0;
  ch->prechoice_muddled = 0;
  ch->prechoice_atomic = 1;
  ch->prechoice_mallocd = 1;
  ch->prechoice_normalized = VOID;
  ch->prechoice_uselock = uselock;
  u8_init_mutex(&(ch->prechoice_lock));
  return LISP_CONS(ch);
}

FD_EXPORT
/* _fd_add_to_choice:
     Arguments: two dtype pointers
     Returns: a dtype pointer
   Combines two dtype pointers into a single lisp value.  This is used
   by the macro CHOICE_ADD when choice operations are not being
   inlined.

*/
lispval _fd_add_to_choice(lispval current,lispval v)
{
  return _add_to_choice(current,v);
}

/* Converting prechoices to choices */

/* PRECHOICEs are accumulating choices which accumulate values.  PRECHOICEs
   have a ->normalized field which, when non-void, is the simple choice
   version of the prechoice. */

static lispval prechoice_append(struct FD_PRECHOICE *ch,int freeing_prechoice);

static lispval normalize_choice(lispval x,int free_prechoice)
{
  struct FD_PRECHOICE *ch=
    fd_consptr(struct FD_PRECHOICE *,x,fd_prechoice_type);
  /* Double check that it's really okay to free it. */
  if (free_prechoice) {
    if (FD_CONS_REFCOUNT(ch)>1) {
      free_prechoice = 0; fd_decref(x);}}
  if (ch->prechoice_uselock) lock_prechoice(ch);
  /* If you have a normalized value, use it. */
  if (!(VOIDP(ch->prechoice_normalized))) {
    lispval v = fd_incref(ch->prechoice_normalized);
    if (ch->prechoice_uselock) unlock_prechoice(ch);
    if (free_prechoice) recycle_prechoice((fd_raw_cons)ch);
    return v;}
  /* If it's really empty, just return the empty choice. */
  else if ((ch->prechoice_write-ch->prechoice_data) == 0) {
    ch->prechoice_normalized = EMPTY;
    unlock_prechoice(ch);
    if (ch->prechoice_uselock) unlock_prechoice(ch);
    if (free_prechoice) recycle_prechoice((fd_raw_cons)ch);
    return EMPTY;}
  /* If it's only got one value, return it. */
  else if ((ch->prechoice_write-ch->prechoice_data) == 1) {
    lispval value = fd_incref(*(ch->prechoice_data));
    if (ch->prechoice_uselock) unlock_prechoice(ch);
    if (free_prechoice) recycle_prechoice((fd_raw_cons)ch);
    else ch->prechoice_normalized = fd_incref(value);
    return value;}
  /* If you're going to free the prechoice and it's not nested, you
     can just use the choice you've been depositing values in,
     appropriately initialized, sorted etc.  */
  else if ((free_prechoice) && (ch->prechoice_nested==0)) {
    struct FD_CHOICE *nch = ch->prechoice_choicedata;
    int flags = FD_CHOICE_REALLOC, n = ch->prechoice_size;
    if (ch->prechoice_atomic) flags = flags|FD_CHOICE_ISATOMIC;
    else flags = flags|FD_CHOICE_ISCONSES;
    if (ch->prechoice_muddled) flags = flags|FD_CHOICE_DOSORT;
    else flags = flags|FD_CHOICE_COMPRESS;
    if (ch->prechoice_uselock) unlock_prechoice(ch);
    /* This recycles the prechoice but not its data */
    recycle_prechoice_wrapper(ch);
    return fd_init_choice(nch,n,NULL,flags);}
  else if (ch->prechoice_nested==0) {
    /* If it's not nested, we can mostly just call fd_make_choice. */
    int flags = 0, n_elts = ch->prechoice_write-ch->prechoice_data; lispval result;
    if (ch->prechoice_atomic) flags = flags|FD_CHOICE_ISATOMIC; else {
      /* Incref everything */
      const lispval *scan = ch->prechoice_data, *write = ch->prechoice_write;
      while (scan<write) {fd_incref(*scan); scan++;}
      flags = flags|FD_CHOICE_ISCONSES;}
    if (ch->prechoice_muddled) flags = flags|FD_CHOICE_DOSORT;
    else flags = flags|FD_CHOICE_COMPRESS;
    result = fd_make_choice(n_elts,ch->prechoice_data,flags);
    ch->prechoice_normalized = fd_incref(result);
    if (ch->prechoice_uselock) unlock_prechoice(ch);
    return result;}
  else if (ch->prechoice_nested == (ch->prechoice_write-ch->prechoice_data) ) {
    /* They're all choices */
    int n_choices = ch->prechoice_nested;
    struct FD_CHOICE **choices = (struct FD_CHOICE **) ch->prechoice_data;
    lispval combined = fd_merge_choices(choices,n_choices);
    if (free_prechoice) {
      if (ch->prechoice_uselock) unlock_prechoice(ch);
      recycle_prechoice((fd_raw_cons)ch);}
    else {
      ch->prechoice_normalized = fd_incref(combined);
      if (ch->prechoice_uselock) unlock_prechoice(ch);}
    return combined;}
  else if (ch->prechoice_size > fd_choicemerge_threshold) {
    int is_atomic = 1, nested = ch->prechoice_nested,
      n_loners = (ch->prechoice_write-ch->prechoice_data)-nested;
    struct FD_CHOICE *loners = fd_alloc_choice(n_loners);
    struct FD_CHOICE **choices = u8_alloc_n(nested+1,struct FD_CHOICE *);
    struct FD_CHOICE **write_choices = choices;
    *write_choices++=loners;
    lispval *xdata = (lispval *) (FD_XCHOICE_ELTS(loners)), *write = xdata;
    lispval *scan = ch->prechoice_data, *limit = ch->prechoice_write;
    while (scan<limit) {
      lispval v = *scan++;
      if (FD_CHOICEP(v))
        *write_choices++ = (fd_choice)v;
      else {
        if (FD_CONSP(v)) is_atomic=0;
        *write++=v;}}
    fd_init_choice(loners,write-xdata,NULL,
                   FD_CHOICE_INCREF|FD_CHOICE_DOSORT|FD_CHOICE_COMPRESS|
                   ((is_atomic) ? (FD_CHOICE_ISATOMIC) :
                    (FD_CHOICE_ISCONSES)));
    lispval combined = fd_merge_choices(choices,nested+1);
    if (free_prechoice) {
      if (ch->prechoice_uselock) unlock_prechoice(ch);
      recycle_prechoice((fd_raw_cons)ch);}
    else {
      ch->prechoice_normalized = fd_incref(combined);
      if (ch->prechoice_uselock) unlock_prechoice(ch);}
    u8_free(choices);
    fd_decref(((lispval)loners));
    return combined;}
  else {
    /* If the choice is small enough, we can call prechoice_append,
       which just appends the choices together and relies on sort
       and compression to remove duplicates.  We don't want to do
       this if the choice is really huge, because we don't want to
       sort the huge vector. */
    lispval converted = prechoice_append(ch,free_prechoice);
    if (free_prechoice) {
      if (ch->prechoice_uselock) unlock_prechoice(ch);
      fd_decref(x);
      return converted;}
    else {
      ch->prechoice_normalized = fd_incref(converted);
      if (ch->prechoice_uselock) unlock_prechoice(ch);
      return converted;}}
}

FD_EXPORT
/* _fd_make_simple_choice:
      Arguments: a dtype pointer
      Returns: a dtype pointer
  This returns a normalized choice for an prechoice or an incref'd value
  otherwise. */
lispval _fd_make_simple_choice(lispval x)
{
  if (PRECHOICEP(x))
    return normalize_choice(x,0);
  else return fd_incref(x);
}

FD_EXPORT
/* _fd_simplify_choice:
      Arguments: a dtype pointer
      Returns: a dtype pointer
  This returns a normalized choice for a PRECHOICE, or its argument
  otherwise. */
lispval _fd_simplify_choice(lispval x)
{
  if (PRECHOICEP(x))
    return normalize_choice(x,1);
  else return x;
}

FD_EXPORT int _fd_choice_size(lispval x)
{
  return fd_choice_size(x);
}

/* Merging choices */

/* This procedures merge choices by scanning over them linearly, merging
   elements. */

struct FD_CHOICE_SCANNER {
  lispval top;
  const lispval *ptr, *lim;};

static int resort_scanners(struct FD_CHOICE_SCANNER *v,int n,int atomic)
{
  const lispval *ptr = v[0].ptr, *lim = v[0].lim;
  if (ptr == lim) {
    int i = 1; while ((i<n) && (v[i].ptr == v[i].lim)) i++;
    memmove(v,v+i,sizeof(struct FD_CHOICE_SCANNER)*(n-i));
    return n-i;}
  else {
    int i = 1; lispval head = v[0].top;
    if (atomic)
      while (i<n) if (head<=v[i].top) break; else i++;
    else while (i<n)
      if (cons_compare(head,v[i].top)<=0) break;
      else i++;
    memmove(v,v+1,sizeof(struct FD_CHOICE_SCANNER)*(i-1));
    v[i-1].top = head; v[i-1].ptr = ptr; v[i-1].lim = lim;
    return n;}
}

static int scanner_loop(struct FD_CHOICE_SCANNER *scanners,
                               int n_scanners,
                               lispval *vals)
{
  lispval *write = vals, last = FD_NEVERSEEN;
  while (n_scanners>1) {
    lispval top = scanners[0].top;
    if (!(LISP_EQUAL(last,top))) {*write++=fd_incref(top); last = top;}
    scanners[0].ptr++;
    if (scanners[0].ptr == scanners[0].lim)
      if (n_scanners==1) n_scanners--;
      else n_scanners = resort_scanners(scanners,n_scanners,0);
    else {
      top = scanners[0].top = scanners[0].ptr[0];
      if (n_scanners==1) {}
      else if (cons_compare(top,scanners[1].top)<0) {}
      else n_scanners = resort_scanners(scanners,n_scanners,0);}}
  if (n_scanners==1) {
    const lispval top = scanners[0].top, *read, *limit;
    if (LISP_EQUAL(top,last)) scanners[0].ptr++;
    read = scanners[0].ptr; limit = scanners[0].lim;
    while (read<limit) {
      *write = fd_incref(*read); write++; read++;}}
  return write-vals;
}

static int atomic_scanner_loop(struct FD_CHOICE_SCANNER *scanners,
                               int n_scanners,
                               lispval *vals)
{
  lispval *write = vals, last = FD_NEVERSEEN;
  while (n_scanners>1) {
    lispval top = scanners[0].top;
    if (top!=last) {*write++=top; last = top;}
    scanners[0].ptr++;
    if (scanners[0].ptr == scanners[0].lim)
      if (n_scanners==1) n_scanners--;
      else n_scanners = resort_scanners(scanners,n_scanners,1);
    else {
      top = scanners[0].top = scanners[0].ptr[0];
      if (n_scanners==1) {}
      else if (top<scanners[1].top) {}
      else n_scanners = resort_scanners(scanners,n_scanners,1);}}
  if (n_scanners==1) {
    lispval top = scanners[0].top; int len;
    if (top == last) scanners[0].ptr++;
    len = scanners[0].lim-scanners[0].ptr;
    memcpy(write,scanners[0].ptr,len*sizeof(lispval));
    write = write+len;}
  return write-vals;
}

static int compare_scanners(const void *x,const void *y)
{
  struct FD_CHOICE_SCANNER *xs = (struct FD_CHOICE_SCANNER *)x;
  struct FD_CHOICE_SCANNER *ys = (struct FD_CHOICE_SCANNER *)y;
  return cons_compare(xs->top,ys->top);
}

static int compare_scanners_atomic(const void *x,const void *y)
{
  struct FD_CHOICE_SCANNER *xs = (struct FD_CHOICE_SCANNER *)x;
  struct FD_CHOICE_SCANNER *ys = (struct FD_CHOICE_SCANNER *)y;
  lispval xv = xs->top, yv = ys->top;
  if (xv<yv) return -1;
  else if (xv == yv) return 0;
  else return 1;
}

FD_EXPORT
/* fd_merge_choices:
     Arguments: a vector of pointers to choice structures and a length
     Returns: a dtype pointer
  Combines the elements of all of the choices into a single sorted choice.
  It does this in linear time by marching along all the choices together.
  */
lispval fd_merge_choices(struct FD_CHOICE **choices,int n_choices)
{
  int n_scanners = n_choices, max_space = 0, atomicp = 1, new_size, flags = 0;
  lispval *write;
  struct FD_CHOICE *new_choice;
  struct FD_CHOICE_SCANNER *scanners=
    u8_alloc_n(n_choices,struct FD_CHOICE_SCANNER);
  /* Initialize the scanners. */
  int i = 0; while (i < n_scanners) {
    const lispval *data = FD_XCHOICE_DATA(choices[i]);
    scanners[i].top = data[0];
    scanners[i].ptr = data;
    scanners[i].lim = data+FD_XCHOICE_SIZE(choices[i]);
    if ((atomicp) && (!(FD_XCHOICE_ATOMICP(choices[i])))) atomicp = 0;
    max_space = max_space+FD_XCHOICE_SIZE(choices[i]);
    i++;}
  /* Make a new choice with enough space for everything. */
  new_choice = fd_alloc_choice(max_space);
  /* Where we write */
  write = (lispval *)FD_XCHOICE_DATA(new_choice);
  if (atomicp)
    qsort(scanners,n_scanners,sizeof(struct FD_CHOICE_SCANNER),
          compare_scanners_atomic);
  else qsort(scanners,n_scanners,sizeof(struct FD_CHOICE_SCANNER),
             compare_scanners);
  if (atomicp)
    new_size = atomic_scanner_loop(scanners,n_scanners,write);
  else new_size = scanner_loop(scanners,n_scanners,write);
  u8_free(scanners);
  if (atomicp) flags = flags|FD_CHOICE_ISATOMIC;
  else flags = flags|FD_CHOICE_ISCONSES;
  return fd_init_choice(new_choice,new_size,
                        FD_XCHOICE_DATA(new_choice),
                        (flags|FD_CHOICE_REALLOC));
}

static
/* prechoice_append simply appends together the component choices and then
   relies on sorting and compression by fd_init_choice to remove duplicates.
   This is a fine approach which the result set is relatively small and
   sorting the resulting choice isn't a big deal. */
lispval prechoice_append(struct FD_PRECHOICE *ch,int freeing_prechoice)
{
  struct FD_CHOICE *result = fd_alloc_choice(ch->prechoice_size);
  lispval *base = (lispval *)FD_XCHOICE_DATA(result);
  lispval *write = base, *write_limit = base+(ch->prechoice_size);
  lispval *scan = ch->prechoice_data, *limit = ch->prechoice_write;
  while (scan < limit) {
    lispval v = *scan;
    if (write>=write_limit) {
      u8_log(LOG_WARN,"prechoice_inconsistency",
              "total size is more than the recorded %d",ch->prechoice_size);
      abort();}
    else if (CHOICEP(v)) {
      struct FD_CHOICE *each = (struct FD_CHOICE *)v;
      int freed = ((freeing_prechoice) && (FD_CONS_REFCOUNT(each)==1));
      if (write+FD_XCHOICE_SIZE(each)>write_limit) {
        u8_log(LOG_WARN,"prechoice_inconsistency",
                "total size is more than the recorded %d",ch->prechoice_size);
        abort();}
      else if (FD_XCHOICE_ATOMICP(each)) {
        memcpy(write,FD_XCHOICE_DATA(each),
               sizeof(lispval)*FD_XCHOICE_SIZE(each));
        write = write+FD_XCHOICE_SIZE(each);}
      else if (freed) {
        memcpy(write,FD_XCHOICE_DATA(each),
               sizeof(lispval)*FD_XCHOICE_SIZE(each));
        write = write+FD_XCHOICE_SIZE(each);}
      else {
        const lispval *vscan = FD_XCHOICE_DATA(each),
          *vlimit = vscan+FD_XCHOICE_SIZE(each);
        while (vscan < vlimit) {
          lispval ev = *vscan++; *write++=fd_incref(ev);}}
      if (freed) {
        fd_free_choice(each);
        *scan = VOID;}}
    else if (freeing_prechoice) {
      *write++=v; *scan = VOID;}
    else if (ATOMICP(v)) *write++=v;
    else {*write++=fd_incref(v);}
    scan++;}
  if ((write-base)>1)
    return fd_init_choice(result,write-base,NULL,
                          (FD_CHOICE_DOSORT|
                           ((ch->prechoice_atomic)?(FD_CHOICE_ISATOMIC):
                            (FD_CHOICE_ISCONSES))));
  else {
    lispval v = base[0];
    u8_free(result);
    return v;}
}

/* QCHOICEs */

/* QCHOICEs are simply wrappers around an underlying choice which
   will inihibit iteration over arguments. */

FD_EXPORT
/* fd_init_qchoice:
      Arguments: a pointer to a FD_QCHOICE struct and a dtype pointer
      Returns: a dtype pointer
  Initializes the structure with the qchoice.
 */
lispval fd_init_qchoice(struct FD_QCHOICE *ptr,lispval choice)
{
  if (ptr == NULL) ptr = u8_alloc(struct FD_QCHOICE);
  FD_INIT_FRESH_CONS(ptr,fd_qchoice_type);
  ptr->qchoiceval = choice;
  return LISP_CONS(ptr);
}

static ssize_t write_qchoice_dtype(struct FD_OUTBUF *s,lispval x)
{
  struct FD_QCHOICE *qc = FD_XQCHOICE(x);
  return fd_write_dtype(s,qc->qchoiceval);
}

static int unparse_qchoice(struct U8_OUTPUT *s,lispval x)
{
  struct FD_QCHOICE *qc = FD_XQCHOICE(x);
  u8_puts(s,"#"); fd_unparse(s,qc->qchoiceval);
  return 1;
}

static lispval copy_qchoice(lispval x,int deep)
{
  struct FD_QCHOICE *copied = u8_alloc(struct FD_QCHOICE);
  struct FD_QCHOICE *qc = FD_XQCHOICE(x);
  FD_INIT_FRESH_CONS(copied,fd_qchoice_type);
  if (deep)
    copied->qchoiceval = fd_deep_copy(qc->qchoiceval);
  else {
    fd_incref(qc->qchoiceval);
    copied->qchoiceval = qc->qchoiceval;}
  return LISP_CONS(copied);
}

static int compare_qchoice(lispval x,lispval y,fd_compare_flags flags)
{
  struct FD_QCHOICE *xqc = FD_XQCHOICE(x), *yqc = FD_XQCHOICE(y);
  return (LISP_COMPARE(xqc->qchoiceval,yqc->qchoiceval,flags));
}

/* Set operations (intersection, union, difference) on choices */

static int compare_choicep_size(const void *cx,const void *cy)
{
  int xsz = FD_XCHOICE_SIZE((*((struct FD_CHOICE **)cx)));
  int ysz = FD_XCHOICE_SIZE((*((struct FD_CHOICE **)cy)));
  if (xsz<ysz) return -1;
  else if (xsz == ysz) return 0;
  else return 1;
}

FD_EXPORT
/* fd_intersect_choices:
    Arguments: a pointer to a vector of pointers to choice structures
     and its length (an int)
    Returns: the intersection of the choices.
 */
lispval fd_intersect_choices(struct FD_CHOICE **choices,int n_choices)
{
  lispval v, *results, *write; int max_results, atomicp = 1;
  /* Sort the choices by size, iterate over the smallest and check
     the others in order. */
  qsort(choices,n_choices,sizeof(struct FD_CHOICE *),
        compare_choicep_size);
  max_results = FD_XCHOICE_SIZE(choices[0]);
  write = results = u8_big_alloc_n(max_results,lispval);
  {
    const lispval *scan = FD_XCHOICE_DATA(choices[0]);
    const lispval *limit = scan+max_results;
    while (scan<limit) {
      lispval item = *scan++; int i = 1; while (i < n_choices)
        if (choice_containsp(item,choices[i])) i++;
        else break;
      if (i == n_choices) {
        if (CONSP(item)) {
          atomicp = 0;
          fd_incref(item);
          *write++=item;}
        else *write++=item;}}}
  if (write == results)
    v=EMPTY;
  else if (write == (results+1))
    v = results[0];
  else if (atomicp)
    v=fd_make_choice(write-results,results,FD_CHOICE_ISATOMIC);
  else v=fd_make_choice(write-results,results,0);
  u8_big_free(results);
  return v;
}

FD_EXPORT
/* fd_intersection:
     Arguments: a pointer to a vector of lisp elements
       and the length of the vector
     Returns: a dtype pointer
  Computes the intersection of a set of choices.
 */
lispval fd_intersection(lispval *v,unsigned int n)
{
  if (n == 0)
    return EMPTY;
  else if (n == 1)
    return fd_incref(v[0]);
  else {
    lispval result = VOID;
    /* The simplest case is where one or more of the inputs
       is a singleton.  If more than one is a singleton and they
       are different, we just return the empty set.  If they're the same,
       we'll just do membership tests on that one item.

       To resolve this, we scan all the items being intersected,
       using result to store the singleton value, with VOID
       indicating we haven't stored anything there. */
    int i = 0, prechoices = 0; while (i < n)
      if (EMPTYP(v[i]))
        /* If you find any empty, the intersection is empty. */
        return EMPTY;
      else if (CHOICEP(v[i])) i++; /* Pass any choices */
      else if (PRECHOICEP(v[i])) {
        /* Count the prechoices, because you'll have to convert them. */
        i++; prechoices++;}
    /* After this point, we know we have a singleton value. */
      else if (VOIDP(result))
        /* This must be our first singleton. */
        result = v[i++];
      else if (LISP_EQUAL(result,v[i]))
        /* This is a consistent singleton */
        i++;
    /* If it's not a consistent singleton, we can just return the
       empty choice. */
      else return EMPTY;
    if (!(VOIDP(result))) {
      /* This is the case where we found a singleton. */
      int i = 0; while (i < n)
        if (CHOICEP(v[i]))
          if (fd_choice_containsp(result,v[i])) i++;
          else return EMPTY;
        else if (PRECHOICEP(v[i])) {
          /* Arguably, it might not make sense to do this conversion
             right now. */
          lispval sc = fd_make_simple_choice(v[i]);
          if (fd_choice_containsp(result,sc)) {
            fd_decref(sc); i++;}
          else {
            fd_decref(sc);
            return EMPTY;}}
        else i++;
      return fd_incref(result);}
    /* At this point, all we have is choices and prechoices, so we need to
       make a vector of the choices on which to call fd_intersect_choices.
       We also need to keep track of the values we convert so that
       we can clean them up when we're done. */
    else if (prechoices) {
      /* This is the case where we have choices to convert. */
      struct FD_CHOICE **choices, *_choices[16];
      lispval *conversions, _conversions[16];
      lispval result = VOID; int i, n_choices = 0, n_conversions = 0;
      /* Try to use a stack vector if possible. */
      if (n>16) {
        choices = u8_alloc_n(n,struct FD_CHOICE *);
        conversions = u8_alloc_n(n,lispval);}
      else {choices=_choices; conversions=_conversions;}
      i = 0; while (i < n)
        /* We go down doing conversions.  Note that we do the same thing
           as above with handling singletons because the PRECHOICEs might
           resolve to singletons. */
        if (CHOICEP(v[i])) {
          choices[n_choices++]=(struct FD_CHOICE *)v[i++];}
        else if (PRECHOICEP(v[i])) {
          lispval nc = fd_make_simple_choice(v[i++]);
          if (CHOICEP(nc)) {
            choices[n_choices++]=(struct FD_CHOICE *)nc;
            conversions[n_conversions++]=nc;}
          /* These are all in case a PRECHOICE turns out to be
             empty or a singleton */
          else if (EMPTYP(nc)) break;
          else if (VOIDP(result)) result = nc;
          else if (LISP_EQUAL(result,nc)) i++;
          else {
            /* We record it because we converted it. */
            if (CONSP(nc)) conversions[n_conversions++]=nc;
            result = EMPTY; break;}}
        else i++;
      if (VOIDP(result))
        /* The normal case, just do the intersection */
        result = fd_intersect_choices(choices,n_choices);
      /* One of the prechoices turned out to be empty */
      else if (EMPTYP(result)) {}
      else {
        /* One of the prechoices turned out to be a singleton */
        int k = 0; while (k < n_choices)
          if (choice_containsp(result,choices[k])) k++;
          else {result = EMPTY; break;}}
      /* Now, clean up your conversions. */
      i = 0; while (i < n_conversions) {
        fd_decref(conversions[i]); i++;}
      if (n>16) {u8_free(choices); u8_free(conversions);}
      return result;}
    else if (n < 1024) { /* Random value */
      struct FD_CHOICE *choices[n];
      lispval result; int i = 0;
      while (i < n) {choices[i]=(struct FD_CHOICE *)v[i]; i++;}
      result = fd_intersect_choices(choices,n);
      return result;}
    else {
      struct FD_CHOICE **choices = u8_malloc(n*sizeof(struct FD_CHOICE *));
      lispval result; int i = 0;
      while (i < n) {choices[i]=(struct FD_CHOICE *)v[i]; i++;}
      result = fd_intersect_choices(choices,n);
      u8_free(choices);
      return result;}
  }
}

FD_EXPORT
/* fd_union:
     Arguments: a pointer to a vector of lisp elements
       and the length of the vector
     Returns: a dtype pointer
  Computes the union of a set of choices.
 */
lispval fd_union(lispval *v,unsigned int n)
{
  if (n == 0) return EMPTY;
  else {
    lispval result = EMPTY; int i = 0;
    while (i < n) {
      lispval elt = v[i++]; fd_incref(elt);
      CHOICE_ADD(result,elt);}
    return fd_simplify_choice(result);}
}

static lispval compute_choice_difference
  (struct FD_CHOICE *whole,struct FD_CHOICE *part)
{
  int wsize = FD_XCHOICE_SIZE(whole), psize = FD_XCHOICE_SIZE(part);
  int watomicp = FD_XCHOICE_ATOMICP(whole), patomicp = FD_XCHOICE_ATOMICP(part);
  const lispval *wscan = FD_XCHOICE_DATA(whole), *wlim = wscan+wsize;
  const lispval *pscan = FD_XCHOICE_DATA(part), *plim = pscan+psize;
  struct FD_CHOICE *result = fd_alloc_choice(wsize);
  lispval *newv = (lispval *)FD_XCHOICE_DATA(result), *write = newv;
  if ((watomicp) && (patomicp)) {
    while ( (wscan<wlim) && (pscan<plim) ) {
      if (*wscan == *pscan) {
        wscan++;
        pscan++;
        continue;}
      else if (*wscan < *pscan) {
        *write = *wscan;
        write++;
        wscan++;
        continue;}
      else pscan++;
      if (pscan >= plim)
        break;
      else if (*wscan <= *pscan)
        continue;
      else {
        /* We are now looking for the next element of 'part' which
           would be at or after the next element of whole. */
        lispval nextval = *wscan;
        const lispval *bottom = pscan, *top = plim-1;
        size_t window = top-bottom;
        const lispval *middle = bottom + (window/2);
        while (top > bottom) {
          lispval midval = *middle;
          if (nextval == midval)
            break;
          else if (nextval < midval)
            top = middle-1;
          else bottom = middle+1;
          window = top - bottom;
          middle = bottom + (window/2);}
        if (middle>pscan) pscan=middle;}}}
  else while ( (wscan<wlim) && (pscan<plim) ) {
      if (LISP_EQUAL(*wscan,*pscan)) {
        wscan++;
        pscan++;}
      else if (cons_compare(*wscan,*pscan)<0) {
        *write = fd_incref(*wscan);
        write++;
        wscan++;}
      else pscan++;}
  /* Now we just copy the remainder, since *part* has run out */
  if (watomicp) {
    memmove(write,wscan,sizeof(lispval)*(wlim-wscan));
    write = write + (wlim-wscan);}
  else while (wscan < wlim) {
      *write = fd_incref(*wscan);
      write++; wscan++;}
  if (write-newv>1)
    return fd_init_choice(result,write-newv,NULL,
                          ((watomicp)?(FD_CHOICE_ISATOMIC):(0)));
  else if (write == newv) {
    fd_free_choice(result);
    return EMPTY;}
  else {
    lispval singleton = newv[0];
    fd_free_choice(result);
    return singleton;}
}

FD_EXPORT
/* fd_difference:
     Arguments: two dtype pointers
     Returns: a dtype pointer
  Computes the difference of two sets of choices.
*/
lispval fd_difference(lispval value,lispval remove)
{
  if (EMPTYP(value)) return value;
  else if (EMPTYP(remove)) return fd_incref(value);
  else if ((PRECHOICEP(value)) || (PRECHOICEP(remove))) {
    lispval svalue = fd_make_simple_choice(value);
    lispval sremove = fd_make_simple_choice(remove);
    lispval result = fd_difference(svalue,sremove);
    fd_decref(svalue);
    fd_decref(sremove);
    return result;}
  else if (CHOICEP(value))
    if (!(CHOICEP(remove)))
      if (fd_choice_containsp(remove,value)) {
        struct FD_CHOICE *vchoice=
          fd_consptr(struct FD_CHOICE *,value,fd_choice_type);
        int size = FD_XCHOICE_SIZE(vchoice);
        int atomicp = FD_XCHOICE_ATOMICP(vchoice);
        if (size==1)
          return EMPTY;
        else if (size==2)
          if (LISP_EQUAL(remove,(FD_XCHOICE_DATA(vchoice))[0]))
            return fd_incref((FD_XCHOICE_DATA(vchoice))[1]);
          else return fd_incref((FD_XCHOICE_DATA(vchoice))[0]);
        else {
          struct FD_CHOICE *new_choice = fd_alloc_choice(size-1);
          lispval *newv = (lispval *)FD_XCHOICE_DATA(new_choice);
          const lispval *read = FD_XCHOICE_DATA(vchoice), *lim = read+size;
          lispval *write = newv;
          int flags = ((atomicp)?(FD_CHOICE_ISATOMIC):(0))|FD_CHOICE_REALLOC;
          while (read < lim)
            if (LISP_EQUAL(*read,remove))
              read++;
            else {
              *write = fd_incref(*read);
              write++;
              read++;}
          return fd_init_choice(new_choice,size-1,newv,flags);}}
      else return fd_incref(value);
    else return compute_choice_difference
           (fd_consptr(struct FD_CHOICE *,value,fd_choice_type),
            fd_consptr(struct FD_CHOICE *,remove,fd_choice_type));
  else if (CHOICEP(remove))
    if (fd_choice_containsp(value,remove))
      return EMPTY;
    else return fd_incref(value);
  else if (LISP_EQUAL(value,remove)) return EMPTY;
  else return fd_incref(value);
}

FD_EXPORT
/* fd_overlapp:
     Arguments: two dtype pointers
     Returns: a dtype pointer
  Returns 1 if the two arguments have any elements in common.
  On non-choices, this is just equalp, on a non-choice and a choice,
   this is just choice_containsp, and on two choices, it's currently
   just implemented as a series of choice_containsp operations.
*/
int fd_overlapp(lispval xarg,lispval yarg)
{
  if (EMPTYP(xarg)) return 0;
  else if (EMPTYP(yarg)) return 0;
  else {
    int retval = 0;
    lispval x, y;
    if (PRECHOICEP(xarg))
      x = normalize_choice(xarg,0);
    else x = xarg;
    if (PRECHOICEP(yarg))
      y = normalize_choice(yarg,0);
    else y = yarg;
    if (CHOICEP(x))
      if (CHOICEP(y))
        if (FD_CHOICE_SIZE(x)>FD_CHOICE_SIZE(y)) {
          DO_CHOICES(elt,y)
            if (choice_containsp(elt,(fd_choice)x)) {
              retval = 1;
              break;}}
        else {
          DO_CHOICES(elt,x)
            if (choice_containsp(elt,(fd_choice)y)) {
              retval = 1;
              break;}}
      else retval = choice_containsp(y,(fd_choice)x);
    else if (CHOICEP(y))
      retval = choice_containsp(x,(fd_choice)y);
    else retval = LISP_EQUAL(x,y);
    if (PRECHOICEP(xarg)) fd_decref(x);
    if (PRECHOICEP(yarg)) fd_decref(y);
    return retval;}
}

FD_EXPORT
/* fd_containsp:
     Arguments: two dtype pointers
     Returns: a dtype pointer
  Returns 1 if the the first argument is a proper subset of the second argument.
  On non-choices, this is just equalp, on a non-choice and a choice,
   this is just choice_containsp, and on two choices, it's currently
   just implemented as a series of choice_containsp operations.
*/
int fd_containsp(lispval xarg,lispval yarg)
{
  if (EMPTYP(xarg)) return 0;
  else if (EMPTYP(yarg)) return 0;
  else {
    lispval x, y; int retval = 0;
    if (PRECHOICEP(xarg))
      x = normalize_choice(xarg,0);
    else x = xarg;
    if (PRECHOICEP(yarg))
      y = normalize_choice(yarg,0);
    else y = yarg;
    if (CHOICEP(x))
      if (CHOICEP(y)) {
        int contained = 1;
        DO_CHOICES(elt,x)
          if (choice_containsp(elt,(fd_choice)y)) {}
          else {
            contained = 0;
            FD_STOP_DO_CHOICES;
            break;}
        if (contained) 
          retval = 1;}
      else retval = 0;
    else if (CHOICEP(y))
      retval = choice_containsp(x,(fd_choice)y);
    else retval = LISP_EQUAL(x,y);
    if (PRECHOICEP(xarg)) fd_decref(x);
    if (PRECHOICEP(yarg)) fd_decref(y);
    return retval;}
}

/* Natsorting CHOICES */

FD_EXPORT
lispval *fd_natsort_choice(fd_choice ch,lispval *tmpbuf,ssize_t tmp_len)
{
  int len = FD_XCHOICE_SIZE(ch);
  const lispval *data = FD_XCHOICE_DATA(ch);
  lispval *natsorted = (tmp_len>len) ? (tmpbuf) :
    u8_alloc_n(len,lispval);
  memcpy(natsorted,data,len*sizeof(lispval));
  lispval_sort(natsorted,len,FD_COMPARE_NATSORT);
  return natsorted;
}


/* Type initializations */

void fd_init_choices_c()
{
  u8_register_source_file(_FILEINFO);

  fd_type_names[fd_qchoice_type]="qchoice";
  fd_dtype_writers[fd_qchoice_type]=write_qchoice_dtype;
  fd_comparators[fd_qchoice_type]=compare_qchoice;
  fd_unparsers[fd_qchoice_type]=unparse_qchoice;
  fd_copiers[fd_qchoice_type]=copy_qchoice;

  fd_type_names[fd_prechoice_type]="prechoice";
  fd_recyclers[fd_prechoice_type]=recycle_prechoice;
  fd_dtype_writers[fd_prechoice_type]=write_prechoice_dtype;
  fd_comparators[fd_prechoice_type]=compare_prechoice;
  fd_comparators[fd_choice_type]=compare_prechoice;
  fd_unparsers[fd_prechoice_type]=unparse_prechoice;
  fd_copiers[fd_prechoice_type]=copy_prechoice;

  fd_type_names[fd_choice_type]="choice";
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
