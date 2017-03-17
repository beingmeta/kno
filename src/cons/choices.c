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

int fd_mergesort_threshold=FD_MERGESORT_THRESHOLD;

static fdtype normalize_choice(fdtype x,int free_achoice);

#define lock_achoice(ach) u8_lock_mutex(&((ach)->achoice_lock))
#define unlock_achoice(ach) u8_unlock_mutex(&((ach)->achoice_lock))

/* Basic operations on ACHOICES */

static void recycle_achoice(struct FD_RAW_CONS *c)
{
  struct FD_ACHOICE *ch=(struct FD_ACHOICE *)c;
  if (ch->achoice_data) {
    const fdtype *read=ch->achoice_data, *lim=ch->achoice_write;
    if ((ch->achoice_atomic==0) || (ch->achoice_nested))
      while (read < lim) {
        fdtype v=*read++; fd_decref(v);}
    if (ch->achoice_mallocd) {
      u8_free(ch->achoice_choicedata);
      ch->achoice_choicedata=NULL;
      ch->achoice_mallocd=0;}}
  fd_decref(ch->achoice_normalized);
  u8_destroy_mutex(&(ch->achoice_lock));
  if (!(FD_STATIC_CONSP(ch))) u8_free(ch);
}

static void recycle_achoice_wrapper(struct FD_ACHOICE *ch)
{
  fd_decref(ch->achoice_normalized);
  u8_destroy_mutex(&(ch->achoice_lock));
  if (!(FD_STATIC_CONSP(ch))) u8_free(ch);
}
static int write_achoice_dtype(struct FD_OUTBUF *s,fdtype x)
{
  fdtype sc=fd_make_simple_choice(x);
  int n_bytes=fd_write_dtype(s,sc);
  fd_decref(sc);
  return n_bytes;
}

static fdtype copy_achoice(fdtype x,int deep)
{
  return normalize_choice(x,0);
}

static int unparse_achoice(struct U8_OUTPUT *s,fdtype x)
{
  fdtype sc=fd_make_simple_choice(x);
  fd_unparse(s,sc);
  fd_decref(sc);
  return 1;
}

static int compare_achoice(fdtype x,fdtype y,fd_compare_flags flags)
{
  fdtype sx=fd_make_simple_choice(x), sy=fd_make_simple_choice(y);
  int compare=FDTYPE_COMPARE(sx,sy,flags);
  fd_decref(sx); fd_decref(sy);
  return compare;
}

/* Sorting and compressing choices */

/* We separate the cases of sorting atomic and composite objects,
   since we can do a direct pointer comparison on atomic objects. */

FD_FASTOP void swap(fdtype *a,fdtype *b)
{
  fdtype t;
  t = *a;
  *a = *b;
  *b = t;
}

static int atomic_sortedp(fdtype *v,int n)
{
  int i=0, lim=n-1;
  while (i<lim)
    if (v[i]<v[i+1]) i++; else return 0;
  return 1;
}

static void atomic_sort(fdtype *v,int n)
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

static void cons_sort(fdtype *v,int n)
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
static int compress_choice(fdtype *v,int n,int atomicp)
{
  fdtype *write=v, *scan=v, *limit=v+n, pt;
  if (n == 0) return n;
  pt=*scan++; *write++=pt;
  /* We separate out the atomic and cons cases because we can do
     pointer comparisons for the atomic cases. */
  if (atomicp)
    while (scan < limit)
      if (pt==*scan) {
        scan++;
        while ((scan<limit) && (pt==*scan)) scan++;}
      else *write++=pt=*scan++;
  else while (scan < limit)
    if (cons_compare(pt,*scan)==0) {
      fd_decref(*scan); scan++;
      while ((scan<limit) && (cons_compare(pt,*scan)==0)) {
        fd_decref(*scan); scan++;}}
    else *write++=pt=*scan++;
  return write-v;
}

/* This does a simple binary search of a sorted choice vector,
   looking for a particular element. Once more, we separate out the
   atomic case because it just requires pointer comparisons.  */
static int choice_containsp(fdtype x,struct FD_CHOICE *choice)
{
  int size=FD_XCHOICE_SIZE(choice);
  const fdtype *bottom=FD_XCHOICE_DATA(choice), *top=bottom+(size-1);
  if (FD_ATOMICP(x)) {
    while (top>=bottom) {
      const fdtype *middle=bottom+(top-bottom)/2;
      if (x == *middle) return 1;
      else if (FD_CONSP(*middle)) top=middle-1;
      else if (x < *middle) top=middle-1;
      else bottom=middle+1;}
    return 0;}
  else {
    while (top>=bottom) {
        const fdtype *middle=bottom+(top-bottom)/2;
        int comparison=cons_compare(x,*middle);
        if (comparison == 0) return 1;
        else if (comparison<0) top=middle-1;
        else bottom=middle+1;}
      return 0;}
}

FD_EXPORT
/* fd_choice_containsp:
     Arguments: two dtype pointers
     Returns: 1 or 0
 This returns 1 if the first argument is in the choice represented by
 the second argument.  Note that if the second argument isn't a choice,
 this is the same as FDTYPE_EQUAL. */
int fd_choice_containsp(fdtype key,fdtype x)
{
  if (FD_CHOICEP(x))
    return choice_containsp(key,(struct FD_CHOICE *)x);
  else if (FD_ACHOICEP(x)) {
    int flag=0;
    fdtype sv=fd_make_simple_choice(x);
    if (FD_CHOICEP(sv))
      flag=choice_containsp(key,(struct FD_CHOICE *)sv);
    else if (FDTYPE_EQUAL(key,sv)) flag=1;
    fd_decref(sv);
    return flag;}
  else if (FDTYPE_EQUAL(x,key)) return 1;
  else return 0;
}

FD_EXPORT
fdtype fd_init_choice
  (struct FD_CHOICE *ch,int n,const fdtype *data,int flags)
{
  int atomicp=1, newlen=n;
  const fdtype *base, *scan, *limit;
  if (FD_EXPECT_FALSE((n==0) && (flags&FD_CHOICE_REALLOC))) {
    if (ch) u8_free(ch);
    return FD_EMPTY_CHOICE;}
  else if (n==1) {
    fdtype elt=(data!=NULL)?(data[0]):
      (ch!=NULL)?((FD_XCHOICE_DATA(ch))[0]):
      (FD_NULL);
    if (ch) u8_free(ch);
    if ((data) && (flags&FD_CHOICE_FREEDATA)) u8_free(data);
    if (elt==FD_NULL) {
      fd_seterr(_("BadInitData"),"fd_init_choice",NULL,FD_VOID);
      return FD_ERROR_VALUE;}
    else {
      return elt;}}
  else if (ch==NULL) {
    ch=fd_alloc_choice(n);
    if (ch==NULL) {
      u8_graberr(-1,"fd_init_choice",NULL);
      return FD_ERROR_VALUE;}
    if (data)
      memcpy((fdtype *)FD_XCHOICE_DATA(ch),data,sizeof(fdtype)*n);
    else {
      fdtype *write=&(ch->choice_0), *writelim=write+n;
      while (write<writelim) *write++=FD_VOID;}}
  else if ((data) && (data!=FD_XCHOICE_DATA(ch))) {
    if (flags&FD_CHOICE_INCREF) {
      fdtype *write=(fdtype *)FD_XCHOICE_DATA(ch);
      fdtype *read=(fdtype *)data, *limit=read+n;
      while (read<limit) {
        fdtype v=fd_incref(*read); read++; *write++=v;}}
    else memcpy((fdtype *)FD_XCHOICE_DATA(ch),data,sizeof(fdtype)*n);}
  /* Copy the data unless its yours. */
  base=FD_XCHOICE_DATA(ch); scan=base; limit=scan+n;
  /* Determine if the choice is atomic. */
  if (flags&FD_CHOICE_ISATOMIC) atomicp=1;
  else if (flags&FD_CHOICE_ISCONSES) atomicp=0;
  else while (scan<limit)
    if (FD_ATOMICP(*scan)) scan++; else {atomicp=0; break;}
  /* Now sort and compress it if requested */
  if (flags&FD_CHOICE_DOSORT) {
    if (atomicp) atomic_sort((fdtype *)base,n);
    else cons_sort((fdtype *)base,n);
    newlen=compress_choice((fdtype *)base,n,atomicp);}
  else if (flags&FD_CHOICE_COMPRESS)
    newlen=compress_choice((fdtype *)base,n,atomicp);
  else newlen=n;
  /* Free the original data vector if requested. */
  if ((data) && (flags&FD_CHOICE_FREEDATA)) {
    u8_free((fdtype *)data);}
  if ((newlen==1) && (flags&FD_CHOICE_REALLOC)) {
    fdtype v=base[0];
    u8_free(ch);
    return v;}
  else if ((flags&FD_CHOICE_REALLOC) && (newlen<(n/2)))
    ch=u8_realloc(ch,sizeof(struct FD_CHOICE)+((newlen-1)*sizeof(fdtype)));
  else {}
  if (ch) {
    FD_INIT_XCHOICE(ch,newlen,atomicp);
    return FDTYPE_CONS(ch);}
  else {
    u8_graberr(-1,"fd_init_choice",NULL);
    return FD_ERROR_VALUE;}
}

FD_EXPORT
/* fd_make_achoice:
     Arguments: two dtype pointers
     Returns: a dtype pointer
   Allocates and initializes an FD_ACHOICE to include the two arguments.
   If the elements are themselves ACHOICEs, they are normalized into
   simple choices.
*/
fdtype fd_make_achoice(fdtype x,fdtype y)
{
  struct FD_ACHOICE *ch=u8_alloc(struct FD_ACHOICE);
  fdtype nx=fd_simplify_choice(x), ny=fd_simplify_choice(y);
  FD_INIT_CONS(ch,fd_achoice_type);
  ch->achoice_choicedata=fd_alloc_choice(64);
  ch->achoice_write=ch->achoice_data=(fdtype *)FD_XCHOICE_DATA(ch->achoice_choicedata);
  ch->achoice_limit=ch->achoice_data+64; ch->achoice_normalized=FD_VOID;
  ch->achoice_mallocd=1; ch->achoice_nested=0; ch->achoice_muddled=0;
  ch->achoice_size=FD_CHOICE_SIZE(nx)+FD_CHOICE_SIZE(ny);
  if (FD_CHOICEP(nx)) ch->achoice_nested++;
  if (FD_CHOICEP(ny)) ch->achoice_nested++;
  if (ch->achoice_nested) {
    ch->achoice_write[0]=nx; ch->achoice_write[1]=ny; ch->achoice_muddled=1;}
  else if (cons_compare(nx,ny)<1) {
    ch->achoice_write[0]=nx; ch->achoice_write[1]=ny;}
  else {ch->achoice_write[0]=ny; ch->achoice_write[1]=nx;}
  ch->achoice_atomic=1;
  if (FD_CONSP(nx)) {
    if ((FD_CHOICEP(nx)) && (FD_ATOMIC_CHOICEP(nx))) {}
    else ch->achoice_atomic=0;}
  if (FD_CONSP(ny)) {
    if ((FD_CHOICEP(ny)) && (FD_ATOMIC_CHOICEP(ny))) {}
    else ch->achoice_atomic=0;}
  ch->achoice_write=ch->achoice_write+2;
  ch->achoice_uselock=1; 
  u8_init_mutex(&(ch->achoice_lock));
  return FDTYPE_CONS(ch);
}

FD_EXPORT
/* fd_init_achoice:
     Arguments: a pointer to an FD_ACHOICE structure, a size, and a flag (int)
     Returns: a dtype pointer
   Initializes an FD_ACHOICE with an initial vector of slots.
   If the flag argument is non-zero, the mutex on the FD_ACHOICE will
   be initialized and used in all operations.  If the pointer argument
   is NULL, an FD_ACHOICE structure is allocated. */
fdtype fd_init_achoice(struct FD_ACHOICE *ch,int lim,int uselock)
{
  if (ch==NULL) ch=u8_alloc(struct FD_ACHOICE);
  FD_INIT_CONS(ch,fd_achoice_type);
  ch->achoice_choicedata=fd_alloc_choice(lim);
  ch->achoice_write=ch->achoice_data=(fdtype *)FD_XCHOICE_DATA(ch->achoice_choicedata);
  ch->achoice_limit=ch->achoice_data+lim; ch->achoice_size=0;
  ch->achoice_nested=0; ch->achoice_muddled=0; ch->achoice_atomic=1; ch->achoice_mallocd=1;
  ch->achoice_normalized=FD_VOID;
  ch->achoice_uselock=uselock;
  u8_init_mutex(&(ch->achoice_lock));
  return FDTYPE_CONS(ch);
}

FD_EXPORT
/* _fd_add_to_choice:
     Arguments: two dtype pointers
     Returns: a dtype pointer
   Combines two dtype pointers into a single lisp value.  This is used
   by the macro FD_ADD_TO_CHOICE when choice operations are not being
   inlined.

*/
fdtype _fd_add_to_choice(fdtype current,fdtype v)
{
  return _add_to_choice(current,v);
}

/* Converting achoices to choices */

/* ACHOICEs are accumulating choices which accumulate values.  ACHOICEs
   have a ->normalized field which, when non-void, is the simple choice
   version of the achoice. */

static fdtype convert_achoice(struct FD_ACHOICE *ch,int freeing_achoice);

static fdtype normalize_choice(fdtype x,int free_achoice)
{
  if (FD_ACHOICEP(x)) {
    struct FD_ACHOICE *ch=
      fd_consptr(struct FD_ACHOICE *,x,fd_achoice_type);
    /* Double check that it's really okay to free it. */
    if (free_achoice) {
      if (FD_CONS_REFCOUNT(ch)>1) {
        free_achoice=0; fd_decref(x);}}
    if (ch->achoice_uselock) lock_achoice(ch);
    /* If you have a normalized value, use it. */
    if (!(FD_VOIDP(ch->achoice_normalized))) {
      fdtype v=fd_incref(ch->achoice_normalized);
      if (ch->achoice_uselock) unlock_achoice(ch);
      if (free_achoice) fd_decref(x);
      return v;}
    /* If it's really empty, just return the empty choice. */
    else if ((ch->achoice_write-ch->achoice_data) == 0) {
      if (ch->achoice_uselock) unlock_achoice(ch);
      if (free_achoice) recycle_achoice((fd_raw_cons)ch);
      else ch->achoice_normalized=FD_EMPTY_CHOICE;
      return FD_EMPTY_CHOICE;}
    /* If it's only got one value, return it. */
    else if ((ch->achoice_write-ch->achoice_data) == 1) {
      fdtype value=fd_incref(*(ch->achoice_data));
      if (ch->achoice_uselock) unlock_achoice(ch);
      if (free_achoice) recycle_achoice((fd_raw_cons)ch);
      ch->achoice_normalized=fd_incref(value);
      return value;}
    /* If you're going to free the achoice and it's not nested, you
       can just use the choice you've been depositing values in,
       appropriately initialized, sorted etc.  */
    else if ((free_achoice) && (ch->achoice_nested==0)) {
      struct FD_CHOICE *nch=ch->achoice_choicedata;
      int flags=FD_CHOICE_REALLOC, n=ch->achoice_size;
      if (ch->achoice_atomic) flags=flags|FD_CHOICE_ISATOMIC;
      else flags=flags|FD_CHOICE_ISCONSES;
      if (ch->achoice_muddled) flags=flags|FD_CHOICE_DOSORT;
      else flags=flags|FD_CHOICE_COMPRESS;
      if (ch->achoice_uselock) unlock_achoice(ch);
      recycle_achoice_wrapper(ch);
      return fd_init_choice(nch,n,NULL,flags);}
    else if (ch->achoice_nested==0) {
      /* If it's not nested, we can mostly just call fd_make_choice. */
      int flags=0, n_elts=ch->achoice_write-ch->achoice_data; fdtype result;
      if (ch->achoice_atomic) flags=flags|FD_CHOICE_ISATOMIC; else {
        /* Incref everything */
        const fdtype *scan=ch->achoice_data, *write=ch->achoice_write;
        while (scan<write) {fd_incref(*scan); scan++;}
        flags=flags|FD_CHOICE_ISCONSES;}
      if (ch->achoice_muddled) flags=flags|FD_CHOICE_DOSORT;
      else flags=flags|FD_CHOICE_COMPRESS;
      result=fd_make_choice(n_elts,ch->achoice_data,flags);
      ch->achoice_normalized=fd_incref(result);
      if (ch->achoice_uselock) unlock_achoice(ch);
      return result;}
    else if (1) { /* (ch->size<fd_mergesort_threshold)  */
      /* If the choice is small enough, we can call convert_achoice,
         which just appends the choices together and relies on sort
         and compression to remove duplicates.  We don't want to do
         this if the choice is really huge, because we don't want to
         sort the huge vector. */
      fdtype converted=convert_achoice(ch,free_achoice);
      if (free_achoice) {
        if (ch->achoice_uselock) unlock_achoice(ch);
        fd_decref(x);
        return converted;}
      else {
        ch->achoice_normalized=fd_incref(converted);
        if (ch->achoice_uselock) unlock_achoice(ch);
        return converted;}}
    else {
      /* This is the hairy case, where you are actually merging a bunch
         of choices.  You create a tmp_choice of all the non choices
         and merge this with all the choice elements.*/
      struct FD_CHOICE **choices, *_choices[16], *tmp_choice;
      const fdtype *scan=ch->achoice_data, *lim=ch->achoice_write; fdtype result;
      fdtype *write;
      int n_entries=ch->achoice_write-ch->achoice_data, n_vals=n_entries-ch->achoice_nested;
      int i=0, n_choices;
      /* Figure out how many choices we need to merge. */
      if (n_vals) n_choices=ch->achoice_nested+1; else n_choices=ch->achoice_nested;
      /* We try to use a stack mallocd choices vector. */
      if (n_choices>16)
        choices=u8_alloc_n(n_choices,struct FD_CHOICE *);
      else choices=_choices;
      /* If we are using a tmp_choice, we initialize it and fill it while copying
         choice elements. */
      if (n_vals) {
        int flags=FD_CHOICE_DOSORT, atomicp=1;
        tmp_choice=fd_alloc_choice(n_vals);
        write=(fdtype *)FD_XCHOICE_DATA(tmp_choice);
        choices[i++]=tmp_choice;
        while (scan < lim)
          if (FD_CHOICEP(*scan)) {
            choices[i++]=(struct FD_CHOICE *)(*scan); scan++;}
          else {
            if ((atomicp) && (!(FD_ATOMICP(*scan)))) atomicp=0;
            *(write++)=*scan++;}
        if (atomicp) flags=flags|FD_CHOICE_ISATOMIC;
        else flags=flags|FD_CHOICE_ISCONSES;
        fd_init_choice(tmp_choice,n_vals,FD_XCHOICE_DATA(tmp_choice),flags);}
      /* Otherwise, just copy the choices into the vector. */
      else while (scan < lim) {
          choices[i++]=(struct FD_CHOICE *)(*scan); scan++;}
      /* This does the merging of the sorted choices. */
      result=fd_merge_choices(choices,n_choices);
      /* We free the tmp_choice if we made it. */
      if ((n_vals)&&(result!=((fdtype)tmp_choice)))
        u8_free((struct FD_CHOICE *)tmp_choice);
      if (n_choices>16) u8_free(choices);
      if (ch->achoice_uselock) unlock_achoice(ch);
      if (free_achoice) {
        recycle_achoice((fd_raw_cons)ch);
        return result;}
      else {
        ch->achoice_normalized=result;
        return fd_incref(result);}}}
  else return x;
}

FD_EXPORT
/* _fd_make_simple_choice:
      Arguments: a dtype pointer
      Returns: a dtype pointer
  This returns a normalized choice for an achoice or an incref'd value
  otherwise. */
fdtype _fd_make_simple_choice(fdtype x)
{
  if (FD_ACHOICEP(x))
    return normalize_choice(x,0);
  else return fd_incref(x);
}

FD_EXPORT
/* _fd_simplify_choice:
      Arguments: a dtype pointer
      Returns: a dtype pointer
  This returns a normalized choice for an ACHOICE, or its argument
  otherwise. */
fdtype _fd_simplify_choice(fdtype x)
{
  if (FD_ACHOICEP(x))
    return normalize_choice(x,1);
  else return x;
}

FD_EXPORT int _fd_choice_size(fdtype x)
{
  return fd_choice_size(x);
}

/* Merging choices */

/* This procedures merge choices by scanning over them linearly, merging
   elements. */

struct FD_CHOICE_SCANNER {
  fdtype top;
  const fdtype *ptr, *lim;};

static int resort_scanners(struct FD_CHOICE_SCANNER *v,int n,int atomic)
{
  const fdtype *ptr=v[0].ptr, *lim=v[0].lim;
  if (ptr==lim) {
    int i=1; while ((i<n) && (v[i].ptr==v[i].lim)) i++;
    memmove(v,v+i,sizeof(struct FD_CHOICE_SCANNER)*(n-i));
    return n-i;}
  else {
    int i=1; fdtype head=v[0].top;
    if (atomic)
      while (i<n) if (head<=v[i].top) break; else i++;
    else while (i<n)
      if (cons_compare(head,v[i].top)<=0) break;
      else i++;
    memmove(v,v+1,sizeof(struct FD_CHOICE_SCANNER)*(i-1));
    v[i-1].top=head; v[i-1].ptr=ptr; v[i-1].lim=lim;
    return n;}
}

static int scanner_loop(struct FD_CHOICE_SCANNER *scanners,
                               int n_scanners,
                               fdtype *vals)
{
  fdtype *write=vals, last=FD_NEVERSEEN;
  while (n_scanners>1) {
    fdtype top=scanners[0].top;
    if (!(FDTYPE_EQUAL(last,top))) {*write++=fd_incref(top); last=top;}
    scanners[0].ptr++;
    if (scanners[0].ptr==scanners[0].lim)
      if (n_scanners==1) n_scanners--;
      else n_scanners=resort_scanners(scanners,n_scanners,0);
    else {
      top=scanners[0].top=scanners[0].ptr[0];
      if (n_scanners==1) {}
      else if (cons_compare(top,scanners[1].top)<0) {}
      else n_scanners=resort_scanners(scanners,n_scanners,0);}}
  if (n_scanners==1) {
    const fdtype top=scanners[0].top, *read, *limit;
    if (FDTYPE_EQUAL(top,last)) scanners[0].ptr++;
    read=scanners[0].ptr; limit=scanners[0].lim;
    while (read<limit) {
      *write=fd_incref(*read); write++; read++;}}
  return write-vals;
}

static int atomic_scanner_loop(struct FD_CHOICE_SCANNER *scanners,
                               int n_scanners,
                               fdtype *vals)
{
  fdtype *write=vals, last=FD_NEVERSEEN;
  while (n_scanners>1) {
    fdtype top=scanners[0].top;
    if (top!=last) {*write++=top; last=top;}
    scanners[0].ptr++;
    if (scanners[0].ptr==scanners[0].lim)
      if (n_scanners==1) n_scanners--;
      else n_scanners=resort_scanners(scanners,n_scanners,1);
    else {
      top=scanners[0].top=scanners[0].ptr[0];
      if (n_scanners==1) {}
      else if (top<scanners[1].top) {}
      else n_scanners=resort_scanners(scanners,n_scanners,1);}}
  if (n_scanners==1) {
    fdtype top=scanners[0].top; int len;
    if (top==last) scanners[0].ptr++;
    len=scanners[0].lim-scanners[0].ptr;
    memcpy(write,scanners[0].ptr,len*sizeof(fdtype));
    write=write+len;}
  return write-vals;
}

static int compare_scanners(const void *x,const void *y)
{
  struct FD_CHOICE_SCANNER *xs=(struct FD_CHOICE_SCANNER *)x;
  struct FD_CHOICE_SCANNER *ys=(struct FD_CHOICE_SCANNER *)y;
  return cons_compare(xs->top,ys->top);
}

static int compare_scanners_atomic(const void *x,const void *y)
{
  struct FD_CHOICE_SCANNER *xs=(struct FD_CHOICE_SCANNER *)x;
  struct FD_CHOICE_SCANNER *ys=(struct FD_CHOICE_SCANNER *)y;
  fdtype xv=xs->top, yv=ys->top;
  if (xv<yv) return -1;
  else if (xv==yv) return 0;
  else return 1;
}

FD_EXPORT
/* fd_merge_choices:
     Arguments: a vector of pointers to choice structures and a length
     Returns: a dtype pointer
  Combines the elements of all of the choices into a single sorted choice.
  It does this in linear time by marching along all the choices together.
  */
fdtype fd_merge_choices(struct FD_CHOICE **choices,int n_choices)
{
  int n_scanners=n_choices, max_space=0, atomicp=1, new_size, flags=0;
  fdtype *write;
  struct FD_CHOICE *new_choice;
  struct FD_CHOICE_SCANNER *scanners=
    u8_alloc_n(n_choices,struct FD_CHOICE_SCANNER);
  /* Initialize the scanners. */
  int i=0; while (i < n_scanners) {
    const fdtype *data=FD_XCHOICE_DATA(choices[i]);
    scanners[i].top=data[0];
    scanners[i].ptr=data;
    scanners[i].lim=data+FD_XCHOICE_SIZE(choices[i]);
    if ((atomicp) && (!(FD_XCHOICE_ATOMICP(choices[i])))) atomicp=0;
    max_space=max_space+FD_XCHOICE_SIZE(choices[i]);
    i++;}
  /* Make a new choice with enough space for everything. */
  new_choice=fd_alloc_choice(max_space);
  /* Where we write */
  write=(fdtype *)FD_XCHOICE_DATA(new_choice);
  if (atomicp)
    qsort(scanners,n_scanners,sizeof(struct FD_CHOICE_SCANNER),
          compare_scanners_atomic);
  else qsort(scanners,n_scanners,sizeof(struct FD_CHOICE_SCANNER),
             compare_scanners);
  if (atomicp)
    new_size=atomic_scanner_loop(scanners,n_scanners,write);
  else new_size=scanner_loop(scanners,n_scanners,write);
  u8_free(scanners);
  if (atomicp) flags=flags|FD_CHOICE_ISATOMIC;
  else flags=flags|FD_CHOICE_ISCONSES;
  return fd_init_choice(new_choice,new_size,
                        FD_XCHOICE_DATA(new_choice),
                        (flags|FD_CHOICE_REALLOC));
}

static
/* convert_achoice simply appends together the component choices and then
   relies on sorting and compression by fd_init_choice to remove duplicates.
   This is a fine approach which the result set is relatively small and
   sorting the resulting choice isn't a big deal. */
fdtype convert_achoice(struct FD_ACHOICE *ch,int freeing_achoice)
{
  struct FD_CHOICE *result=fd_alloc_choice(ch->achoice_size);
  fdtype *base=(fdtype *)FD_XCHOICE_DATA(result);
  fdtype *write=base, *write_limit=base+(ch->achoice_size);
  fdtype *scan=ch->achoice_data, *limit=ch->achoice_write;
  while (scan < limit) {
    fdtype v=*scan;
    if (write>=write_limit) {
      u8_log(LOG_WARN,"achoice_inconsistency",
              "total size is more than the recorded %d",ch->achoice_size);
      abort();}
    else if (FD_CHOICEP(v)) {
      struct FD_CHOICE *each=(struct FD_CHOICE *)v;
      int freed=((freeing_achoice) && (FD_CONS_REFCOUNT(each)==1));
      if (write+FD_XCHOICE_SIZE(each)>write_limit) {
        u8_log(LOG_WARN,"achoice_inconsistency",
                "total size is more than the recorded %d",ch->achoice_size);
        abort();}
      else if (FD_XCHOICE_ATOMICP(each)) {
        memcpy(write,FD_XCHOICE_DATA(each),
               sizeof(fdtype)*FD_XCHOICE_SIZE(each));
        write=write+FD_XCHOICE_SIZE(each);}
      else if (freed) {
        memcpy(write,FD_XCHOICE_DATA(each),
               sizeof(fdtype)*FD_XCHOICE_SIZE(each));
        write=write+FD_XCHOICE_SIZE(each);}
      else {
        const fdtype *vscan=FD_XCHOICE_DATA(each),
          *vlimit=vscan+FD_XCHOICE_SIZE(each);
        while (vscan < vlimit) {
          fdtype ev=*vscan++; *write++=fd_incref(ev);}}
      if (freed) {
        struct FD_CHOICE *ch=(struct FD_CHOICE *)each;
        if (FD_MALLOCD_CONSP(ch)) u8_free(ch);
        *scan=FD_VOID;}}
    else if (freeing_achoice) {
      *write++=v; *scan=FD_VOID;}
    else if (FD_ATOMICP(v)) *write++=v;
    else {*write++=fd_incref(v);}
    scan++;}
  if ((write-base)>1)
    return fd_init_choice(result,write-base,NULL,
                          (FD_CHOICE_DOSORT|
                           ((ch->achoice_atomic)?(FD_CHOICE_ISATOMIC):
                            (FD_CHOICE_ISCONSES))));
  else {
    fdtype v=base[0];
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
fdtype fd_init_qchoice(struct FD_QCHOICE *ptr,fdtype choice)
{
  if (ptr==NULL) ptr=u8_alloc(struct FD_QCHOICE);
  FD_INIT_CONS(ptr,fd_qchoice_type);
  ptr->qchoiceval=choice;
  return FDTYPE_CONS(ptr);
}

static int write_qchoice_dtype(struct FD_OUTBUF *s,fdtype x)
{
  struct FD_QCHOICE *qc=FD_XQCHOICE(x);
  return fd_write_dtype(s,qc->qchoiceval);
}

static int unparse_qchoice(struct U8_OUTPUT *s,fdtype x)
{
  struct FD_QCHOICE *qc=FD_XQCHOICE(x);
  u8_puts(s,"#"); fd_unparse(s,qc->qchoiceval);
  return 1;
}

static fdtype copy_qchoice(fdtype x,int deep)
{
  struct FD_QCHOICE *copied=u8_alloc(struct FD_QCHOICE);
  struct FD_QCHOICE *qc=FD_XQCHOICE(x);
  FD_INIT_CONS(copied,fd_qchoice_type);
  if (deep)
    copied->qchoiceval=fd_deep_copy(qc->qchoiceval);
  else {
    fd_incref(qc->qchoiceval);
    copied->qchoiceval=qc->qchoiceval;}
  return FDTYPE_CONS(copied);
}

static int compare_qchoice(fdtype x,fdtype y,fd_compare_flags flags)
{
  struct FD_QCHOICE *xqc=FD_XQCHOICE(x), *yqc=FD_XQCHOICE(y);
  return (FDTYPE_COMPARE(xqc->qchoiceval,yqc->qchoiceval,flags));
}

/* Set operations (intersection, union, difference) on choices */

static int compare_choicep_size(const void *cx,const void *cy)
{
  int xsz=FD_XCHOICE_SIZE((*((struct FD_CHOICE **)cx)));
  int ysz=FD_XCHOICE_SIZE((*((struct FD_CHOICE **)cy)));
  if (xsz<ysz) return -1;
  else if (xsz==ysz) return 0;
  else return 1;
}

FD_EXPORT
/* fd_intersect_choices:
    Arguments: a pointer to a vector of pointers to choice structures
     and its length (an int)
    Returns: the intersection of the choices.
 */
fdtype fd_intersect_choices(struct FD_CHOICE **choices,int n_choices)
{
  fdtype *results, *write; int max_results, atomicp=1;
  qsort(choices,n_choices,sizeof(struct FD_CHOICE *),
        compare_choicep_size);
  max_results=FD_XCHOICE_SIZE(choices[0]);
  write=results=u8_alloc_n(max_results,fdtype);
  {
    const fdtype *scan=FD_XCHOICE_DATA(choices[0]);
    const fdtype *limit=scan+max_results;
    while (scan<limit) {
      fdtype item=*scan++; int i=1; while (i < n_choices)
        if (choice_containsp(item,choices[i])) i++;
        else break;
      if (i==n_choices) {
        if (FD_CONSP(item)) {
          atomicp=0; fd_incref(item); *write++=item;}
        else *write++=item;}}}
  if (write==results) {
    u8_free(results);
    return FD_EMPTY_CHOICE;}
  else if (write==(results+1)) {
    fdtype v=results[0]; u8_free(results);
    return v;}
  else if (atomicp)
    return fd_make_choice(write-results,results,
                          (FD_CHOICE_ISATOMIC|FD_CHOICE_FREEDATA));
  else return fd_make_choice(write-results,results,
                             FD_CHOICE_FREEDATA);
}

FD_EXPORT
/* fd_intersection:
     Arguments: a pointer to a vector of lisp elements
       and the length of the vector
     Returns: a dtype pointer
  Computes the intersection of a set of choices.
 */
fdtype fd_intersection(fdtype *v,int n)
{
  if (n == 0) return FD_EMPTY_CHOICE;
  else if (n == 1) return fd_incref(v[0]);
  else {
    fdtype result=FD_VOID;
    /* The simplest case is where one or more of the inputs
       is a singleton.  If more than one is a singleton and they
       are different, we just return the empty set.  If they're the same,
       we'll just do membership tests on that one item.

       To resolve this, we scan all the items being intersected,
       using result to store the singleton value, with FD_VOID
       indicating we haven't stored anything there. */
    int i=0, achoices=0; while (i < n)
      if (FD_EMPTY_CHOICEP(v[i]))
        /* If you find any empty, the intersection is empty. */
        return FD_EMPTY_CHOICE;
      else if (FD_CHOICEP(v[i])) i++; /* Pass any choices */
      else if (FD_ACHOICEP(v[i])) {
        /* Count the achoices, because you'll have to convert them. */
        i++; achoices++;}
    /* After this point, we know we have a singleton value. */
      else if (FD_VOIDP(result))
        /* This must be our first singleton. */
        result=v[i++];
      else if (FDTYPE_EQUAL(result,v[i]))
        /* This is a consistent singleton */
        i++;
    /* If it's not a consistent singleton, we can just return the
       empty choice. */
      else return FD_EMPTY_CHOICE;
    if (!(FD_VOIDP(result))) {
      /* This is the case where we found a singleton. */
      int i=0; while (i < n)
        if (FD_CHOICEP(v[i]))
          if (fd_choice_containsp(result,v[i])) i++;
          else return FD_EMPTY_CHOICE;
        else if (FD_ACHOICEP(v[i])) {
          /* Arguably, it might not make sense to do this conversion
             right now. */
          fdtype sc=fd_make_simple_choice(v[i]);
          if (fd_choice_containsp(result,sc)) {
            fd_decref(sc); i++;}
          else {
            fd_decref(sc);
            return FD_EMPTY_CHOICE;}}
        else i++;
      return fd_incref(result);}
    /* At this point, all we have is choices and achoices, so we need to
       make a vector of the choices on which to call fd_intersect_choices.
       We also need to keep track of the values we convert so that
       we can clean them up when we're done. */
    else if (achoices) {
      /* This is the case where we have choices to convert. */
      struct FD_CHOICE **choices, *_choices[16];
      fdtype *conversions, _conversions[16];
      fdtype result=FD_VOID; int i, n_choices=0, n_conversions=0;
      /* Try to use a stack vector if possible. */
      if (n>16) {
        choices=u8_alloc_n(n,struct FD_CHOICE *);
        conversions=u8_alloc_n(n,fdtype);}
      else {choices=_choices; conversions=_conversions;}
      i=0; while (i < n)
        /* We go down doing conversions.  Note that we do the same thing
           as above with handling singletons because the ACHOICEs might
           resolve to singletons. */
        if (FD_CHOICEP(v[i])) {
          choices[n_choices++]=(struct FD_CHOICE *)v[i++];}
        else if (FD_ACHOICEP(v[i])) {
          fdtype nc=fd_make_simple_choice(v[i++]);
          if (FD_CHOICEP(nc)) {
            choices[n_choices++]=(struct FD_CHOICE *)nc;
            conversions[n_conversions++]=nc;}
          /* These are all in case an ACHOICE turns out to be
             empty or a singleton */
          else if (FD_EMPTY_CHOICEP(nc)) break;
          else if (FD_VOIDP(result)) result=nc;
          else if (FDTYPE_EQUAL(result,nc)) i++;
          else {
            /* We record it because we converted it. */
            if (FD_CONSP(nc)) conversions[n_conversions++]=nc;
            result=FD_EMPTY_CHOICE; break;}}
        else i++;
      if (FD_VOIDP(result))
        /* The normal case, just do the intersection */
        result=fd_intersect_choices(choices,n_choices);
      /* One of the achoices turned out to be empty */
      else if (FD_EMPTY_CHOICEP(result)) {}
      else {
        /* One of the achoices turned out to be a singleton */
        int k=0; while (k < n_choices)
          if (choice_containsp(result,choices[k])) k++;
          else {result=FD_EMPTY_CHOICE; break;}}
      /* Now, clean up your conversions. */
      i=0; while (i < n_conversions) {
        fd_decref(conversions[i]); i++;}
      if (n>16) {u8_free(choices); u8_free(conversions);}
      return result;}
    else {
      struct FD_CHOICE **choices, *_choices[16];
      fdtype result; int i=0;
      if (n>16)
        choices=u8_alloc_n(n,struct FD_CHOICE *);
      else choices=_choices;
      while (i < n) {choices[i]=(struct FD_CHOICE *)v[i]; i++;}
      result=fd_intersect_choices(choices,n);
      if (n>16) u8_free(choices);
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
fdtype fd_union(fdtype *v,int n)
{
  if (n == 0) return FD_EMPTY_CHOICE;
  else {
    fdtype result=FD_EMPTY_CHOICE; int i=0;
    while (i < n) {
      fdtype elt=v[i++]; fd_incref(elt);
      FD_ADD_TO_CHOICE(result,elt);}
    return fd_simplify_choice(result);}
}

static fdtype compute_choice_difference
  (struct FD_CHOICE *whole,struct FD_CHOICE *part)
{
  int wsize=FD_XCHOICE_SIZE(whole), psize=FD_XCHOICE_SIZE(part);
  int watomicp=FD_XCHOICE_ATOMICP(whole), patomicp=FD_XCHOICE_ATOMICP(part);
  const fdtype *wscan=FD_XCHOICE_DATA(whole), *wlim=wscan+wsize;
  const fdtype *pscan=FD_XCHOICE_DATA(part), *plim=pscan+psize;
  struct FD_CHOICE *result=fd_alloc_choice(wsize);
  fdtype *newv=(fdtype *)FD_XCHOICE_DATA(result), *write=newv;
  /* An additional optimization here might quit as soon as we start getting
     into nonatomic elements of PART while WHOLE is all atomic. */
  if ((watomicp) && (patomicp))
    while ((wscan<wlim) && (pscan<plim))
      if (*wscan == *pscan) {wscan++; pscan++;}
      else if (*wscan < *pscan) {
        *write=*wscan; write++; wscan++;}
      else pscan++;
  else while ((wscan<wlim) && (pscan<plim))
    if (FDTYPE_EQUAL(*wscan,*pscan)) {wscan++; pscan++;}
    else if (cons_compare(*wscan,*pscan)<0) {
      *write=fd_incref(*wscan); write++; wscan++;}
    else pscan++;
  if (watomicp)
    while (wscan < wlim) {*write=*wscan; write++; wscan++;}
  else while (wscan < wlim)
    {*write=fd_incref(*wscan); write++; wscan++;}
  if (write-newv>1)
    return fd_init_choice(result,write-newv,NULL,
                          ((watomicp)?(FD_CHOICE_ISATOMIC):(0)));
  else if (write==newv) {
    u8_free(result);
    return FD_EMPTY_CHOICE;}
  else {
    fdtype singleton=newv[0];
    u8_free(result);
    return singleton;}
}

FD_EXPORT
/* fd_difference:
     Arguments: two dtype pointers
     Returns: a dtype pointer
  Computes the difference of two sets of choices.
*/
fdtype fd_difference(fdtype value,fdtype remove)
{
  if (FD_EMPTY_CHOICEP(value)) return value;
  else if (FD_EMPTY_CHOICEP(remove)) return fd_incref(value);
  else if ((FD_ACHOICEP(value)) || (FD_ACHOICEP(remove))) {
    fdtype svalue=fd_make_simple_choice(value), sremove=fd_make_simple_choice(remove);
    fdtype result=fd_difference(svalue,sremove);
    fd_decref(svalue); fd_decref(sremove);
    return result;}
  else if (FD_CHOICEP(value))
    if (!(FD_CHOICEP(remove)))
      if (fd_choice_containsp(remove,value)) {
        struct FD_CHOICE *vchoice=
          fd_consptr(struct FD_CHOICE *,value,fd_choice_type);
        int size=FD_XCHOICE_SIZE(vchoice), atomicp=FD_XCHOICE_ATOMICP(vchoice);
        if (size==1) 
          return FD_EMPTY_CHOICE;
        else if (size==2)
          if (FDTYPE_EQUAL(remove,(FD_XCHOICE_DATA(vchoice))[0]))
            return fd_incref((FD_XCHOICE_DATA(vchoice))[1]);
          else return fd_incref((FD_XCHOICE_DATA(vchoice))[0]);
        else {
          struct FD_CHOICE *new_choice=fd_alloc_choice(size-1);
          fdtype *newv=(fdtype *)FD_XCHOICE_DATA(new_choice), *write=newv;
          const fdtype *read=FD_XCHOICE_DATA(vchoice), *lim=read+size;
          int flags=((atomicp)?(FD_CHOICE_ISATOMIC):(0))|FD_CHOICE_REALLOC;
          while (read < lim)
            if (FDTYPE_EQUAL(*read,remove)) read++;
            else {*write=fd_incref(*read); write++; read++;}
          return fd_init_choice(new_choice,size-1,newv,flags);}}
      else return fd_incref(value);
    else return compute_choice_difference
           (fd_consptr(struct FD_CHOICE *,value,fd_choice_type),
            fd_consptr(struct FD_CHOICE *,remove,fd_choice_type));
  else if (FD_CHOICEP(remove))
    if (fd_choice_containsp(value,remove))
      return FD_EMPTY_CHOICE;
    else return fd_incref(value);
  else if (FDTYPE_EQUAL(value,remove)) return FD_EMPTY_CHOICE;
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
int fd_overlapp(fdtype xarg,fdtype yarg)
{
  if (FD_EMPTY_CHOICEP(xarg)) return 0;
  else if (FD_EMPTY_CHOICEP(yarg)) return 0;
  else {
    fdtype x, y; int retval=0;
    if (FD_ACHOICEP(xarg)) x=normalize_choice(xarg,0); else x=xarg;
    if (FD_ACHOICEP(yarg)) y=normalize_choice(yarg,0); else y=yarg;
    if (FD_CHOICEP(x))
      if (FD_CHOICEP(y))
        if (FD_CHOICE_SIZE(x)>FD_CHOICE_SIZE(y)) {
          FD_DO_CHOICES(elt,y)
            if (choice_containsp(elt,(fd_choice)x)) {retval=1; break;}}
        else {
          FD_DO_CHOICES(elt,x)
            if (choice_containsp(elt,(fd_choice)y)) {retval=1; break;}}
      else retval=choice_containsp(y,(fd_choice)x);
    else if (FD_CHOICEP(y)) retval=choice_containsp(x,(fd_choice)y);
    else retval=FDTYPE_EQUAL(x,y);
    if (FD_ACHOICEP(xarg)) fd_decref(x);
    if (FD_ACHOICEP(yarg)) fd_decref(y);
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
int fd_containsp(fdtype xarg,fdtype yarg)
{
  if (FD_EMPTY_CHOICEP(xarg)) return 0;
  else if (FD_EMPTY_CHOICEP(yarg)) return 0;
  else {
    fdtype x, y; int retval=0;
    if (FD_ACHOICEP(xarg)) x=normalize_choice(xarg,0); else x=xarg;
    if (FD_ACHOICEP(yarg)) y=normalize_choice(yarg,0); else y=yarg;
    if (FD_CHOICEP(x))
      if (FD_CHOICEP(y)) {
        int contained=1;
        FD_DO_CHOICES(elt,x)
          if (choice_containsp(elt,(fd_choice)y)) {}
          else {
            contained=0; FD_STOP_DO_CHOICES; break;}
        if (contained) retval=1;}
      else retval=0;
    else if (FD_CHOICEP(y)) retval=choice_containsp(x,(fd_choice)y);
    else retval=FDTYPE_EQUAL(x,y);
    if (FD_ACHOICEP(xarg)) fd_decref(x);
    if (FD_ACHOICEP(yarg)) fd_decref(y);
    return retval;}
}

/* Natsorting CHOICES */

FD_EXPORT
fdtype *fd_natsort_choice(fd_choice ch,fdtype *tmpbuf,ssize_t tmp_len)
{
  int len=FD_XCHOICE_SIZE(ch);
  const fdtype *data=FD_XCHOICE_DATA(ch);
  fdtype *natsorted=(tmp_len>len) ? (tmpbuf) : u8_alloc_n(len,fdtype);
  memcpy(natsorted,data,len*sizeof(fdtype));
  fdtype_sort(natsorted,len,FD_COMPARE_NATSORT);
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

  fd_type_names[fd_achoice_type]="achoice";
  fd_recyclers[fd_achoice_type]=recycle_achoice;
  fd_dtype_writers[fd_achoice_type]=write_achoice_dtype;
  fd_comparators[fd_achoice_type]=compare_achoice;
  fd_comparators[fd_choice_type]=compare_achoice;
  fd_unparsers[fd_achoice_type]=unparse_achoice;
  fd_copiers[fd_achoice_type]=copy_achoice;

  fd_type_names[fd_choice_type]="choice";
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
