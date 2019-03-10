/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Choices in FramerD

   Choices are a central datatype in FramerD and used throughout
   the representation, inference, and scripting components.  As data
   structures, choices are basically *sets*: unordered collections of
   elements without duplication.  They are called choices because of
   their central role in inference and non-deterministic evaluation,
   where they describe alternate compute paths.

   As implemented in FramerD, there are two basic kinds of choices:
   simple choices and accumulating choices.  Simple choices are immutable
   collections of lisp objects, represented by simple vectors;
   accumulating choices are more complex structures designed to support
   fast addition of new items while providing for operationally fast
   conversion to simple choices.  In practice, accumulating choices are
   almost never returned as values or passed as parameters, but are
   almost always converted into simple choices.

   With respect to most of the choice related functions, non-choice
   objects are treated as *singleton choices* with only one element.

   The interpreter introduces an additional category of "quoted choices"
   which are simple choices (or the constant empty choice) wrapped in a
   structure which inhibits automatic iteration by the interpeter.

   SIMPLE CHOICES

   Simple choices are a block of memory starting with the standard CONS
   header, followed by a size field (an int), and further followed by the
   elements of the choice.  The choice and its elements are allocated in
   a block to improve page and cache locality.

   If all the elements of a simple choice are atomic (i.e. not CONSes
   needing referencing or dereferencing), the high order bit of the size
   field is set to one (the macros for accessing simple choices
   automatically mask out this bit).  Such choices are called *atomic
   choices* and are much faster to work with than regular choices.

   The elements of simple choices are canonically ordered for the current
   memory image.  Atoms are sorted by their integer pointer value and
   precede conses.  Conses are sorted first by their numeric typecode and
   then using their type-specific comparator functions.

   The ordering of simple choices allows many operations to execute very
   quickly.  For example, membership can be determined in log time by a
   binary search, M-way unions can be done in MxN linear time by
   advancing along each choice in parallel.

   PRECHOICEs

   Prechoices are designed to support the fast addition of new
   elements to a choice and fast conversion to simple choices.  The fast
   conversion is supported by (a) caching conversion results on the
   PRECHOICE and (b) keeping various state variables to determine which
   algorithm to use for a conversion.

   Because one common usage pattern is the conversion and dereferencing
   of a PRECHOICE, special consideration is taken for optimizations
   possible during *destructive conversion*.  For example, this is
   routine in the interpreter, where a given non-deterministic function
   call accumulates results in a PRECHOICE but then converts the prechoice
   to return the values.  Destructive conversion can be faster than
   normal conversion for a variety of reasons, but especially because it
   is possible to avoid redundant incref/decref activity when copying
   values from the prechoice into the choice proper.

   Any object can be added to a PRECHOICE, but adding a PRECHOICE to an
   PRECHOICE forces the prechoice to be converted first.  In particular,
   CHOICEs can be added to PRECHOICEs and are added all at once.  During
   accumulation, the PRECHOICE keeps track of the following state
   variables:
    * size: the maximum number of basic (non-choice) elements in the
       PRECHOICE.  This bounds the size of any converted CHOICE but is
       not exact because we do not check for overlapping elements among
       component choices.
    * atomicity: whether any of the basic (non-choice) elements of the PRECHOICE
       are CONSes
    * nestings: how many of the elements of the PRECHOICE are choices
    * muddling: whether the elements of the PRECHOICE are disordered, i.e.
       whether they obey the ordering criteria for simple choices.

   These properties are used to select the optimal algorithm for
   converting a PRECHOICE into a simple choice.  Adding a value to an
   PRECHOICE automatically updates these properties.  In addition, the
   general way of adding an item (FD_ADD_TO_CHOICE) handles the special
   case of adding the same object repeatedly by converting such identical
   additions into no-ops.  (Note that because FD_ADD_TO_CHOICE consumes
   its argument by contract, the "no-op" may require an implicit
   dereference.)

*/

#ifndef FRAMERD_CHOICES_H
#define FRAMERD_CHOICES_H 1
#ifndef FRAMERD_CHOICES_H_INFO
#define FRAMERD_CHOICES_H_INFO "include/framerd/"
#endif

#ifndef FD_FAST_CHOICE_CONTAINSP
#define FD_FAST_CHOICE_CONTAINSP 0
#endif

/* Choices */

typedef struct FD_CHOICE {
  FD_CONS_HEADER;
  unsigned int choice_size:31;
  unsigned int choice_isatomic:1;
  lispval choice_0;} FD_CHOICE;
typedef struct FD_CHOICE *fd_choice;

#define FD_CHOICE_BYTES (sizeof(struct FD_CHOICE))

#define FD_CHOICE_SIZE_MASK 0x7FFFFFFF
#define FD_CHOICEP(x) (FD_TYPEP((x),fd_choice_type))
#define FD_XCHOICE(x) (fd_consptr(struct FD_CHOICE *,(x),fd_choice_type))
#define FD_XCHOICE_DATA(ch) ((const lispval *) (&((ch)->choice_0)))
#define FD_XCHOICE_ELTS(ch) ((const lispval *) (&((ch)->choice_0)))
#define FD_CHOICE_DATA(x)   (FD_XCHOICE_DATA(FD_CONSPTR(fd_choice,(x))))
#define FD_CHOICE_ELTS(x)   (FD_XCHOICE_DATA(FD_CONSPTR(fd_choice,(x))))
#define FD_XCHOICE_SIZE(ch) ((ch)->choice_size)

#define FD_ATOMIC_CHOICEP(x) ((FD_XCHOICE(x))->choice_isatomic)
#define FD_XCHOICE_ATOMICP(x) ((x)->choice_isatomic)

#define FD_INIT_XCHOICE(ch,sz,atomicp) \
  FD_INIT_CONS(ch,fd_choice_type); \
  ch->choice_size=sz; ch->choice_isatomic=atomicp

FD_FASTOP struct FD_CHOICE *fd_alloc_choice(int n_choices)
{
  assert(n_choices>0);
  size_t base_size = FD_CHOICE_BYTES;
  size_t extra_elts = (n_choices-1)*LISPVAL_LEN;
  return u8_big_alloc(base_size+extra_elts);
}
FD_FASTOP void fd_free_choice(struct FD_CHOICE *ch)
{
  u8_big_free(ch);
}

/* Flags to pass to fd_init_choice (and fd_make_choice) */

/* Sort the results into canonical order. */
#define FD_CHOICE_DOSORT 1
/* Compress (remove identical elements) from the choice.
   This is implied by FD_CHOICE_DOSORT. */
#define FD_CHOICE_COMPRESS 2
/* The choice is atomic: no elements are conses. */
#define FD_CHOICE_ISATOMIC 4
/* The choice is not atomic: some elements are conses. */
#define FD_CHOICE_ISCONSES 8
/* Free the data pointer when finished. */
#define FD_CHOICE_FREEDATA 16
/* Free or realloc the pointer if neccessary. */
#define FD_CHOICE_REALLOC  32
/* Incref data elements when copying */
#define FD_CHOICE_INCREF  64

FD_EXPORT lispval fd_init_choice
  (struct FD_CHOICE *ch,int n,const lispval *data,int flags);
FD_FASTOP lispval fd_make_choice(int n,const lispval *data,int flags)
{
  struct FD_CHOICE *ch = fd_alloc_choice(n);
  return fd_init_choice(ch,n,data,flags);
}

/* Accumulating choices */

typedef struct FD_PRECHOICE {
  FD_CONS_HEADER;
  unsigned int prechoice_size, prechoice_nested;
  unsigned int prechoice_muddled:1, prechoice_mallocd:1,
    prechoice_atomic:1, prechoice_uselock:1;
  lispval *prechoice_data, *prechoice_write, *prechoice_limit;
  lispval prechoice_normalized;
  struct FD_CHOICE *prechoice_choicedata;
#if U8_THREADS_ENABLED
  u8_mutex prechoice_lock;
#endif
} FD_PRECHOICE;
typedef struct FD_PRECHOICE *fd_prechoice;

#define FD_PRECHOICE_BYTES (sizeof(struct FD_PRECHOICE))

#define FD_PRECHOICEP(x) (FD_TYPEP((x),fd_prechoice_type))
#define FD_XPRECHOICE(x) (FD_CONSPTR(fd_prechoice,x))
#define FD_PRECHOICE_SIZE(x) ((FD_XPRECHOICE(x))->prechoice_size)
#define FD_PRECHOICE_LENGTH(x) \
  (((FD_XPRECHOICE(x))->prechoice_write)-((FD_XPRECHOICE(x))->prechoice_data))
FD_EXPORT lispval fd_make_prechoice(lispval x,lispval y);
FD_EXPORT lispval fd_init_prechoice(struct FD_PRECHOICE *ch,int lim,int uselock);
FD_EXPORT struct FD_CHOICE *fd_cleanup_choice(struct FD_CHOICE *ch,unsigned int flags);
FD_EXPORT lispval _fd_add_to_choice(lispval current,lispval add);
FD_EXPORT lispval fd_merge_choices(struct FD_CHOICE **choices,int n_choices);
FD_EXPORT int _fd_choice_size(lispval x);
FD_EXPORT lispval _fd_make_simple_choice(lispval x);
FD_EXPORT lispval _fd_simplify_choice(lispval x);

#define FD_AMBIGP(x)   ((FD_CHOICEP(x))||(FD_PRECHOICEP(x)))
#define FD_UNAMBIGP(x) (!(FD_AMBIGP(x)))

#define FD_CHOICE_SIZE(x) \
  ((FD_EMPTY_CHOICEP(x)) ? (0) : \
   (!(FD_CONSP(x))) ? (1) : \
   (FD_CHOICEP(x)) ? (FD_XCHOICE_SIZE(FD_XCHOICE(x))) : \
   (FD_PRECHOICEP(x)) ? (FD_PRECHOICE_SIZE(x)) : (1))

#if FD_INLINE_CHOICES
static U8_MAYBE_UNUSED int fd_choice_size(lispval x)
{
  if (FD_EMPTY_CHOICEP(x)) return 0;
  else if (!(FD_CONSP(x))) return 1;
  else if (FD_CHOICEP(x))
    return FD_XCHOICE_SIZE(FD_XCHOICE(x));
  else if (FD_PRECHOICEP(x))
    return FD_PRECHOICE_SIZE(x);
  else return 1;
}
static U8_MAYBE_UNUSED lispval fd_simplify_choice(lispval x)
{
  if (FD_PRECHOICEP(x))
    return _fd_simplify_choice(x);
  else return x;
}
static U8_MAYBE_UNUSED lispval fd_make_simple_choice(lispval x)
{
  if (FD_PRECHOICEP(x))
    return _fd_make_simple_choice(x);
  else return fd_incref(x);
}
#else
#define fd_choice_size(x) _fd_choice_size(x)
#define fd_simplify_choice(x) _fd_simplify_choice(x)
#define fd_make_simple_choice(x) _fd_make_simple_choice(x)
#endif

#define FD_SIMPLIFY_CHOICE(ref) ((ref)=fd_simplify_choice(ref))

#ifndef FD_CHOICEMERGE_THRESHOLD
#define FD_CHOICEMERGE_THRESHOLD 32
#endif

FD_EXPORT ssize_t fd_choicemerge_threshold;

/* Quoted choices */

typedef struct FD_QCHOICE {
  FD_CONS_HEADER;
  lispval qchoiceval;} FD_QCHOICE;
typedef struct FD_QCHOICE *fd_qchoice;

FD_EXPORT lispval fd_init_qchoice(struct FD_QCHOICE *ptr,lispval choice);
FD_EXPORT lispval fd_make_qchoice(lispval val);

#define FD_QCHOICEP(x) (FD_TYPEP((x),fd_qchoice_type) )
#define FD_EMPTY_QCHOICEP(x) \
  ((FD_TYPEP((x),fd_qchoice_type)) &&                                 \
   (((fd_consptr(struct FD_QCHOICE *,x,fd_qchoice_type))->qchoiceval) \
    == (FD_EMPTY_CHOICE)))
#define FD_XQCHOICE(x) (fd_consptr(struct FD_QCHOICE *,x,fd_qchoice_type))
#define FD_QCHOICE_SIZE(x) (FD_CHOICE_SIZE(FD_XQCHOICE(x)->qchoiceval))

/* Generic choice operations */

#if FD_INLINE_CHOICES
static void _prechoice_add(struct FD_PRECHOICE *ch,lispval v)
{
  int old_size, new_size, write_off, comparison;
  lispval nv;
  if (FD_PRECHOICEP(v)) {
    nv=fd_simplify_choice(v);}
  else nv=v;
  if (FD_EMPTY_CHOICEP(nv)) return;
  else if (ch->prechoice_write>ch->prechoice_data)
    if (FD_EQ(nv,*(ch->prechoice_write-1))) comparison=0;
    else comparison=cons_compare(*(ch->prechoice_write-1),nv);
  else comparison=1;
  if (comparison==0) {fd_decref(nv); return;}
  if (ch->prechoice_uselock) u8_lock_mutex(&(ch->prechoice_lock));
  if (ch->prechoice_write >= ch->prechoice_limit) {
    struct FD_CHOICE *prechoice_choicedata;
    old_size  = ch->prechoice_limit-ch->prechoice_data;
    write_off = ch->prechoice_write-ch->prechoice_data;
    new_size=old_size*2;
    prechoice_choicedata=
      u8_big_realloc(ch->prechoice_choicedata,
                     FD_CHOICE_BYTES+(LISPVEC_BYTELEN(new_size-1)));
    ch->prechoice_choicedata=prechoice_choicedata;
    ch->prechoice_data=((lispval *)FD_XCHOICE_DATA(prechoice_choicedata));
    ch->prechoice_write=ch->prechoice_data+write_off;
    ch->prechoice_limit=ch->prechoice_data+new_size;}
  *(ch->prechoice_write++)=nv;
  fd_decref(ch->prechoice_normalized);
  ch->prechoice_normalized=VOID;
  if (comparison>0) ch->prechoice_muddled=1;
  if (FD_CHOICEP(nv)) {
    ch->prechoice_nested++;
    ch->prechoice_muddled=1;
    if (ch->prechoice_atomic)
      if (!(FD_ATOMIC_CHOICEP(nv))) ch->prechoice_atomic=0;
    ch->prechoice_size=ch->prechoice_size+FD_CHOICE_SIZE(nv);}
  else if ((ch->prechoice_atomic) && (FD_CONSP(nv))) {
    ch->prechoice_size++; ch->prechoice_atomic=0;}
  else ch->prechoice_size++;
  if (ch->prechoice_uselock)
    u8_unlock_mutex(&(ch->prechoice_lock));
}
static U8_MAYBE_UNUSED lispval _add_to_choice(lispval current,lispval new)
{
  FD_PTR_CHECK1(new,"_add_to_choice");
  if (FD_EMPTY_CHOICEP(new)) return current;
  else if ((FD_CONSP(new)) && (FD_STATIC_CONSP(new)))
    new=fd_copy(new);
  if (FD_EMPTY_CHOICEP(current))
    if (!(FD_CONSP(new)))
      return new;
    else if (FD_PRECHOICEP(new))
      if ((FD_CONS_REFCOUNT(((struct FD_CONS *)new)))>1)
        return fd_simplify_choice(new);
      else return new;
    else return new;
  else if (current==new) {
    fd_decref(new);
    return current;}
  else if (FD_PRECHOICEP(current)) {
    _prechoice_add((struct FD_PRECHOICE *)current,new);
    return current;}
  else if (LISP_EQUAL(current,new)) {
    fd_decref(new);
    return current;}
  else return fd_make_prechoice(current,new);
}
#define FD_ADD_TO_CHOICE(x,v) x=_add_to_choice(x,v)
/* This does a simple binary search of a sorted choice vector made up,
   solely of atoms.  */
static U8_MAYBE_UNUSED int atomic_choice_containsp(lispval x,lispval ch)
{
  if (FD_ATOMICP(ch)) return (x==ch);
  else {
    struct FD_CHOICE *qchoiceval=fd_consptr(fd_choice,ch,fd_choice_type);
    int prechoice_size=FD_XCHOICE_SIZE(qchoiceval);
    const lispval *bottom=FD_XCHOICE_DATA(qchoiceval), *top=bottom+(prechoice_size-1);
    while (top>=bottom) {
      const lispval *middle=bottom+(top-bottom)/2;
      if (x == *middle) return 1;
      else if (FD_CONSP(*middle)) top=middle-1;
      else if (x < *middle) top=middle-1;
      else bottom=middle+1;}
    return 0;}
}
#else
#define FD_ADD_TO_CHOICE(x,v)                    \
   if (FD_DEBUG_BADPTRP(v))                      \
     _fd_bad_pointer(v,(u8_context)"FD_ADD_TO_CHOICE"); \
   else x=_fd_add_to_choice(x,v)
#endif

#if FD_FAST_CHOICE_CONTAINSP
/* This does a simple binary search of a sorted choice vector,
   looking for a particular element. Once more, we separate out the
   atomic case because it just requires pointer comparisons.  */
static U8_MAYBE_UNUSED int fast_choice_containsp(lispval x,struct FD_CHOICE *choice)
{
  int size = FD_XCHOICE_SIZE(choice);
  const lispval *bottom = FD_XCHOICE_DATA(choice), *top = bottom+(size-1);
  if (FD_ATOMICP(x)) {
    while (top>=bottom) {
      const lispval *middle = bottom+(top-bottom)/2;
      if (x == *middle) return 1;
      else if (FD_CONSP(*middle)) top = middle-1;
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
#endif

#define FD_DO_CHOICES(elt,valexpr) \
  lispval elt, _val=valexpr, _singlev[1]; \
  const lispval *_scan, *_limit;          \
  int _need_gc=0; \
  FD_PTR_CHECK1(_val,"FD_DO_CHOICES");              \
  if (FD_PRECHOICEP(_val)) {\
    _need_gc=1; _val=fd_make_simple_choice(_val);} \
   if (FD_CHOICEP(_val)) {\
    _scan=FD_CHOICE_DATA(_val); _limit=_scan+FD_CHOICE_SIZE(_val);} \
   else if (FD_EMPTY_CHOICEP(_val)) { \
     _scan=_singlev+1; _limit=_scan;}  \
   else if (FD_QCHOICEP(_val)) { \
     _singlev[0] = FD_XQCHOICE(_val)->qchoiceval; \
     _val = _singlev[0]; fd_incref(_val); _need_gc = 1; \
     _scan=_singlev; _limit=_scan+1;}\
   else {\
     _singlev[0]=_val; _scan=_singlev; _limit=_scan+1;} \
  while ((_scan<_limit) ? (elt=*(_scan++)) : \
         ((_need_gc) ? (fd_decref(_val),0) : (0)))

#define FD_STOP_DO_CHOICES \
   if (_need_gc) fd_decref(_val)

FD_EXPORT lispval fd_union(lispval *v,unsigned int n);
FD_EXPORT lispval fd_intersection(lispval *v,unsigned int n);
FD_EXPORT lispval fd_difference(lispval whole,lispval part);
FD_EXPORT int fd_choice_containsp(lispval key,lispval x);
FD_EXPORT int fd_overlapp(lispval,lispval);
FD_EXPORT int fd_containsp(lispval,lispval);

FD_EXPORT lispval fd_intersect_choices(struct FD_CHOICE **,int);

FD_EXPORT lispval *fd_natsort_choice(fd_choice ch,lispval *,ssize_t);


#endif /* FRAMERD_CHOICES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
