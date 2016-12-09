/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
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

   ACCUMULATING CHOICES (ACHOICEs)

   Accumulating choices are designed to support the fast addition of new
   elements to a choice and fast conversion to simple choices.  The fast
   conversion is supported by (a) caching conversion results on the
   ACHOICE and (b) keeping various state variables to determine which
   algorithm to use for a conversion.

   Because one common usage pattern is the conversion and dereferencing
   of an ACHOICE, special consideration is taken for optimizations
   possible during *destructive conversion*.  For example, this is
   routine in the interpreter, where a given non-deterministic function
   call accumulates results in an ACHOICE but then converts the achoice
   to return the values.  Destructive conversion can be faster than
   normal conversion for a variety of reasons, but especially because it
   is possible to avoid redundant incref/decref activity when copying
   values from the achoice into the choice proper.

   Any object can be added to an ACHOICE, but adding an ACHOICE to an
   ACHOICE forces the achoice to be converted first.  In particular,
   CHOICEs can be added to ACHOICEs and are added all at once.  During
   accumulation, the ACHOICE keeps track of the following state
   variables:
    * size: the maximum number of basic (non-choice) elements in the
       ACHOICE.  This bounds the size of any converted CHOICE but is
       not exact because we do not check for overlapping elements among
       component choices.
    * atomicity: whether any of the basic (non-choice) elements of the ACHOICE
       are CONSes
    * nestings: how many of the elements of the ACHOICE are choices
    * muddling: whether the elements of the ACHOICE are disordered, i.e.
       whether they obey the ordering criteria for simple choices.

   These properties are used to select the optimal algorithm for
   converting an ACHOICE into a simple choice.  Adding a value to an
   ACHOICE automatically updates these properties.  In addition, the
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

#ifndef FD_MERGESORT_THRESHOLD
#define FD_MERGESORT_THRESHOLD 100000
#endif

FD_EXPORT int fd_mergesort_threshold;

/* Choices */

typedef struct FD_CHOICE {
  FD_CONS_HEADER;
  unsigned int size;
  fdtype elt0;} FD_CHOICE;
typedef struct FD_CHOICE *fd_choice;

#define FD_CHOICE_SIZE_MASK 0x7FFFFFFF
#define FD_CHOICEP(x) (FD_PTR_TYPEP(x,fd_choice_type))
#define FD_XCHOICE(x) (FD_GET_CONS(x,fd_choice_type,struct FD_CHOICE *))
#define FD_CHOICE_BITS(x) \
  ((FD_STRIP_CONS(x,fd_choice_type,struct FD_CHOICE *))->size)
#define FD_XCHOICE_DATA(ch) ((const fdtype *) (&(ch->elt0)))
#define FD_CHOICE_DATA(x) \
  (FD_XCHOICE_DATA(FD_STRIP_CONS(x,fd_choice_type,struct FD_CHOICE *)))
#define FD_XCHOICE_SIZE(ch) ((ch->size)&(FD_CHOICE_SIZE_MASK))
#define FD_XCHOICE_FLAGS(ch) ((ch->size)&(~(FD_CHOICE_SIZE_MASK)))
#define FD_CHOICE_FLAGS(ch) (FD_XCHOICE_FLAGS(FD_XCHOICE(ch)))

#define FD_ATOMIC_CHOICE_MASK 0x80000000
#define FD_ATOMIC_CHOICEP(x) ((FD_CHOICE_BITS(x))&(FD_ATOMIC_CHOICE_MASK))
#define FD_XCHOICE_ATOMICP(x) ((x->size)&(FD_ATOMIC_CHOICE_MASK))

#define FD_INIT_XCHOICE(ch,sz,atomicp) \
  FD_INIT_CONS(ch,fd_choice_type); \
  ch->size=((sz)|((atomicp)?(FD_ATOMIC_CHOICE_MASK):(0)))

#define fd_alloc_choice(n) \
  (assert(n>0),u8_malloc(sizeof(struct FD_CHOICE)+((n-1)*sizeof(fdtype))))
#define fd_realloc_choice(ch,n)                                         \
  (assert(n>0),u8_realloc((ch),sizeof(struct FD_CHOICE)+((n-1)*sizeof(fdtype))))

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

FD_EXPORT fdtype fd_init_choice
  (struct FD_CHOICE *ch,int n,const fdtype *data,int flags);
#define fd_make_choice(n,data,flags) \
  fd_init_choice(fd_alloc_choice(n),n,data,flags)

/* Accumulating choices */

typedef struct FD_ACHOICE {
  FD_CONS_HEADER;
  unsigned int ach_size, ach_nested;
  unsigned ach_muddled:1, ach_mallocd:1, ach_atomic:1, fd_uselock:1;
  fdtype *ach_data, *ach_write, *ach_limit;
  fdtype ach_normalized; struct FD_CHOICE *ach_normchoice;
#if U8_THREADS_ENABLED
  u8_mutex fd_lock;
#endif
} FD_ACHOICE;
typedef struct FD_ACHOICE *fd_achoice;

#define FD_ACHOICEP(x) (FD_PTR_TYPEP(x,fd_achoice_type))
#define FD_XACHOICE(x) (FD_STRIP_CONS(x,fd_achoice_type,struct FD_ACHOICE *))
#define FD_ACHOICE_SIZE(x) ((FD_XACHOICE(x))->ach_size)
#define FD_ACHOICE_LENGTH(x) \
  (((FD_XACHOICE(x))->ach_write)-((FD_XACHOICE(x))->ach_data))
FD_EXPORT fdtype fd_make_achoice(fdtype x,fdtype y);
FD_EXPORT fdtype fd_init_achoice(struct FD_ACHOICE *ch,int lim,int uselock);
FD_EXPORT fdtype _fd_add_to_choice(fdtype current,fdtype add);
FD_EXPORT fdtype fd_merge_choices(struct FD_CHOICE **choices,int n_choices);
FD_EXPORT int _fd_choice_size(fdtype x);
FD_EXPORT fdtype _fd_make_simple_choice(fdtype x);
FD_EXPORT fdtype _fd_simplify_choice(fdtype x);

#define FD_AMBIGP(x) ((FD_CHOICEP(x))||(FD_ACHOICEP(x)))

#define FD_CHOICE_SIZE(x) \
  ((FD_EMPTY_CHOICEP(x)) ? (0) : \
   (!(FD_CONSP(x))) ? (1) : \
   (FD_CHOICEP(x)) ? (FD_XCHOICE_SIZE(FD_XCHOICE(x))) : \
   (FD_ACHOICEP(x)) ? (FD_ACHOICE_SIZE(x)) : (1))

#if FD_INLINE_CHOICES
static MAYBE_UNUSED int fd_choice_size(fdtype x)
{
  if (FD_EMPTY_CHOICEP(x)) return 0;
  else if (!(FD_CONSP(x))) return 1;
  else if (FD_CHOICEP(x))
    return FD_XCHOICE_SIZE(FD_XCHOICE(x));
  else if (FD_ACHOICEP(x))
    return FD_ACHOICE_SIZE(x);
  else return 1;
}
static MAYBE_UNUSED fdtype fd_simplify_choice(fdtype x)
{
  if (FD_ACHOICEP(x)) return _fd_simplify_choice(x);
  else return x;
}
static MAYBE_UNUSED fdtype fd_make_simple_choice(fdtype x)
{
  if (FD_ACHOICEP(x)) return _fd_make_simple_choice(x);
  else return fd_incref(x);
}
#else
#define fd_choice_size(x) _fd_choice_size(x)
#define fd_simplify_choice(x) _fd_simplify_choice(x)
#define fd_make_simple_choice(x) _fd_make_simple_choice(x)
#endif

#define FD_SIMPLIFY_CHOICE(ref) ((ref)=fd_simplify_choice(ref))

/* Quoted choices */

typedef struct FD_QCHOICE {
  FD_CONS_HEADER;
  fdtype choice;} FD_QCHOICE;
typedef struct FD_QCHOICE *fd_qchoice;

FD_EXPORT fdtype fd_init_qchoice(struct FD_QCHOICE *ptr,fdtype choice);

#define FD_QCHOICEP(x) (FD_PTR_TYPEP(x,fd_qchoice_type))
#define FD_EMPTY_QCHOICEP(x) \
  ((FD_PTR_TYPEP(x,fd_qchoice_type)) && \
   (((FD_GET_CONS(x,fd_qchoice_type,struct FD_QCHOICE *))->choice)==FD_EMPTY_CHOICE))
#define FD_XQCHOICE(x) (FD_GET_CONS(x,fd_qchoice_type,struct FD_QCHOICE *))
#define FD_QCHOICE_SIZE(x) (FD_CHOICE_SIZE(FD_XQCHOICE(x)->choice))

/* Generic choice operations */

#if FD_INLINE_CHOICES
static void _achoice_add(struct FD_ACHOICE *ch,fdtype v)
{
  int old_size, new_size, write_off, comparison;
  fdtype nv;
  if (FD_ACHOICEP(v)) {
    nv=fd_simplify_choice(v);}
  else nv=v;
  if (FD_EMPTY_CHOICEP(nv)) return;
  else if (ch->ach_write>ch->ach_data)
    if (FD_EQ(nv,*(ch->ach_write-1))) comparison=0;
    else comparison=cons_compare(*(ch->ach_write-1),nv);
  else comparison=1;
  if (comparison==0) {fd_decref(nv); return;}
  if (ch->fd_uselock) fd_lock_struct(ch);
  if (ch->ach_write >= ch->ach_limit) {
    struct FD_CHOICE *ach_normchoice;
    old_size=ch->ach_limit-ch->ach_data; write_off=ch->ach_write-ch->ach_data;
    if (old_size<0x10000) new_size=old_size*2;
    else new_size=old_size+0x20000;
    ach_normchoice=u8_realloc(ch->ach_normchoice,
                   sizeof(struct FD_CHOICE)+
                   (sizeof(fdtype)*(new_size-1)));
    ch->ach_normchoice=ach_normchoice;
    ch->ach_data=((fdtype *)FD_XCHOICE_DATA(ach_normchoice));
    ch->ach_write=ch->ach_data+write_off;
    ch->ach_limit=ch->ach_data+new_size;}
  *(ch->ach_write++)=nv;
  fd_decref(ch->ach_normalized); ch->ach_normalized=FD_VOID;
  if (comparison>0) ch->ach_muddled=1;
  if (FD_CHOICEP(nv)) {
    ch->ach_nested++; ch->ach_muddled=1;
    if (ch->ach_atomic)
      if (!(FD_ATOMIC_CHOICEP(nv))) ch->ach_atomic=0;
    ch->ach_size=ch->ach_size+FD_CHOICE_SIZE(nv);}
  else if ((ch->ach_atomic) && (FD_CONSP(nv))) {
    ch->ach_size++; ch->ach_atomic=0;}
  else ch->ach_size++;
  if (ch->fd_uselock) fd_unlock_struct(ch);
}
static fdtype _add_to_choice(fdtype current,fdtype new)
{
  FD_PTR_CHECK1(new,"_add_to_choice");
  if (FD_EMPTY_CHOICEP(new)) return current;
  else if (FD_EMPTY_CHOICEP(current))
    if (FD_ACHOICEP(new))
      if ((FD_CONS_REFCOUNT(((struct FD_CONS *)new)))>1)
        return fd_simplify_choice(new);
      else return new;
    else return new;
  else if (current==new) {
    fd_decref(new); return current;}
  else if (FD_ACHOICEP(current)) {
    _achoice_add((struct FD_ACHOICE *)current,new);
    return current;}
  else if (FDTYPE_EQUAL(current,new)) {
    fd_decref(new); return current;}
  else return fd_make_achoice(current,new);
}
#define FD_ADD_TO_CHOICE(x,v) x=_add_to_choice(x,v)
/* This does a simple binary search of a sorted choice vector made up,
   solely of atoms.  */
static MAYBE_UNUSED int atomic_choice_containsp(fdtype x,fdtype ch)
{
  if (FD_ATOMICP(ch)) return (x==ch);
  else {
    struct FD_CHOICE *choice=FD_GET_CONS(ch,fd_choice_type,fd_choice);
    int ach_size=FD_XCHOICE_SIZE(choice);
    const fdtype *bottom=FD_XCHOICE_DATA(choice), *top=bottom+(ach_size-1);
    while (top>=bottom) {
      const fdtype *middle=bottom+(top-bottom)/2;
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

#define FD_DO_CHOICES(elt,valexpr) \
  fdtype elt, _val=valexpr, _singlev[1]; \
  const fdtype *_scan, *_limit;          \
  int _need_gc=0; \
  FD_PTR_CHECK1(_val,"FD_DO_CHOICES");              \
  if (FD_ACHOICEP(_val)) {\
    _need_gc=1; _val=fd_make_simple_choice(_val);} \
   if (FD_CHOICEP(_val)) {\
    _scan=FD_CHOICE_DATA(_val); _limit=_scan+FD_CHOICE_SIZE(_val);} \
   else if (FD_EMPTY_CHOICEP(_val)) { \
     _scan=_singlev+1; _limit=_scan;}  \
   else {\
     _singlev[0]=_val; _scan=_singlev; _limit=_scan+1;} \
  while ((_scan<_limit) ? (elt=*(_scan++)) : ((_need_gc) ? (fd_decref(_val),0) : (0)))

#define FD_STOP_DO_CHOICES \
   if (_need_gc) fd_decref(_val)

FD_EXPORT fdtype fd_union(fdtype *v,int n);
FD_EXPORT fdtype fd_intersection(fdtype *v,int n);
FD_EXPORT fdtype fd_difference(fdtype whole,fdtype part);
FD_EXPORT int fd_choice_containsp(fdtype key,fdtype x);
FD_EXPORT int fd_overlapp(fdtype,fdtype);
FD_EXPORT int fd_containsp(fdtype,fdtype);

FD_EXPORT fdtype fd_intersect_choices(struct FD_CHOICE **,int);

#endif /* FRAMERD_CHOICES_H */
