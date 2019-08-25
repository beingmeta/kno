/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Choices in Kno

   Choices are a central datatype in Kno and used throughout
   the representation, inference, and scripting components.  As data
   structures, choices are basically *sets*: unordered collections of
   elements without duplication.  They are called choices because of
   their central role in inference and non-deterministic evaluation,
   where they describe alternate compute paths.

   As implemented in Kno, there are two basic kinds of choices:
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
   general way of adding an item (KNO_ADD_TO_CHOICE) handles the special
   case of adding the same object repeatedly by converting such identical
   additions into no-ops.  (Note that because KNO_ADD_TO_CHOICE consumes
   its argument by contract, the "no-op" may require an implicit
   dereference.)

*/

#ifndef KNO_CHOICES_H
#define KNO_CHOICES_H 1
#ifndef KNO_CHOICES_H_INFO
#define KNO_CHOICES_H_INFO "include/kno/"
#endif

#ifndef KNO_FAST_CHOICE_CONTAINSP
#define KNO_FAST_CHOICE_CONTAINSP 0
#endif

/* Choices */

typedef struct KNO_CHOICE {
  KNO_CONS_HEADER;
  unsigned int choice_size:31;
  unsigned int choice_isatomic:1;
  lispval choice_0;} KNO_CHOICE;
typedef struct KNO_CHOICE *kno_choice;

#define KNO_CHOICE_BYTES (sizeof(struct KNO_CHOICE))

#define KNO_CHOICE_SIZE_MASK 0x7FFFFFFF
#define KNO_CHOICEP(x) (KNO_TYPEP((x),kno_choice_type))
#define KNO_XCHOICE(x) (kno_consptr(struct KNO_CHOICE *,(x),kno_choice_type))
#define KNO_XCHOICE_DATA(ch) ((const lispval *) (&((ch)->choice_0)))
#define KNO_XCHOICE_ELTS(ch) ((const lispval *) (&((ch)->choice_0)))
#define KNO_CHOICE_DATA(x)   (KNO_XCHOICE_DATA(KNO_CONSPTR(kno_choice,(x))))
#define KNO_CHOICE_ELTS(x)   (KNO_XCHOICE_DATA(KNO_CONSPTR(kno_choice,(x))))
#define KNO_XCHOICE_SIZE(ch) ((ch)->choice_size)

#define KNO_ATOMIC_CHOICEP(x) ((KNO_XCHOICE(x))->choice_isatomic)
#define KNO_XCHOICE_ATOMICP(x) ((x)->choice_isatomic)

#define KNO_INIT_XCHOICE(ch,sz,atomicp) \
  KNO_INIT_CONS(ch,kno_choice_type); \
  ch->choice_size=sz; ch->choice_isatomic=atomicp

KNO_FASTOP struct KNO_CHOICE *kno_alloc_choice(int n_choices)
{
  assert(n_choices>0);
  size_t base_size = KNO_CHOICE_BYTES;
  size_t extra_elts = (n_choices-1)*LISPVAL_LEN;
  return u8_big_alloc(base_size+extra_elts);
}
KNO_FASTOP void kno_free_choice(struct KNO_CHOICE *ch)
{
  u8_big_free(ch);
}

/* Flags to pass to kno_init_choice (and kno_make_choice) */

/* Sort the results into canonical order. */
#define KNO_CHOICE_DOSORT 1
/* Compress (remove identical elements) from the choice.
   This is implied by KNO_CHOICE_DOSORT. */
#define KNO_CHOICE_COMPRESS 2
/* The choice is atomic: no elements are conses. */
#define KNO_CHOICE_ISATOMIC 4
/* The choice is not atomic: some elements are conses. */
#define KNO_CHOICE_ISCONSES 8
/* Free the data pointer when finished. */
#define KNO_CHOICE_FREEDATA 16
/* Free or realloc the pointer if neccessary. */
#define KNO_CHOICE_REALLOC  32
/* Incref data elements when copying */
#define KNO_CHOICE_INCREF  64

KNO_EXPORT lispval kno_init_choice
  (struct KNO_CHOICE *ch,int n,const lispval *data,int flags);
KNO_FASTOP lispval kno_make_choice(int n,const lispval *data,int flags)
{
  struct KNO_CHOICE *ch = kno_alloc_choice(n);
  return kno_init_choice(ch,n,data,flags);
}

/* Accumulating choices */

typedef struct KNO_PRECHOICE {
  KNO_CONS_HEADER;
  unsigned int prechoice_size, prechoice_nested;
  unsigned int prechoice_muddled:1, prechoice_mallocd:1,
    prechoice_atomic:1, prechoice_uselock:1;
  lispval *prechoice_data, *prechoice_write, *prechoice_limit;
  lispval prechoice_normalized;
  struct KNO_CHOICE *prechoice_choicedata;
#if U8_THREADS_ENABLED
  u8_mutex prechoice_lock;
#endif
} KNO_PRECHOICE;
typedef struct KNO_PRECHOICE *kno_prechoice;

#define KNO_PRECHOICE_BYTES (sizeof(struct KNO_PRECHOICE))

#define KNO_PRECHOICEP(x) (KNO_TYPEP((x),kno_prechoice_type))
#define KNO_XPRECHOICE(x) (KNO_CONSPTR(kno_prechoice,x))
#define KNO_PRECHOICE_SIZE(x) ((KNO_XPRECHOICE(x))->prechoice_size)
#define KNO_PRECHOICE_LENGTH(x) \
  (((KNO_XPRECHOICE(x))->prechoice_write)-((KNO_XPRECHOICE(x))->prechoice_data))
KNO_EXPORT lispval kno_make_prechoice(lispval x,lispval y);
KNO_EXPORT lispval kno_init_prechoice(struct KNO_PRECHOICE *ch,int lim,int uselock);
KNO_EXPORT struct KNO_CHOICE *kno_cleanup_choice(struct KNO_CHOICE *ch,unsigned int flags);
KNO_EXPORT lispval _kno_add_to_choice(lispval current,lispval add);
KNO_EXPORT void _kno_prechoice_add(struct KNO_PRECHOICE *ch,lispval v);
KNO_EXPORT int _kno_contains_atomp(lispval x,lispval ch);
KNO_EXPORT lispval kno_merge_choices(struct KNO_CHOICE **choices,int n_choices);
KNO_EXPORT int _kno_choice_size(lispval x);
KNO_EXPORT lispval _kno_make_simple_choice(lispval x);
KNO_EXPORT lispval _kno_simplify_choice(lispval x);

#define KNO_AMBIGP(x)   ((KNO_CHOICEP(x))||(KNO_PRECHOICEP(x)))
#define KNO_UNAMBIGP(x) (!(KNO_AMBIGP(x)))

KNO_EXPORT int _KNO_CHOICE_SIZE(lispval x);
#if KNO_PROFILING
#define KNO_CHOICE_SIZE(x) _KNO_CHOICE_SIZE(x)
#else
#define KNO_CHOICE_SIZE(x) \
  ((KNO_EMPTY_CHOICEP(x)) ? (0) : \
   (!(KNO_CONSP(x))) ? (1) : \
   (KNO_CHOICEP(x)) ? (KNO_XCHOICE_SIZE(KNO_XCHOICE(x))) : \
   (KNO_PRECHOICEP(x)) ? (KNO_PRECHOICE_SIZE(x)) : (1))
#endif

#if KNO_INLINE_CHOICES
KNO_FASTOP U8_MAYBE_UNUSED int kno_choice_size(lispval x)
{
  if (KNO_EMPTY_CHOICEP(x)) return 0;
  else if (!(KNO_CONSP(x))) return 1;
  else if (KNO_CHOICEP(x))
    return KNO_XCHOICE_SIZE(KNO_XCHOICE(x));
  else if (KNO_PRECHOICEP(x))
    return KNO_PRECHOICE_SIZE(x);
  else return 1;
}
KNO_FASTOP U8_MAYBE_UNUSED lispval kno_simplify_choice(lispval x)
{
  if (KNO_PRECHOICEP(x))
    return _kno_simplify_choice(x);
  else return x;
}
KNO_FASTOP U8_MAYBE_UNUSED lispval kno_make_simple_choice(lispval x)
{
  if (KNO_PRECHOICEP(x))
    return _kno_make_simple_choice(x);
  else return kno_incref(x);
}
#else
#define kno_choice_size(x) _kno_choice_size(x)
#define kno_simplify_choice(x) _kno_simplify_choice(x)
#define kno_make_simple_choice(x) _kno_make_simple_choice(x)
#endif

#define KNO_SIMPLIFY_CHOICE(ref) ((ref)=kno_simplify_choice(ref))

#ifndef KNO_CHOICEMERGE_THRESHOLD
#define KNO_CHOICEMERGE_THRESHOLD 32
#endif

KNO_EXPORT ssize_t kno_choicemerge_threshold;

/* Quoted choices */

typedef struct KNO_QCHOICE {
  KNO_CONS_HEADER;
  lispval qchoiceval;} KNO_QCHOICE;
typedef struct KNO_QCHOICE *kno_qchoice;

KNO_EXPORT lispval kno_init_qchoice(struct KNO_QCHOICE *ptr,lispval choice);
KNO_EXPORT lispval kno_make_qchoice(lispval val);

#define KNO_QCHOICEP(x) (KNO_TYPEP((x),kno_qchoice_type) )
#define KNO_EMPTY_QCHOICEP(x) \
  ((KNO_TYPEP((x),kno_qchoice_type)) &&                                 \
   (((kno_consptr(struct KNO_QCHOICE *,x,kno_qchoice_type))->qchoiceval) \
    == (KNO_EMPTY_CHOICE)))
#define KNO_XQCHOICE(x) (kno_consptr(struct KNO_QCHOICE *,x,kno_qchoice_type))
#define KNO_QCHOICE_SIZE(x) (KNO_CHOICE_SIZE(KNO_XQCHOICE(x)->qchoiceval))
#define KNO_QCHOICEVAL(x) \
  ((kno_consptr(struct KNO_QCHOICE *,x,kno_qchoice_type))->qchoiceval)

/* Generic choice operations */

#if KNO_INLINE_CHOICES
static void _prechoice_add(struct KNO_PRECHOICE *ch,lispval v)
{
  int old_size, new_size, write_off, comparison;
  lispval nv;
  if (KNO_PRECHOICEP(v)) {
    nv=kno_simplify_choice(v);}
  else nv=v;
  if (KNO_EMPTY_CHOICEP(nv)) return;
  else if (ch->prechoice_write>ch->prechoice_data)
    if (KNO_EQ(nv,*(ch->prechoice_write-1))) comparison=0;
    else comparison=cons_compare(*(ch->prechoice_write-1),nv);
  else comparison=1;
  if (comparison==0) {kno_decref(nv); return;}
  if (ch->prechoice_uselock) u8_lock_mutex(&(ch->prechoice_lock));
  if (ch->prechoice_write >= ch->prechoice_limit) {
    struct KNO_CHOICE *prechoice_choicedata;
    old_size  = ch->prechoice_limit-ch->prechoice_data;
    write_off = ch->prechoice_write-ch->prechoice_data;
    new_size=old_size*2;
    prechoice_choicedata=
      u8_big_realloc(ch->prechoice_choicedata,
                     KNO_CHOICE_BYTES+(LISPVEC_BYTELEN(new_size-1)));
    ch->prechoice_choicedata=prechoice_choicedata;
    ch->prechoice_data=((lispval *)KNO_XCHOICE_DATA(prechoice_choicedata));
    ch->prechoice_write=ch->prechoice_data+write_off;
    ch->prechoice_limit=ch->prechoice_data+new_size;}
  *(ch->prechoice_write++)=nv;
  kno_decref(ch->prechoice_normalized);
  ch->prechoice_normalized=VOID;
  if (comparison>0) ch->prechoice_muddled=1;
  if (KNO_CHOICEP(nv)) {
    ch->prechoice_nested++;
    ch->prechoice_muddled=1;
    if (ch->prechoice_atomic)
      if (!(KNO_ATOMIC_CHOICEP(nv))) ch->prechoice_atomic=0;
    ch->prechoice_size=ch->prechoice_size+KNO_CHOICE_SIZE(nv);}
  else if ((ch->prechoice_atomic) && (KNO_CONSP(nv))) {
    ch->prechoice_size++; ch->prechoice_atomic=0;}
  else ch->prechoice_size++;
  if (ch->prechoice_uselock)
    u8_unlock_mutex(&(ch->prechoice_lock));
}
#define kno_prechoice_add _prechoice_add
static U8_MAYBE_UNUSED lispval _add_to_choice(lispval current,lispval new)
{
  KNO_PTR_CHECK1(new,"_add_to_choice");
  if ( (KNO_EMPTY_CHOICEP(new)) || (KNO_VOIDP(new)) )
    return current;
  else if ((KNO_CONSP(new)) && (KNO_STATIC_CONSP(new)))
    new=kno_copy(new);
  if (KNO_EMPTY_CHOICEP(current))
    if (!(KNO_CONSP(new)))
      return new;
    else if (KNO_PRECHOICEP(new))
      if ((KNO_CONS_REFCOUNT(((struct KNO_CONS *)new)))>1)
        return kno_simplify_choice(new);
      else return new;
    else return new;
  else if (current==new) {
    kno_decref(new);
    return current;}
  else if (KNO_PRECHOICEP(current)) {
    _prechoice_add((struct KNO_PRECHOICE *)current,new);
    return current;}
  else if (LISP_EQUAL(current,new)) {
    kno_decref(new);
    return current;}
  else return kno_make_prechoice(current,new);
}
#define KNO_ADD_TO_CHOICE(x,v) x=_add_to_choice(x,v)
/* This does a simple binary search of a sorted choice vector made up,
   solely of atoms.  */
static U8_MAYBE_UNUSED int _choice_contains_atomp(lispval x,lispval ch)
{
  if (KNO_ATOMICP(ch)) return (x==ch);
  else {
    struct KNO_CHOICE *qchoiceval=kno_consptr(kno_choice,ch,kno_choice_type);
    int prechoice_size=KNO_XCHOICE_SIZE(qchoiceval);
    const lispval *bottom=KNO_XCHOICE_DATA(qchoiceval), *top=bottom+(prechoice_size-1);
    while (top>=bottom) {
      const lispval *middle=bottom+(top-bottom)/2;
      if (x == *middle) return 1;
      else if (KNO_CONSP(*middle)) top=middle-1;
      else if (x < *middle) top=middle-1;
      else bottom=middle+1;}
    return 0;}
}
#define kno_contains_atomp _choice_contains_atomp
#else
#define kno_prechoice_add _kno_prechoice_add
#define KNO_ADD_TO_CHOICE(x,v)                    \
   if (KNO_DEBUG_BADPTRP(v))                      \
     _kno_bad_pointer(v,(u8_context)"KNO_ADD_TO_CHOICE"); \
   else x=_kno_add_to_choice(x,v)
#define kno_contains_atomp _kno_contains_atomp
#endif

#if KNO_FAST_CHOICE_CONTAINSP
/* This does a simple binary search of a sorted choice vector,
   looking for a particular element. Once more, we separate out the
   atomic case because it just requires pointer comparisons.  */
static U8_MAYBE_UNUSED int fast_choice_containsp(lispval x,struct KNO_CHOICE *choice)
{
  int size = KNO_XCHOICE_SIZE(choice);
  const lispval *bottom = KNO_XCHOICE_DATA(choice), *top = bottom+(size-1);
  if (KNO_ATOMICP(x)) {
    while (top>=bottom) {
      const lispval *middle = bottom+(top-bottom)/2;
      if (x == *middle) return 1;
      else if (KNO_CONSP(*middle)) top = middle-1;
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

static U8_MAYBE_UNUSED void
kno_dochoices_helper(lispval *_valp,
                     const lispval **scan,
                     const lispval **limit,
                     lispval *singlev,
                     int *need_gcp)
{
  lispval _val = *_valp;
  if (KNO_PRECHOICEP(_val)) {
    *need_gcp=1;
    _val=kno_make_simple_choice(_val);}
  if (KNO_CHOICEP(_val)) {
    *scan=KNO_CHOICE_DATA(_val);
    *limit=KNO_CHOICE_DATA(_val)+KNO_CHOICE_SIZE(_val);}
  else if (KNO_EMPTY_CHOICEP(_val)) {
    *scan=singlev+1;
    *limit=singlev+1;}
  else {
    singlev[0]=_val;
    *scan=singlev;
    *limit=singlev+1;}
  *_valp = _val;
}

#if KNO_EXTREME_PROFILING
#define KNO_DO_CHOICES(elt,valexpr) \
  lispval elt, _val=valexpr, _singlev[1]; \
  const lispval *_scan, *_limit;          \
  int _need_gc=0; \
  KNO_PTR_CHECK1(_val,"KNO_DO_CHOICES");              \
  kno_dochoices_helper(&_val,&_scan,&_limit,_singlev,&_need_gc); \
  while ((_scan<_limit) ? (elt=*(_scan++)) : \
         ((_need_gc) ? (kno_decref(_val),0) : (0)))
#else
#define KNO_DO_CHOICES(elt,valexpr) \
  lispval elt, _val=valexpr, _singlev[1]; \
  const lispval *_scan, *_limit;          \
  int _need_gc=0; \
  KNO_PTR_CHECK1(_val,"KNO_DO_CHOICES");              \
  if (KNO_PRECHOICEP(_val)) {\
    _need_gc=1; _val=kno_make_simple_choice(_val);} \
   if (KNO_CHOICEP(_val)) {\
    _scan=KNO_CHOICE_DATA(_val); _limit=_scan+KNO_CHOICE_SIZE(_val);} \
   else if (KNO_EMPTY_CHOICEP(_val)) { \
     _scan=_singlev+1; _limit=_scan;}  \
   else if (KNO_QCHOICEP(_val)) { \
     _singlev[0] = KNO_XQCHOICE(_val)->qchoiceval; \
     _val = _singlev[0]; kno_incref(_val); _need_gc = 1; \
     _scan=_singlev; _limit=_scan+1;}\
   else {\
     _singlev[0]=_val; _scan=_singlev; _limit=_scan+1;} \
  while ((_scan<_limit) ? (elt=*(_scan++)) : \
         ((_need_gc) ? (kno_decref(_val),0) : (0)))
#endif

#define KNO_STOP_DO_CHOICES \
   if (_need_gc) kno_decref(_val)

KNO_EXPORT lispval kno_union(const lispval *v,unsigned int n);
KNO_EXPORT lispval kno_intersection(const lispval *v,unsigned int n);
KNO_EXPORT lispval kno_difference(lispval whole,lispval part);
KNO_EXPORT int kno_choice_containsp(lispval key,lispval x);
KNO_EXPORT int kno_overlapp(lispval,lispval);
KNO_EXPORT int kno_containsp(lispval,lispval);

KNO_EXPORT lispval kno_intersect_choices(struct KNO_CHOICE **,int);

KNO_EXPORT lispval *kno_natsort_choice(kno_choice ch,lispval *,ssize_t);


#endif /* KNO_CHOICES_H */

