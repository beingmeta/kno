/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_APPLY_H
#define FRAMERD_APPLY_H 1
#ifndef FRAMERD_APPLY_H_INFO
#define FRAMERD_APPLY_H_INFO "include/framerd/apply.h"
#endif

#include "stacks.h"

FD_EXPORT fd_exception fd_NotAFunction, fd_TooManyArgs, fd_TooFewArgs;

FD_EXPORT int fd_wrap_apply;

#ifndef FD_WRAP_APPLY_DEFAULT
#define FD_WRAP_APPLY_DEFAULT 0
#endif

#ifndef FD_INLINE_STACKS
#define FD_INLINE_STACKS 0
#endif

#ifndef FD_INLINE_APPLY
#define FD_INLINE_APPLY 0
#endif

typedef struct FD_FUNCTION FD_FUNCTION;
typedef struct FD_FUNCTION *fd_function;

#ifndef FD_MAX_APPLYFCNS
#define FD_MAX_APPLYFCNS 16
#endif

/* Various callables */

typedef fdtype (*fd_cprim0)();
typedef fdtype (*fd_cprim1)(fdtype);
typedef fdtype (*fd_cprim2)(fdtype,fdtype);
typedef fdtype (*fd_cprim3)(fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim4)(fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim5)(fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim6)(fdtype,fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim7)(fdtype,fdtype,fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim8)(fdtype,fdtype,fdtype,
			    fdtype,fdtype,fdtype,
			    fdtype,fdtype);
typedef fdtype (*fd_cprim9)(fdtype,fdtype,fdtype,
			    fdtype,fdtype,fdtype,
			    fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim10)(fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype);
typedef fdtype (*fd_cprim11)(fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype);
typedef fdtype (*fd_cprim12)(fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprim13)(fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype);
typedef fdtype (*fd_cprim14)(fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype);
typedef fdtype (*fd_cprim15)(fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype);
typedef fdtype (*fd_cprimn)(int n,fdtype *);

typedef fdtype (*fd_xprim0)(fd_function);
typedef fdtype (*fd_xprim1)(fd_function,fdtype);
typedef fdtype (*fd_xprim2)(fd_function,fdtype,fdtype);
typedef fdtype (*fd_xprim3)(fd_function,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim4)(fd_function,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim5)(fd_function,
                            fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim6)(fd_function,fdtype,fdtype,
                            fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim7)(fd_function,fdtype,fdtype,
                            fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim8)(fd_function,fdtype,fdtype,
                            fdtype,fdtype,fdtype,fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim9)(fd_function,
			    fdtype,fdtype,fdtype,
			    fdtype,fdtype,fdtype,
			    fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim10)(fd_function,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype);
typedef fdtype (*fd_xprim11)(fd_function,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype);
typedef fdtype (*fd_xprim12)(fd_function,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprim13)(fd_function,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype);
typedef fdtype (*fd_xprim14)(fd_function,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype);
typedef fdtype (*fd_xprim15)(fd_function,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype,
			     fdtype,fdtype,fdtype);
typedef fdtype (*fd_xprimn)(fd_function,int n,fdtype *);

#define FD_FUNCTION_FIELDS \
  FD_CONS_HEADER;							\
  u8_string fcn_name, fcn_filename;					\
  u8_string fcn_documentation;						\
  unsigned int fcn_ndcall:1, fcn_xcall:1, fcn_wrap_calls:1;		\
  fdtype fcnid;								\
  short fcn_arity, fcn_min_arity;					\
  fdtype fcn_attribs;							\
  int *fcn_typeinfo;							\
  fdtype *fcn_defaults;							\
  union {                                                               \
    fd_cprim0 call0; fd_cprim1 call1; fd_cprim2 call2;                  \
    fd_cprim3 call3; fd_cprim4 call4; fd_cprim5 call5;                  \
    fd_cprim6 call6; fd_cprim7 call7; fd_cprim8 call8;                  \
    fd_cprim9 call9; fd_cprim10 call10; fd_cprim11 call11;		\
    fd_cprim12 call12; fd_cprim13 call13; fd_cprim14 call14;		\
    fd_cprim15 call15;							\
    fd_cprimn calln;							\
    fd_xprim0 xcall0; fd_xprim1 xcall1; fd_xprim2 xcall2;               \
    fd_xprim3 xcall3; fd_xprim4 xcall4; fd_xprim5 xcall5;               \
    fd_xprim6 xcall6; fd_xprim7 xcall7; fd_xprim8 xcall8;               \
    fd_xprim9 xcall9; fd_xprim10 xcall10; fd_xprim11 xcall11;		\
    fd_xprim12 xcall12; fd_xprim13 xcall13; fd_xprim14 xcall14;		\
    fd_xprim15 xcall15;							\
    fd_xprimn xcalln;							\
    void *fnptr;}                                                       \
  fcn_handler

struct FD_FUNCTION {
  FD_FUNCTION_FIELDS;
};

/* This maps types to whether they have function (FD_FUNCTION_FIELDS) header. */
FD_EXPORT short fd_functionp[];

FD_EXPORT fdtype fd_new_cprimn
(u8_string name,u8_string filename,u8_string doc,
 fd_cprimn fn,int min_arity,int ndcall,int xcall);
FD_EXPORT fdtype fd_new_cprim0
(u8_string name,u8_string filename,u8_string doc,fd_cprim0 fn,int xcall);
FD_EXPORT fdtype fd_new_cprim1
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim1 fn,int min_arity,int ndcall,int xcall,
 int type0,fdtype dflt0);
FD_EXPORT fdtype fd_new_cprim2
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim2 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1);
FD_EXPORT fdtype fd_new_cprim3
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim3 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2);
FD_EXPORT fdtype fd_new_cprim4
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim4 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3);
FD_EXPORT fdtype fd_new_cprim5
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim5 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4);
FD_EXPORT fdtype fd_new_cprim6
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim6 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5);
FD_EXPORT fdtype fd_new_cprim7
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim7 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5,int type6,fdtype dflt6);
FD_EXPORT fdtype fd_new_cprim8
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim8 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5,int type6,fdtype dflt6,
 int type7,fdtype dflt7);
FD_EXPORT fdtype fd_new_cprim9
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim9 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5,int type6,fdtype dflt6,
 int type7,fdtype dflt7,int type8,fdtype dflt8);
FD_EXPORT fdtype fd_new_cprim10
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim10 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5,int type6,fdtype dflt6,
 int type7,fdtype dflt7,int type8,fdtype dflt8,
 int type9,fdtype dflt9);
FD_EXPORT fdtype fd_new_cprim11
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim11 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5,int type6,fdtype dflt6,
 int type7,fdtype dflt7,int type8,fdtype dflt8,
 int type9,fdtype dflt9,int type10,fdtype dflt10);
FD_EXPORT fdtype fd_new_cprim12
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim12 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5,int type6,fdtype dflt6,
 int type7,fdtype dflt7,int type8,fdtype dflt8,
 int type9,fdtype dflt9,int type10,fdtype dflt10,
 int type11,fdtype dflt11);
FD_EXPORT fdtype fd_new_cprim13
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim13 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5,int type6,fdtype dflt6,
 int type7,fdtype dflt7,int type8,fdtype dflt8,
 int type9,fdtype dflt9,int type10,fdtype dflt10,
 int type11,fdtype dflt11,int type12,fdtype dflt12);
FD_EXPORT fdtype fd_new_cprim14
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim14 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5,int type6,fdtype dflt6,
 int type7,fdtype dflt7,int type8,fdtype dflt8,
 int type9,fdtype dflt9,int type10,fdtype dflt10,
 int type11,fdtype dflt11,int type12,fdtype dflt12,
 int type13,fdtype dflt13);
FD_EXPORT fdtype fd_new_cprim15
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim15 fn,int min_arity,int ndcall,int call,
 int type0,fdtype dflt0,
 int type1,fdtype dflt1,int type2,fdtype dflt2,
 int type3,fdtype dflt3,int type4,fdtype dflt4,
 int type5,fdtype dflt5,int type6,fdtype dflt6,
 int type7,fdtype dflt7,int type8,fdtype dflt8,
 int type9,fdtype dflt9,int type10,fdtype dflt10,
 int type11,fdtype dflt11,int type12,fdtype dflt12,
 int type13,fdtype dflt13,int type14,fdtype dflt14);


#define fd_make_cprimn(name,fn,min_arity)		\
  fd_new_cprimn(name,_FILEINFO,NULL,fn,min_arity,0,0)


#define fd_make_cprim0(name,fn) \
  fd_new_cprim0(name,_FILEINFO,NULL,fn,0)
#define fd_make_cprim1(name,fn,min) \
  fd_new_cprim1(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		-1,FD_VOID)
#define fd_make_cprim2(name,fn,min) \
  fd_new_cprim2(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim3(name,fn,min) \
  fd_new_cprim3(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim4(name,fn,min) \
  fd_new_cprim4(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim5(name,fn,min) \
  fd_new_cprim5(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim6(name,fn,min) \
  fd_new_cprim6(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		-1,FD_VOID)
#define fd_make_cprim7(name,fn,min) \
  fd_new_cprim7(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		-1,FD_VOID-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim8(name,fn,min) \
  fd_new_cprim8(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		-1,FD_VOID-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim9(name,fn,min) \
  fd_new_cprim9(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		-1,FD_VOID-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,   \
		-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim10(name,fn,min) \
  fd_new_cprim10(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim11(name,fn,min) \
  fd_new_cprim11(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID)
#define fd_make_cprim12(name,fn,min) \
  fd_new_cprim12(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim13(name,fn,min) \
  fd_new_cprim13(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1)
#define fd_make_cprim14(name,fn,min) \
  fd_new_cprim14(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)
#define fd_make_cprim15(name,fn,min) \
  fd_new_cprim15(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0, \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,  \
		 -1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID)

#define fd_make_cprim1x(name,fn,min,...) \
  fd_new_cprim1(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim2x(name,fn,min,...) \
  fd_new_cprim2(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim3x(name,fn,min,...) \
  fd_new_cprim3(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim4x(name,fn,min,...) \
  fd_new_cprim4(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim5x(name,fn,min,...) \
  fd_new_cprim5(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim6x(name,fn,min,...) \
  fd_new_cprim6(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim7x(name,fn,min,...) \
  fd_new_cprim7(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim8x(name,fn,min,...) \
  fd_new_cprim8(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim9x(name,fn,min,...) \
  fd_new_cprim9(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim10x(name,fn,min,...) \
  fd_new_cprim10(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim11x(name,fn,min,...) \
  fd_new_cprim11(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim12x(name,fn,min,...) \
  fd_new_cprim12(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim13x(name,fn,min,...) \
  fd_new_cprim13(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim14x(name,fn,min,...) \
  fd_new_cprim14(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)
#define fd_make_cprim15x(name,fn,min,...) \
  fd_new_cprim15(name,_FILEINFO,NULL,fn,((min)&0xFFFF),((min)&0x10000),0,__VA_ARGS__)

#define fd_idefn0(module,name,fn,doc)				\
  fd_idefn(module,fd_new_cprim0(name,_FILEINFO,doc,fn,0))
#define fd_idefn1(module,name,fn,min,doc,...)			\
  fd_idefn(module,fd_new_cprim1(name,_FILEINFO,doc,fn,((min)&0xFFFF),\
				((min)&0x10000),0,\
				__VA_ARGS__))
#define fd_idefn2(module,name,fn,min,doc,...)				\
  fd_idefn(module,fd_new_cprim2(name,_FILEINFO,doc,fn,\
				((min)&0xFFFF),((min)&0x10000),0,\
				__VA_ARGS__))
#define fd_idefn3(module,name,fn,min,doc,...)				\
  fd_idefn(module,fd_new_cprim3(name,_FILEINFO,doc,fn,\
				((min)&0xFFFF),((min)&0x10000),0,\
				__VA_ARGS__))
#define fd_idefn4(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim4(name,_FILEINFO,doc,fn,\
				((min)&0xFFFF),((min)&0x10000),0,\
				__VA_ARGS__))
#define fd_idefn5(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim5(name,_FILEINFO,doc,fn,\
				((min)&0xFFFF),((min)&0x10000),0,\
				__VA_ARGS__))
#define fd_idefn6(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim6(name,_FILEINFO,doc,fn,\
				((min)&0xFFFF),((min)&0x10000),0,\
				__VA_ARGS__))
#define fd_idefn7(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim7(name,_FILEINFO,doc,fn,\
				((min)&0xFFFF),((min)&0x10000),0,\
				__VA_ARGS__))
#define fd_idefn8(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim8(name,_FILEINFO,doc,fn,\
				((min)&0xFFFF),((min)&0x10000),0,\
				__VA_ARGS__))
#define fd_idefn9(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim9(name,_FILEINFO,doc,fn,\
				((min)&0xFFFF),((min)&0x10000),0,\
				__VA_ARGS__))
#define fd_idefn10(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim10(name,_FILEINFO,doc,fn,\
				 ((min)&0xFFFF),((min)&0x10000),0,	\
				 __VA_ARGS__))
#define fd_idefn11(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim11(name,_FILEINFO,doc,fn,			\
				 ((min)&0xFFFF),((min)&0x10000),0,	\
				 __VA_ARGS__))
#define fd_idefn12(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim12(name,_FILEINFO,doc,fn,\
				 ((min)&0xFFFF),((min)&0x10000),0,	\
				 __VA_ARGS__))
#define fd_idefn13(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim13(name,_FILEINFO,doc,fn,\
				 ((min)&0xFFFF),((min)&0x10000),0,	\
				 __VA_ARGS__))
#define fd_idefn14(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim14(name,_FILEINFO,doc,fn,\
				 ((min)&0xFFFF),((min)&0x10000),0,	\
				 __VA_ARGS__))
#define fd_idefn15(module,name,fn,min,doc,...) \
  fd_idefn(module,fd_new_cprim15(name,_FILEINFO,doc,fn,			\
				 ((min)&0xFFFF),((min)&0x10000),0,	\
				 __VA_ARGS__))

#define FD_NEEDS_0_ARGS 0
#define FD_NEEDS_1_ARG  1
#define FD_NEEDS_1_ARGS 1
#define FD_NEEDS_2_ARGS 2
#define FD_NEEDS_3_ARGS 3
#define FD_NEEDS_4_ARGS 4
#define FD_NEEDS_5_ARGS 5
#define FD_NEEDS_6_ARGS 6
#define FD_NEEDS_7_ARGS 7
#define FD_NEEDS_8_ARGS 8
#define FD_NEEDS_9_ARGS 9
#define FD_NEEDS_10_ARGS 10
#define FD_NEEDS_11_ARGS 11
#define FD_NEEDS_12_ARGS 12
#define FD_NEEDS_13_ARGS 13
#define FD_NEEDS_14_ARGS 14
#define FD_NEEDS_15_ARGS 15

#define FD_NDCALL 0x10000

#define FD_FUNCTIONP(x) (fd_functionp[FD_PRIM_TYPE(x)])
#define FD_XFUNCTION(x) \
  ((FD_FUNCTIONP(x)) ? \
   ((struct FD_FUNCTION *)(FD_CONS_DATA(fd_fcnid_ref(x)))) : \
   ((struct FD_FUNCTION *)(u8_raise(fd_TypeError,"function",NULL),NULL)))
#define FD_FUNCTION_ARITY(x)  \
  ((FD_FUNCTIONP(x)) ? \
   (((struct FD_FUNCTION *)(FD_CONS_DATA(fd_fcnid_ref(x))))->fcn_arity) : \
   (0))

/* #define FD_XFUNCTION(x) (fd_consptr(struct FD_FUNCTION *,x,fd_cprim_type)) */
#define FD_PRIMITIVEP(x) (FD_TYPEP(x,fd_cprim_type))

/* Forward reference. Note that fd_sproc_type is defined in the
   pointer type enum in ptr.h. */
#define FD_SPROCP(x) (FD_TYPEP((x),fd_sproc_type))

FD_EXPORT fdtype fd_make_ndprim(fdtype prim);

/* Primitive defining macros */

#define FD_CPRIM(cname,scm_name, ...) \
  static fdtype cname(__VA_ARGS__)
#define FD_NDPRIM(cname,scm_name, ...) \
  static fdtype cname(__VA_ARGS__)

/* Definining functions in tables. */

FD_EXPORT void fd_defn(fdtype table,fdtype fcn);
FD_EXPORT void fd_idefn(fdtype table,fdtype fcn);
FD_EXPORT void fd_defalias(fdtype table,u8_string to,u8_string from);
FD_EXPORT void fd_defalias2(fdtype table,u8_string to,fdtype src,u8_string from);

/* Stack checking */

#if ((FD_THREADS_ENABLED)&&(FD_USE_TLS))
FD_EXPORT u8_tld_key fd_stack_limit_key;
#define fd_stack_limit ((ssize_t)u8_tld_get(fd_stack_limit_key))
#define fd_set_stack_limit(sz) u8_tld_set(fd_stack_limit_key,(void *)(sz))
#elif ((FD_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
FD_EXPORT __thread ssize_t fd_stack_limit;
#define fd_set_stack_limit(sz) fd_stack_limit = (sz)
#else
FD_EXPORT ssize_t stack_limit;
#define fd_set_stack_limit(sz) fd_stack_limit = (sz)
#endif

FD_EXPORT ssize_t fd_stack_setsize(ssize_t limit);
FD_EXPORT ssize_t fd_stack_resize(double factor);
FD_EXPORT int fd_stackcheck(void);
FD_EXPORT ssize_t fd_init_cstack(void);

#define FD_INIT_CSTACK() fd_init_cstack()

/* Profiling */

FD_EXPORT const int fd_calltrack_enabled;

#if FD_CALLTRACK_ENABLED
#include <stdio.h>
#ifndef FD_MAX_CALLTRACK_SENSORS
#define FD_MAX_CALLTRACK_SENSORS 64
#endif

#if ( (FD_CALLTRACK_ENABLED) && (FD_USE_TLS) )
FD_EXPORT u8_tld_key _fd_calltracking_key;
#define fd_calltracking (u8_tld_get(_fd_calltracking_key))
#elif ( (FD_CALLTRACK_ENABLED) && (HAVE__THREAD) )
FD_EXPORT __thread int fd_calltracking;
#elif (FD_CALLTRACK_ENABLED)
FD_EXPORT int fd_calltracking;
#else /* (! FD_CALLTRACK_ENABLED ) */
#define fd_calltracking (0)
#endif

typedef long (*fd_int_sensor)(void);
typedef double (*fd_dbl_sensor)(void);

typedef struct FD_CALLTRACK_SENSOR {
  u8_string name; int enabled;
  fd_int_sensor intfcn; fd_dbl_sensor dblfcn;} FD_CALLTRACK_SENSOR;
typedef FD_CALLTRACK_SENSOR *fd_calltrack_sensor;

typedef struct FD_CALLTRACK_DATUM {
  enum { ct_double, ct_int }  ct_type;
  union { double dblval; int intval;} ct_value;} FD_CALLTRACK_DATUM;
typedef struct FD_CALLTRACK_DATUM *fd_calltrack_datum;

FD_EXPORT fd_calltrack_sensor fd_get_calltrack_sensor(u8_string id,int);

FD_EXPORT fdtype fd_calltrack_sensors(void);
FD_EXPORT fdtype fd_calltrack_sense(int);

FD_EXPORT int fd_start_profiling(u8_string name);
FD_EXPORT void fd_profile_call(u8_string name);
FD_EXPORT void fd_profile_return(u8_string name);
#else
#define fd_start_calltrack(x) (-1)
#define fd_calltrack_call(name);
#define fd_calltrack_return(name);
#endif

/* Tail calls */

#define FD_TAILCALL_ND_ARGS     1
#define FD_TAILCALL_ATOMIC_ARGS 2
#define FD_TAILCALL_VOID_VALUE  4

typedef struct FD_TAILCALL {
  FD_CONS_HEADER;
  int tailcall_flags;
  int tailcall_arity;
  fdtype tailcall_head;} *fd_tailcall;

FD_EXPORT fdtype fd_tail_call(fdtype fcn,int n,fdtype *vec);
FD_EXPORT fdtype fd_step_call(fdtype c);
FD_EXPORT fdtype _fd_finish_call(fdtype);

#define FD_TAILCALLP(x) (FD_TYPEP((x),fd_tailcall_type))

FD_INLINE_FCN fdtype fd_finish_call(fdtype pt)
{
  if (FD_TAILCALLP(pt))
    return _fd_finish_call(pt);
  else return pt;
}

/* Apply functions */

typedef fdtype (*fd_applyfn)(fdtype f,int n,fdtype *);
FD_EXPORT fd_applyfn fd_applyfns[];

FD_EXPORT fdtype fd_call(struct FD_STACK *stack,fdtype fp,int n,fdtype *args);
FD_EXPORT fdtype fd_ndcall(struct FD_STACK *stack,fdtype,int n,fdtype *args);
FD_EXPORT fdtype fd_docall(struct FD_STACK *stack,fdtype,int n,fdtype *args);

FD_EXPORT fdtype _fd_stack_apply(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *args);
FD_EXPORT fdtype _fd_stack_dapply(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *args);
FD_EXPORT fdtype _fd_stack_ndapply(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *args);

#if FD_CALLTRACK_ENABLED
FD_EXPORT fdtype fd_calltrack_apply(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *argv);
#define fd_dcall(stack,fn,n,argv)		\
  ((FD_EXPECT_FALSE(fd_calltracking)) ?		\
   (fd_calltrack_apply(stack,fn,n,argv)) :	\
   (fd_docall(stack,fn,n,argv)))
#else
#define fd_dcall fd_docall
#endif

#if FD_INLINE_APPLY
U8_MAYBE_UNUSED static
fdtype fd_stack_apply(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *args)
{
  fdtype result= (stack) ?
    (fd_call(stack,fn,n_args,args)) :
    (fd_call(fd_stackptr,fn,n_args,args));
  return fd_finish_call(result);
}
static U8_MAYBE_UNUSED
fdtype fd_stack_dapply(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *args)
{
  fdtype result= (stack) ?
    (fd_dcall(stack,fn,n_args,args)) :
    (fd_dcall(fd_stackptr,fn,n_args,args));
  return fd_finish_call(result);
}
static U8_MAYBE_UNUSED
fdtype fd_stack_ndapply(struct FD_STACK *stack,fdtype fn,int n_args,fdtype *args)
{
  fdtype result= (stack) ?
    (fd_ndcall(stack,fn,n_args,args)) :
    (fd_ndcall(fd_stackptr,fn,n_args,args));
  return fd_finish_call(result);
}
#else
#define fd_stack_apply _fd_stack_apply
#define fd_stack_dapply _fd_stack_dapply
#define fd_stack_ndapply _fd_stack_ndapply
#endif

#define fd_apply(fn,n_args,argv) (fd_stack_apply(fd_stackptr,fn,n_args,argv))
#define fd_ndapply(fn,n_args,argv) (fd_stack_ndapply(fd_stackptr,fn,n_args,argv))
#define fd_dapply(fn,n_args,argv) (fd_stack_dapply(fd_stackptr,fn,n_args,argv))

#define FD_APPLICABLEP(x) \
  ((FD_TYPEP(x,fd_fcnid_type)) ?		\
   ((fd_applyfns[FD_FCNID_TYPE(x)])!=NULL) :	\
   ((fd_applyfns[FD_PRIM_TYPE(x)])!=NULL))

#define FD_DTYPE2FCN(x)		     \
  ((FD_FCNIDP(x)) ?		     \
   ((fd_function)(fd_fcnid_ref(x))) : \
   ((fd_function)x))

FD_EXPORT fdtype fd_get_backtrace(struct FD_STACK *stack,fdtype base);
FD_EXPORT void fd_html_backtrace(u8_output out,fdtype rep);

/* Unparsing */

FD_EXPORT int fd_unparse_function
  (u8_output out,fdtype x,u8_string name,u8_string before,u8_string after);

#endif /* FRAMERD_APPLY_H */
