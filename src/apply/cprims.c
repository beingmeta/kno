/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/lexenv.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>


FD_EXPORT int unparse_primitive(u8_output out,lispval x)
{
  struct FD_FUNCTION *fcn = (fd_function)x;
  u8_string name = fcn->fcn_name;
  u8_string filename = fcn->fcn_filename, space;
  u8_byte arity[64]=""; u8_byte codes[64]="";
  u8_byte tmpbuf[32], numbuf[32];
  if ((filename)&&(filename[0]=='\0'))
    filename = NULL;
  if ( (filename) && (space=strchr(filename,' '))) {
    int len = space-filename;
    if (len>30) len=30;
    strncpy(tmpbuf,filename,len);
    tmpbuf[len]='\0';
    filename=tmpbuf;}
  if (filename==NULL) filename="nofile";
  if (fcn->fcn_ndcall) strcat(codes,"∀");
  if ((fcn->fcn_arity<0)&&(fcn->fcn_min_arity<0))
    strcat(arity,"[…]");
  else if (fcn->fcn_arity == fcn->fcn_min_arity) {
    strcat(arity,"[");
    strcat(arity,u8_itoa10(fcn->fcn_arity,numbuf));
    strcat(arity,"]");}
  else if (fcn->fcn_arity<0) {
    strcat(arity,"[");
    strcat(arity,u8_itoa10(fcn->fcn_min_arity,numbuf));
    strcat(arity,"…]");}
  else {
    strcat(arity,"[");
    strcat(arity,u8_itoa10(fcn->fcn_min_arity,numbuf));
    strcat(arity,"-");
    strcat(arity,u8_itoa10(fcn->fcn_arity,numbuf));
    strcat(arity,"]");}
  if (name)
    u8_printf(out,"#<Φ%s%s%s%s%s%s>",
              codes,name,arity,
              U8OPTSTR(" '",filename,"'"));
  else u8_printf(out,"#<Φ%s%s #!0x%s%s%s%s>",
                 codes,arity,
                 u8_uitoa16((unsigned long long) fcn,numbuf),
                 U8OPTSTR("'",filename,"'"));
  return 1;
}
static void recycle_primitive(struct FD_RAW_CONS *c)
{
  struct FD_FUNCTION *fn = (struct FD_FUNCTION *)c;
  if (fn->fcn_typeinfo) u8_free(fn->fcn_typeinfo);
  if (fn->fcn_defaults) u8_free(fn->fcn_defaults);
  if (fn->fcn_attribs) fd_decref(fn->fcn_attribs);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static int dtype_cprim(struct FD_OUTBUF *out,lispval x)
{
  int n_elts=0;
  struct FD_FUNCTION *fcn = (struct FD_FUNCTION *)x;
  unsigned char buf[200], *tagname="%CPRIM";
  struct FD_OUTBUF tmp;
  FD_INIT_OUTBUF(&tmp,buf,200,0);
  fd_write_byte(&tmp,dt_compound);
  fd_write_byte(&tmp,dt_symbol);
  fd_write_4bytes(&tmp,6);
  fd_write_bytes(&tmp,tagname,6);
  if (fcn->fcn_name) n_elts++;
  if (fcn->fcn_filename) n_elts++;
  fd_write_byte(&tmp,dt_vector);
  fd_write_4bytes(&tmp,n_elts);
  if (fcn->fcn_name) {
    size_t len=strlen(fcn->fcn_name);
    fd_write_byte(&tmp,dt_symbol);
    fd_write_4bytes(&tmp,len);
    fd_write_bytes(&tmp,fcn->fcn_name,len);}
  if (fcn->fcn_filename) {
    size_t len=strlen(fcn->fcn_filename);
    fd_write_byte(&tmp,dt_string);
    fd_write_4bytes(&tmp,len);
    fd_write_bytes(&tmp,fcn->fcn_filename,len);}
  size_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

/* Declaring functions */

static struct FD_FUNCTION *new_cprim(u8_string name,
                                     u8_string filename,
                                     u8_string doc,
				     int arity,int min_arity,
				     int non_deterministic,
				     int extended_call)
{
  struct FD_FUNCTION *f = u8_alloc(struct FD_FUNCTION);
  FD_INIT_FRESH_CONS(f,fd_cprim_type);
  f->fcn_name = name;
  f->fcn_filename = filename;
  f->fcn_documentation = doc;
  if (non_deterministic)
    f->fcn_ndcall = 1;
  else f->fcn_ndcall = 0;
  if (extended_call)
    f->fcn_xcall = 1;
  else f->fcn_xcall = 0;
  f->fcn_arity = arity;
  f->fcn_min_arity = min_arity;
  f->fcn_typeinfo = NULL;
  f->fcn_defaults = NULL;
  f->fcnid = VOID;
  if ( (arity>=0) && (min_arity>arity)) {
    u8_log(LOGCRIT,_("Bad primitive definition"),
           "Fixing primitive %s%s%s%s with min_arity=%d > arity=%d",
           name,U8OPTSTR(" (",filename,") "),arity,min_arity);
    f->fcn_min_arity = arity;}
  return f;
}

/* Providing type info and defaults */

FD_EXPORT lispval fd_new_cprimn
(u8_string name,u8_string filename,u8_string doc,
 fd_cprimn fn,int min_arity,int ndcall,int xcall)
{
  struct FD_FUNCTION *f =
    new_cprim(name,filename,doc,-1,min_arity,ndcall,xcall);
  f->fcn_min_arity = min_arity;
  f->fcn_arity = -1;
  f->fcn_handler.calln = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim0
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim0 fn,int xcall)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,0,0,0,xcall);
  f->fcn_typeinfo = NULL;
  f->fcn_defaults = NULL;
  f->fcn_handler.call0 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim1
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim1 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,1,min_arity,ndcall,xcall);
  int *types=u8_alloc_n(1,int);
  lispval *defaults=u8_alloc_n(1,lispval);
  f->fcn_typeinfo = types;
  f->fcn_defaults = defaults;
  types[0]=type0; defaults[0]=dflt0;
  f->fcn_handler.call1 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim2
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim2 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,2,min_arity,ndcall,xcall);
  int *types=u8_alloc_n(2,int);
  lispval *defaults=u8_alloc_n(2,lispval);
  f->fcn_typeinfo = types;
  f->fcn_defaults = defaults;
  types[0]=type0; defaults[0]=dflt0;
  types[1]=type1; defaults[1]=dflt1;
  f->fcn_handler.call2 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim3
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim3 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,3,min_arity,ndcall,xcall);
  int *types=u8_alloc_n(3,int);
  lispval *defaults=u8_alloc_n(3,lispval);
  f->fcn_typeinfo = types;
  f->fcn_defaults = defaults;
  types[0]=type0; defaults[0]=dflt0;
  types[1]=type1; defaults[1]=dflt1;
  types[2]=type2; defaults[2]=dflt2;
  f->fcn_handler.call3 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim4
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim4 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,4,min_arity,ndcall,xcall);
  int *types=u8_alloc_n(4,int);
  lispval *defaults=u8_alloc_n(4,lispval);
  f->fcn_typeinfo = types;
  f->fcn_defaults = defaults;
  types[0]=type0; defaults[0]=dflt0;
  types[1]=type1; defaults[1]=dflt1;
  types[2]=type2; defaults[2]=dflt2;
  types[3]=type3; defaults[3]=dflt3;
  f->fcn_handler.call4 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim5
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim5 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,5,min_arity,ndcall,xcall);
  int *types=u8_alloc_n(5,int);
  lispval *defaults=u8_alloc_n(5,lispval);
  f->fcn_typeinfo = types;
  f->fcn_defaults = defaults;
  types[0]=type0; defaults[0]=dflt0;
  types[1]=type1; defaults[1]=dflt1;
  types[2]=type2; defaults[2]=dflt2;
  types[3]=type3; defaults[3]=dflt3;
  types[4]=type4; defaults[4]=dflt4;
  f->fcn_handler.call5 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim6
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim6 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,6,min_arity,ndcall,xcall);
  int *types=u8_alloc_n(6,int);
  lispval *defaults=u8_alloc_n(6,lispval);
  f->fcn_typeinfo = types;
  f->fcn_defaults = defaults;
  types[0]=type0; defaults[0]=dflt0;
  types[1]=type1; defaults[1]=dflt1;
  types[2]=type2; defaults[2]=dflt2;
  types[3]=type3; defaults[3]=dflt3;
  types[4]=type4; defaults[4]=dflt4;
  types[5]=type5; defaults[5]=dflt5;
  f->fcn_handler.call6 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim7
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim7 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,7,min_arity,ndcall,xcall);
  int *types=u8_alloc_n(7,int);
  lispval *defaults=u8_alloc_n(7,lispval);
  f->fcn_typeinfo = types;
  f->fcn_defaults = defaults;
  types[0]=type0; defaults[0]=dflt0;
  types[1]=type1; defaults[1]=dflt1;
  types[2]=type2; defaults[2]=dflt2;
  types[3]=type3; defaults[3]=dflt3;
  types[4]=type4; defaults[4]=dflt4;
  types[5]=type5; defaults[5]=dflt5;
  types[6]=type6; defaults[6]=dflt6;
  f->fcn_handler.call7 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim8
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim8 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,8,min_arity,ndcall,xcall);
  int *types=u8_alloc_n(8,int);
  lispval *defaults=u8_alloc_n(8,lispval);
  types[0]=type0; defaults[0]=dflt0;
  types[1]=type1; defaults[1]=dflt1;
  types[2]=type2; defaults[2]=dflt2;
  types[3]=type3; defaults[3]=dflt3;
  types[4]=type4; defaults[4]=dflt4;
  types[5]=type5; defaults[5]=dflt5;
  types[6]=type6; defaults[6]=dflt6;
  types[7]=type7; defaults[7]=dflt7;
  f->fcn_handler.call8 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_new_cprim9
(u8_string name,u8_string filename,u8_string doc,
 fd_cprim9 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7,int type8,lispval dflt8)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)
    new_cprim(name,filename,doc,9,min_arity,ndcall,xcall);
  int *types=u8_alloc_n(9,int);
  lispval *defaults=u8_alloc_n(9,lispval);
  f->fcn_typeinfo = types;
  f->fcn_defaults = defaults;
  types[0]=type0; defaults[0]=dflt0;
  types[1]=type1; defaults[1]=dflt1;
  types[2]=type2; defaults[2]=dflt2;
  types[3]=type3; defaults[3]=dflt3;
  types[4]=type4; defaults[4]=dflt4;
  types[5]=type5; defaults[5]=dflt5;
  types[6]=type6; defaults[6]=dflt6;
  types[7]=type7; defaults[7]=dflt7;
  types[8]=type8; defaults[8]=dflt8;
  f->fcn_handler.call9 = fn;
  return LISP_CONS(f);
}

#if 0
FD_EXPORT lispval fd_make_cprimn(u8_string name,fd_cprimn fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,-1,min_arity,0,0);
  f->fcn_min_arity = min_arity;
  f->fcn_arity = -1;
  f->fcn_handler.calln = fn;
  return LISP_CONS(f);
}
FD_EXPORT lispval fd_make_cprim0(u8_string name,fd_cprim0 fn)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,0,0,0,0);
  f->fcn_handler.call0 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_make_cprim1(u8_string name,fd_cprim1 fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,1,min_arity,0,0);
  f->fcn_handler.call1 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_make_cprim2(u8_string name,fd_cprim2 fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,2,min_arity,0,0);
  f->fcn_handler.call2 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_make_cprim3(u8_string name,fd_cprim3 fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,3,min_arity,0,0);
  f->fcn_handler.call3 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_make_cprim4(u8_string name,fd_cprim4 fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,4,min_arity,0,0);
  f->fcn_handler.call4 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_make_cprim5(u8_string name,fd_cprim5 fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,5,min_arity,0,0);
  f->fcn_handler.call5 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_make_cprim6(u8_string name,fd_cprim6 fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,6,min_arity,0,0);
  f->fcn_handler.call6 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_make_cprim7(u8_string name,fd_cprim7 fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,7,min_arity,0,0);
  f->fcn_handler.call7 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_make_cprim8(u8_string name,fd_cprim8 fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,8,min_arity,0,0);
  f->fcn_handler.call8 = fn;
  return LISP_CONS(f);
}

FD_EXPORT lispval fd_make_cprim9(u8_string name,fd_cprim9 fn,int min_arity)
{
  struct FD_FUNCTION *f = new_cprim(name,NULL,NULL,9,min_arity,0,0);
  f->fcn_handler.call9 = fn;
  return LISP_CONS(f);
}
#endif

FD_EXPORT lispval fd_make_ndprim(lispval prim)
{
  struct FD_FUNCTION *f = FD_XFUNCTION(prim);
  f->fcn_ndcall = 1;
  return prim;
}

FD_EXPORT void fd_init_cprims_c()
{
  u8_register_source_file(_FILEINFO);

  fd_functionp[fd_cprim_type]=1;

  fd_unparsers[fd_cprim_type]=unparse_primitive;
  fd_recyclers[fd_cprim_type]=recycle_primitive;
  fd_dtype_writers[fd_cprim_type]=dtype_cprim;
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
