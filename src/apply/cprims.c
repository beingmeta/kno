/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_FCNIDS 1
#define KNO_INLINE_STACKS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/lexenv.h"
#include "kno/apply.h"
#include "kno/cprims.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

int unparse_cprim(u8_output out,lispval x)
{
  struct KNO_FUNCTION *fcn = (kno_function)x;
  u8_string filename = fcn->fcn_filename, space;
  lispval moduleid = fcn->fcn_moduleid;
  u8_string modname =
    (KNO_SYMBOLP(moduleid)) ? (KNO_SYMBOL_NAME(moduleid)) : (NULL);
  u8_byte arity[64]=""; u8_byte codes[64]="";
  u8_byte tmpbuf[32], numbuf[32], namebuf[100];
  u8_string name = fcn->fcn_name, sig = kno_fcn_sig(fcn,namebuf);
  if (sig == NULL) sig = "";
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
  if ( (name) && (modname) )
    u8_printf(out,"#<Φ%s%s%s%s %s %s%s%s>",
              codes,name,arity,sig,modname,
              U8OPTSTR(" '",filename,"'"));
  else if (name)
    u8_printf(out,"#<Φ%s%s%s%s%s%s%s>",
              codes,name,arity,sig,
              U8OPTSTR(" '",filename,"'"));
  else u8_printf(out,"#<Φ%s%s%s #!0x%s%s%s%s>",
                 codes,arity,sig,
                 u8_uitoa16(KNO_LONGVAL( fcn),numbuf),
                 U8OPTSTR("'",filename,"'"));
  return 1;
}
static void recycle_cprim(struct KNO_RAW_CONS *c)
{
  struct KNO_FUNCTION *fn = (struct KNO_FUNCTION *)c;
  if ( (fn->fcn_typeinfo) && (fn->fcn_free_typeinfo) )
    u8_free(fn->fcn_typeinfo);
  if ( (fn->fcn_defaults) && (fn->fcn_free_defaults) )
    u8_free(fn->fcn_defaults);
  if (fn->fcn_attribs) kno_decref(fn->fcn_attribs);
  if (fn->fcn_moduleid) kno_decref(fn->fcn_moduleid);
  if (KNO_MALLOCD_CONSP(c)) u8_free(c);
}

static ssize_t cprim_dtype(struct KNO_OUTBUF *out,lispval x)
{
  int n_elts=0;
  struct KNO_FUNCTION *fcn = (struct KNO_FUNCTION *)x;
  unsigned char buf[200], *tagname="%CPRIM";
  struct KNO_OUTBUF tmp = { 0 };
  KNO_INIT_OUTBUF(&tmp,buf,200,0);
  kno_write_byte(&tmp,dt_compound);
  kno_write_byte(&tmp,dt_symbol);
  kno_write_4bytes(&tmp,6);
  kno_write_bytes(&tmp,tagname,6);
  if (fcn->fcn_name) n_elts++;
  if (fcn->fcn_filename) n_elts++;
  kno_write_byte(&tmp,dt_vector);
  kno_write_4bytes(&tmp,n_elts);
  if (fcn->fcn_name) {
    size_t len=strlen(fcn->fcn_name);
    kno_write_byte(&tmp,dt_symbol);
    kno_write_4bytes(&tmp,len);
    kno_write_bytes(&tmp,fcn->fcn_name,len);}
  if (fcn->fcn_filename) {
    size_t len=strlen(fcn->fcn_filename);
    kno_write_byte(&tmp,dt_string);
    kno_write_4bytes(&tmp,len);
    kno_write_bytes(&tmp,fcn->fcn_filename,len);}
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  kno_write_bytes(out,tmp.buffer,n_bytes);
  kno_close_outbuf(&tmp);
  return n_bytes;
}

/* Creating cprims */

static struct KNO_FUNCTION *make_cprim(u8_string name,
                                       u8_string cname,
                                       u8_string filename,
                                       u8_string doc,
                                       unsigned int flags,
                                       int *typeinfo,
                                       lispval *defaults)
{
  int arity = ( (flags&0x80) ? (-1) : ( flags & (0x7f) ) );
  int min_arity = (flags&0x8000) ? ( (flags>>8) & 0x7f) : (arity);
  int non_deterministic = flags & KNO_NDCALL;
  int extended_call = flags & KNO_XCALL;
  /* We allocate the type/default info together with the function to
     reduce cache/page misses. We might need to worry about how we're
     figuring out these pointers for non-word-aligned architectures, but
     let's not worry about that for now. */
  size_t alloc_size = sizeof(struct KNO_FUNCTION) +
    ( (typeinfo) ? (sizeof(unsigned int)*arity) : (0) ) +
    ( (defaults) ? (sizeof(lispval)*arity) : (0) );
  void *block = u8_malloc(alloc_size);
  struct KNO_FUNCTION *f = (struct KNO_FUNCTION *) block;
  unsigned int *prim_typeinfo = (typeinfo) ?
    ((unsigned int *) (block+sizeof(struct KNO_FUNCTION))) :
    ((unsigned int *)NULL);
  lispval *prim_defaults = ( (defaults) && (typeinfo) ) ?
    ((lispval *) (block+sizeof(struct KNO_FUNCTION)+(sizeof(unsigned int)*arity))) :
    (defaults) ?
    ((lispval *) (block+sizeof(struct KNO_FUNCTION)) ) :
    ((lispval *)NULL);
  KNO_INIT_FRESH_CONS(f,kno_cprim_type);
  f->fcn_name = name;
  f->fcn_filename = filename;
  f->fcn_doc = doc;
  f->fcn_moduleid = KNO_VOID;
  if (non_deterministic)
    f->fcn_ndcall = 1;
  else f->fcn_ndcall = 0;
  if (extended_call)
    f->fcn_xcall = 1;
  else f->fcn_xcall = 0;
  f->fcn_arity = arity;
  f->fcn_min_arity = min_arity;
  f->fcn_typeinfo = prim_typeinfo;
  if (typeinfo) memcpy(prim_typeinfo,typeinfo,sizeof(int)*arity);
  f->fcn_defaults = prim_defaults;
  if (defaults) memcpy(prim_defaults,defaults,sizeof(lispval)*arity);
  f->fcnid = VOID;
  if ( (arity>=0) && (min_arity>arity)) {
    u8_log(LOG_CRIT,_("Bad primitive definition"),
           "Fixing primitive %s%s%s%s with min_arity=%d > arity=%d",
           name,U8OPTSTR(" (",filename,") "),arity,min_arity);
    f->fcn_min_arity = arity;}
  return f;
}

/* Declaring functions */

static struct KNO_FUNCTION *init_cprim(u8_string name,u8_string cname,
                                       int arity,
                                       u8_string filename,
                                       u8_string doc,
                                       int flags,
                                       int *typeinfo,
                                       lispval *defaults)
{
  return make_cprim(name,cname,filename,doc,flags|KNO_MAX_ARGS(arity),
                    typeinfo,defaults);
}

KNO_EXPORT void kno_defprimN(lispval module,kno_cprimn fn,
                             struct KNO_CPRIM_INFO *info)
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,
     info->arity,info->filename,
     info->docstring,info->flags,
     (unsigned int *)NULL,( lispval *)NULL);
  prim->fcn_handler.calln = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim0(lispval module,kno_cprim0 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[0],
                             lispval defaults[0])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call0 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim1(lispval module,kno_cprim1 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[1],
                             lispval defaults[1])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call1 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim2(lispval module,kno_cprim2 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[2],
                             lispval defaults[2])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call2 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim3(lispval module,kno_cprim3 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[3],
                             lispval defaults[3])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call3 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim4(lispval module,kno_cprim4 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[4],
                             lispval defaults[4])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call4 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim5(lispval module,kno_cprim5 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[5],
                             lispval defaults[5])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call5 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim6(lispval module,kno_cprim6 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[6],
                             lispval defaults[6])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call6 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim7(lispval module,kno_cprim7 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[7],
                             lispval defaults[7])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call7 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim8(lispval module,kno_cprim8 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[8],
                             lispval defaults[8])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call8 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim9(lispval module,kno_cprim9 fn,
                             struct KNO_CPRIM_INFO *info,
                             int typeinfo[9],
                             lispval defaults[9])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call9 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim10(lispval module,kno_cprim10 fn,
                              struct KNO_CPRIM_INFO *info,
                              int typeinfo[10],
                              lispval defaults[10])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call10 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim11(lispval module,kno_cprim11 fn,
                              struct KNO_CPRIM_INFO *info,
                              int typeinfo[11],
                              lispval defaults[11])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call11 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim12(lispval module,kno_cprim12 fn,
                              struct KNO_CPRIM_INFO *info,
                              int typeinfo[12],
                              lispval defaults[12])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call12 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim13(lispval module,kno_cprim13 fn,
                              struct KNO_CPRIM_INFO *info,
                              int typeinfo[13],
                              lispval defaults[13])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call13 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim14(lispval module,kno_cprim14 fn,
                              struct KNO_CPRIM_INFO *info,
                              int typeinfo[14],
                              lispval defaults[14])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call14 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

KNO_EXPORT void kno_defprim15(lispval module,kno_cprim15 fn,
                              struct KNO_CPRIM_INFO *info,
                              int typeinfo[15],
                              lispval defaults[15])
{
  struct KNO_FUNCTION *prim = init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call15 = fn;
  lispval primval = (lispval) prim;
  kno_store(module,kno_getsym(info->pname),primval);
  kno_decref(primval);
}

/* Providing type info and defaults */

KNO_EXPORT u8_string kno_fcn_sig(struct KNO_FUNCTION *fcn,u8_byte namebuf[100])
{
  u8_string name = NULL;
  if (fcn->fcn_doc) {
    u8_string sig = fcn->fcn_doc, sig_start = sig, sig_end = NULL;
    if (sig[0] == '`') {
      sig_start++;
      sig_end = strchr(sig_start,'`');}
    else if (sig[0] == '(') {
      sig_end = strchr(sig_start,')');
      if (sig_end) sig_end++;}
    else NO_ELSE;
    size_t sig_len = (sig_end) ? (sig_end-sig_start) : (strlen(sig_start));
    if (sig_len < 90) {
      strncpy(namebuf,sig_start,sig_len);
      namebuf[sig_len]='\0';
      name = namebuf;}}
  return name;
}

KNO_EXPORT lispval kno_new_cprimn
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprimn fn,int min_arity,int ndcall,int xcall)
{
  struct KNO_FUNCTION *f =
    make_cprim(name,cname,filename,doc,
               (KNO_MAX_ARGS(-1)) |
               (KNO_MIN_ARGS(min_arity)) |
               ( (ndcall) ? (KNO_NDCALL) : (0) ) |
               ( (xcall) ? (KNO_XCALL) : (0) ),
               NULL,NULL);
  f->fcn_handler.calln = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim0
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim0 fn,int xcall)
{
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(0,0,0,xcall),NULL,NULL);
  f->fcn_handler.call0 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim1
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim1 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0)
{
  int typeinfo[1] = { type0 };
  lispval defaults[1] = { dflt0 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(1,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call1 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim2
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim2 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1)
{
  int typeinfo[2] = { type0, type1 };
  lispval defaults[2] = { dflt0, dflt1 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(2,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call2 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim3
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim3 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2)
{
  int typeinfo[3] = { type0, type1, type2 };
  lispval defaults[3] = { dflt0, dflt1, dflt2 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(3,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call3 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim4
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim4 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3)
{
  int typeinfo[4] = { type0, type1, type2, type3 };
  lispval defaults[4] = { dflt0, dflt1, dflt2, dflt3 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(4,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call4 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim5
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim5 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4)
{
  int typeinfo[5] = { type0, type1, type2, type3, type4 };
  lispval defaults[5] = { dflt0, dflt1, dflt2, dflt3, dflt4 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(5,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call5 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim6
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim6 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5)
{
  int typeinfo[6] = { type0, type1, type2, type3, type4, type5 };
  lispval defaults[6] = { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(6,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call6 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim7
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim7 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6)
{
  int typeinfo[7] = { type0, type1, type2, type3, type4, type5,
                      type6 };
  lispval defaults[7] = { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5, dflt6 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(7,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call7 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim8
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim8 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7)
{
  int typeinfo[8] =
    { type0, type1, type2, type3, type4, type5, type6, type7 };
  lispval defaults[8] =
    { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5, dflt6, dflt7 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(8,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call8 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim9
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim9 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7,int type8,lispval dflt8)
{
  int typeinfo[9] =
    { type0, type1, type2, type3, type4, type5, type6, type7, type8 };
  lispval defaults[9] =
    { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5, dflt6, dflt7, dflt8 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(9,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call9 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim10
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim10 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7,int type8,lispval dflt8,
 int type9,lispval dflt9)
{
  int typeinfo[10] =
    { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9 };
  lispval defaults[10] =
    { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5, dflt6, dflt7, dflt8, dflt9 };
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(10,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call10 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim11
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim11 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7,int type8,lispval dflt8,
 int type9,lispval dflt9,int type10,lispval dflt10)
{
  int typeinfo[11] =
    { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
      type10 };
  lispval defaults[11] =
    { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5, dflt6, dflt7, dflt8, dflt9,
      dflt10};
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(11,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call11 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim12
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim12 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7,int type8,lispval dflt8,
 int type9,lispval dflt9,int type10,lispval dflt10,
 int type11,lispval dflt11)
{
  int typeinfo[12] =
    { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
      type10, type11 };
  lispval defaults[12] =
    { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5, dflt6, dflt7, dflt8, dflt9,
      dflt10, dflt11};
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(12,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call12 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim13
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim13 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7,int type8,lispval dflt8,
 int type9,lispval dflt9,int type10,lispval dflt10,
 int type11,lispval dflt11,int type12,lispval dflt12)
{
  int typeinfo[13] =
    { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
      type10, type11, type12 };
  lispval defaults[13] =
    { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5, dflt6, dflt7, dflt8, dflt9,
      dflt10, dflt11, dflt12};
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(13,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call13 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim14
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim14 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7,int type8,lispval dflt8,
 int type9,lispval dflt9,int type10,lispval dflt10,
 int type11,lispval dflt11,int type12,lispval dflt12,
 int type13,lispval dflt13)
{
  int typeinfo[14] =
    { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
      type10, type11, type12, type13 };
  lispval defaults[14] =
    { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5, dflt6, dflt7, dflt8, dflt9,
      dflt10, dflt11, dflt12, dflt13};
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(14,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call14 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_new_cprim15
(u8_string name,u8_string cname,u8_string filename,u8_string doc,
 kno_cprim15 fn,int min_arity,int ndcall,int xcall,
 int type0,lispval dflt0,
 int type1,lispval dflt1,int type2,lispval dflt2,
 int type3,lispval dflt3,int type4,lispval dflt4,
 int type5,lispval dflt5,int type6,lispval dflt6,
 int type7,lispval dflt7,int type8,lispval dflt8,
 int type9,lispval dflt9,int type10,lispval dflt10,
 int type11,lispval dflt11,int type12,lispval dflt12,
 int type13,lispval dflt13,int type14,lispval dflt14)
{
  int typeinfo[15] =
    { type0, type1, type2, type3, type4, type5, type6, type7, type8, type9,
      type10, type11, type12, type13, type14 };
  lispval defaults[15] =
    { dflt0, dflt1, dflt2, dflt3, dflt4, dflt5, dflt6, dflt7, dflt8, dflt9,
      dflt10, dflt11, dflt12, dflt13, dflt14};
  struct KNO_FUNCTION *f=(struct KNO_FUNCTION *)
    make_cprim(name,cname,filename,doc,KNO_FNFLAGS(15,min_arity,ndcall,xcall),
               typeinfo,defaults);
  f->fcn_handler.call15 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_make_ndprim(lispval prim)
{
  struct KNO_FUNCTION *f = KNO_XFUNCTION(prim);
  f->fcn_ndcall = 1;
  return prim;
}

KNO_EXPORT void kno_init_cprims_c()
{
  u8_register_source_file(_FILEINFO);

  kno_functionp[kno_cprim_type]=1;

  kno_unparsers[kno_cprim_type]=unparse_cprim;
  kno_recyclers[kno_cprim_type]=recycle_cprim;
  kno_dtype_writers[kno_cprim_type]=cprim_dtype;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
