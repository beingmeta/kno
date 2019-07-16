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

static lispval moduleid_symbol;

int unparse_cprim(u8_output out,lispval x)
{
  struct KNO_CPRIM *fcn = (kno_cprim)x;
  u8_string filename = fcn->fcn_filename, space;
  lispval moduleid = fcn->fcn_moduleid;
  u8_string modname =
    (KNO_SYMBOLP(moduleid)) ? (KNO_SYMBOL_NAME(moduleid)) : (NULL);
  u8_byte arity[64]=""; u8_byte codes[64]="";
  u8_byte tmpbuf[32], numbuf[32], namebuf[100];
  u8_string name = fcn->fcn_name, sig = kno_fcn_sig((kno_function)fcn,namebuf);
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
  struct KNO_CPRIM *fn = (struct KNO_CPRIM *)c;
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
  struct KNO_CPRIM *fcn = (struct KNO_CPRIM *)x;
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

static struct KNO_CPRIM *make_cprim(u8_string name,
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
  int varargs = ( (arity < 0) || (flags & KNO_LEXPR) );
  /* We allocate the type/default info together with the function to
     reduce cache/page misses. We might need to worry about how we're
     figuring out these pointers for non-word-aligned architectures, but
     let's not worry about that for now. */
  size_t alloc_size = sizeof(struct KNO_CPRIM) +
    ( (typeinfo) ? (sizeof(unsigned int)*arity) : (0) ) +
    ( (defaults) ? (sizeof(lispval)*arity) : (0) );
  void *block = u8_malloc(alloc_size);
  struct KNO_CPRIM *f = (struct KNO_CPRIM *) block;
  unsigned int *prim_typeinfo = (typeinfo) ?
    ((unsigned int *) (block+sizeof(struct KNO_CPRIM))) :
    ((unsigned int *)NULL);
  lispval *prim_defaults = ( (defaults) && (typeinfo) ) ?
    ((lispval *) (block+sizeof(struct KNO_CPRIM)+(sizeof(unsigned int)*arity))) :
    (defaults) ?
    ((lispval *) (block+sizeof(struct KNO_CPRIM)) ) :
    ((lispval *)NULL);
  KNO_INIT_FRESH_CONS(f,kno_cprim_type);
  f->fcn_name = name;
  f->fcn_filename = filename;
  f->fcn_doc = doc;
  f->fcn_moduleid = KNO_VOID;
  if (varargs)
    f->fcn_varargs = 1;
  else f->fcn_varargs = 0;
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
  f->cprim_name = cname;
  if ( (arity>=0) && (min_arity>arity)) {
    u8_log(LOG_CRIT,_("Bad primitive definition"),
	   "Fixing primitive %s%s%s%s with min_arity=%d > arity=%d",
	   name,U8OPTSTR(" (",filename,") "),arity,min_arity);
    f->fcn_min_arity = arity;}
  return f;
}

/* Consing small arity cprims, handy in various places */

KNO_EXPORT lispval kno_cons_cprim0
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprim0 fn)
{
  struct KNO_CPRIM *f=(struct KNO_CPRIM *)
    make_cprim(name,cname,filename,doc,flags,NULL,NULL);
  f->fcn_handler.call0 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_cons_cprim1
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprim1 fn)
{
  struct KNO_CPRIM *f=(struct KNO_CPRIM *)
    make_cprim(name,cname,filename,doc,MAX_ARGS(1)|flags,NULL,NULL);
  f->fcn_handler.call1 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_cons_cprim2
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprim2 fn)
{
  struct KNO_CPRIM *f=(struct KNO_CPRIM *)
    make_cprim(name,cname,filename,doc,MAX_ARGS(2)|flags,NULL,NULL);
  f->fcn_handler.call2 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_cons_cprim3
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprim3 fn)
{
  struct KNO_CPRIM *f=(struct KNO_CPRIM *)
    make_cprim(name,cname,filename,doc,MAX_ARGS(3)|flags,NULL,NULL);
  f->fcn_handler.call3 = fn;
  return LISP_CONS(f);
}

KNO_EXPORT lispval kno_cons_cprimN
(u8_string name,u8_string cname,u8_string filename,u8_string doc,int flags,
 kno_cprimn fn)
{
  struct KNO_CPRIM *f=(struct KNO_CPRIM *)
    make_cprim(name,cname,filename,doc,KNO_VAR_ARGS|flags,NULL,NULL);
  f->fcn_handler.calln = fn;
  return LISP_CONS(f);
}

/* Declaring functions */

KNO_EXPORT struct KNO_CPRIM *kno_init_cprim
(u8_string name,u8_string cname,
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

static void link_cprim(struct KNO_CPRIM *cprim,u8_string pname,lispval module)
{
  int rv = kno_store(module,kno_getsym(pname),(lispval)cprim);
  if (rv>0) {
    if ( (KNO_NULLP(cprim->fcn_moduleid)) ||
	 (KNO_VOIDP(cprim->fcn_moduleid)) ) {
      lispval moduleid = kno_get(module,moduleid_symbol,KNO_VOID);
      if (!(KNO_VOIDP(moduleid)))
	cprim->fcn_moduleid=moduleid;}}
  kno_decref((lispval)cprim);
}

KNO_EXPORT void kno_defprimN(lispval module,kno_cprimn fn,
			     struct KNO_CPRIM_INFO *info)
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,
     info->arity,info->filename,
     info->docstring,info->flags,
     (unsigned int *)NULL,( lispval *)NULL);
  prim->fcn_handler.calln = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim0(lispval module,kno_cprim0 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[0],
			     lispval defaults[0])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call0 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim1(lispval module,kno_cprim1 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[1],
			     lispval defaults[1])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call1 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim2(lispval module,kno_cprim2 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[2],
			     lispval defaults[2])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call2 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim3(lispval module,kno_cprim3 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[3],
			     lispval defaults[3])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call3 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim4(lispval module,kno_cprim4 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[4],
			     lispval defaults[4])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call4 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim5(lispval module,kno_cprim5 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[5],
			     lispval defaults[5])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call5 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim6(lispval module,kno_cprim6 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[6],
			     lispval defaults[6])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call6 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim7(lispval module,kno_cprim7 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[7],
			     lispval defaults[7])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call7 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim8(lispval module,kno_cprim8 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[8],
			     lispval defaults[8])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call8 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim9(lispval module,kno_cprim9 fn,
			     struct KNO_CPRIM_INFO *info,
			     int typeinfo[9],
			     lispval defaults[9])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call9 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim10(lispval module,kno_cprim10 fn,
			      struct KNO_CPRIM_INFO *info,
			      int typeinfo[10],
			      lispval defaults[10])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call10 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim11(lispval module,kno_cprim11 fn,
			      struct KNO_CPRIM_INFO *info,
			      int typeinfo[11],
			      lispval defaults[11])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call11 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim12(lispval module,kno_cprim12 fn,
			      struct KNO_CPRIM_INFO *info,
			      int typeinfo[12],
			      lispval defaults[12])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call12 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim13(lispval module,kno_cprim13 fn,
			      struct KNO_CPRIM_INFO *info,
			      int typeinfo[13],
			      lispval defaults[13])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call13 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim14(lispval module,kno_cprim14 fn,
			      struct KNO_CPRIM_INFO *info,
			      int typeinfo[14],
			      lispval defaults[14])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call14 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defprim15(lispval module,kno_cprim15 fn,
			      struct KNO_CPRIM_INFO *info,
			      int typeinfo[15],
			      lispval defaults[15])
{
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call15 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_typedprim1(lispval module,kno_cprim1 fn,
			       struct KNO_CPRIM_INFO *info,
			       kno_lisp_type type1,lispval dflt1)
{
  unsigned int typeinfo[3] = { type1 };
  lispval defaults[3] = { dflt1 };
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call1 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_typedprim2(lispval module,kno_cprim2 fn,
			       struct KNO_CPRIM_INFO *info,
			       kno_lisp_type type1,lispval dflt1,
			       kno_lisp_type type2,lispval dflt2)
{
  unsigned int typeinfo[3] = { type1, type2 };
  lispval defaults[3] = { dflt1, dflt2 };
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call2 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_typedprim3(lispval module,kno_cprim3 fn,
			       struct KNO_CPRIM_INFO *info,
			       kno_lisp_type type1,lispval dflt1,
			       kno_lisp_type type2,lispval dflt2,
			       kno_lisp_type type3,lispval dflt3)
{
  unsigned int typeinfo[3] = { type1, type2, type3 };
  lispval defaults[3] = { dflt1, dflt2, dflt3 };
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call3 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_typedprim4(lispval module,kno_cprim4 fn,
			       struct KNO_CPRIM_INFO *info,
			       kno_lisp_type type1,lispval dflt1,
			       kno_lisp_type type2,lispval dflt2,
			       kno_lisp_type type3,lispval dflt3,
			       kno_lisp_type type4,lispval dflt4)
{
  unsigned int typeinfo[4] = { type1, type2, type3, type4 };
  lispval defaults[4] = { dflt1, dflt2, dflt3, dflt4 };
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call4 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_typedprim5(lispval module,kno_cprim5 fn,
			       struct KNO_CPRIM_INFO *info,
			       kno_lisp_type type1,lispval dflt1,
			       kno_lisp_type type2,lispval dflt2,
			       kno_lisp_type type3,lispval dflt3,
			       kno_lisp_type type4,lispval dflt4,
			       kno_lisp_type type5,lispval dflt5)
{
  unsigned int typeinfo[5] = { type1, type2, type3, type4, type5 };
  lispval defaults[5] = { dflt1, dflt2, dflt3, dflt4, dflt5 };
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call5 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_typedprim6(lispval module,kno_cprim6 fn,
			       struct KNO_CPRIM_INFO *info,
			       kno_lisp_type type1,lispval dflt1,
			       kno_lisp_type type2,lispval dflt2,
			       kno_lisp_type type3,lispval dflt3,
			       kno_lisp_type type4,lispval dflt4,
			       kno_lisp_type type5,lispval dflt5,
			       kno_lisp_type type6,lispval dflt6)
{
  unsigned int typeinfo[6] = { type1, type2, type3, type4, type5, type6 };
  lispval defaults[6] = { dflt1, dflt2, dflt3, dflt4, dflt5, dflt6 };
  struct KNO_CPRIM *prim = kno_init_cprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     (unsigned int *)typeinfo,( lispval *)defaults);
  prim->fcn_handler.call6 = fn;
  link_cprim(prim,info->pname,module);
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

KNO_EXPORT void kno_init_cprims_c()
{
  u8_register_source_file(_FILEINFO);

  init_local_cprims();

  moduleid_symbol = kno_intern("%moduleid");

  kno_functionp[kno_cprim_type]=1;

  kno_unparsers[kno_cprim_type]=unparse_cprim;
  kno_recyclers[kno_cprim_type]=recycle_cprim;
  kno_dtype_writers[kno_cprim_type]=cprim_dtype;
}

static void init_local_cprims()
{

}
