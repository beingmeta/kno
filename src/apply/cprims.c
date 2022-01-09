/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_QONSTS 1
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
  struct KNO_CPRIM *fcn = (kno_cprim)x;
  u8_string filename = fcn->fcn_filename, space;
  lispval moduleid = fcn->fcn_moduleid;
  u8_string modname =
    (KNO_SYMBOLP(moduleid)) ? (KNO_SYMBOL_NAME(moduleid)) : (NULL);
  u8_byte arity[64]=""; u8_byte codes[64]="";
  u8_byte tmpbuf[32], numbuf[32], namebuf[100];
  u8_string name = fcn->fcn_name, sig = "";
  if ((filename)&&(filename[0]=='\0'))
    filename = NULL;
  if ( (filename) && (space=strchr(filename,' '))) {
    int len = space-filename;
    if (len>30) len=30;
    strncpy(tmpbuf,filename,len);
    tmpbuf[len]='\0';
    filename=tmpbuf;}
  if (filename==NULL) filename="nofile";
  if (FCN_NDOPP(fcn)) strcat(codes,"∀");
  if ((fcn->fcn_arity<0)&&(fcn->fcn_min_arity<0))
    strcat(arity,"[…]");
  else if (fcn->fcn_arity==fcn->fcn_min_arity) {
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
  if ( (name) && (fcn->fcn_argnames) ) {
    struct U8_OUTPUT sigbuf;
    lispval *schema = fcn->fcn_argnames;
    U8_INIT_STATIC_OUTPUT_BUF(sigbuf,sizeof(namebuf),namebuf);
    int i = 0, len = fcn->fcn_arginfo_len;
    u8_putc(&sigbuf,'(');
    u8_puts(&sigbuf,name);
    while (i<len) {
      lispval arg =schema[i];
      if (KNO_SYMBOLP(arg)) {
	u8_putc(&sigbuf,' ');
	u8_puts(&sigbuf,KNO_SYMBOL_NAME(arg));}
      else u8_puts(&sigbuf," ??");
      i++;}
    u8_putc(&sigbuf,')');
    sig=namebuf;}
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
  int free_flags = fn->fcn_free;
  if ( (fn->fcn_typeinfo) && ( (free_flags) & (KNO_FCN_FREE_TYPEINFO) ) )
    u8_free(fn->fcn_typeinfo);
  if ( (fn->fcn_defaults) && ( (free_flags) & (KNO_FCN_FREE_DEFAULTS) ) )
    u8_free(fn->fcn_defaults);
  if ( (fn->fcn_doc) && ( (free_flags) & (KNO_FCN_FREE_DOC) ) )
    u8_free(fn->fcn_doc);
  if ( (fn->fcn_argnames) && ( (free_flags) & (KNO_FCN_FREE_SCHEMA) ) )
    u8_free(fn->fcn_argnames);
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
				    lispval *typeinfo,
				    lispval *defaults)
{
  int arity = ( (flags&0x80) ? (-1) : ( flags & (0x7f) ) );
  int min_arity = (flags&0x8000) ? ( (flags>>8) & 0x7f) : (arity);
  int non_deterministic = flags & KNO_NDCALL;
  int extended_call = flags & KNO_XCALL;
  int varargs = ( (arity < 0) || (flags & KNO_VAR_ARGS) );
  /* We allocate the default info together with the function to
     reduce cache misses. */
  struct KNO_CPRIM *f = u8_alloc(struct KNO_CPRIM);
  int arginfo_len = KNO_ARGINFO_LEN(flags);
  KNO_INIT_FRESH_CONS(f,kno_cprim_type);
  f->fcn_name = name;
  f->fcn_filename = filename;
  f->fcn_doc = doc;
  f->fcn_moduleid = KNO_VOID;
  f->fcn_call = KNO_CALL_NOTAIL | KNO_CALL_CPRIM |
    ( (varargs) ? (KNO_CALL_VARARGS) : (0) ) |
    ( (non_deterministic) ? (KNO_CALL_NDCALL) : (0) ) |
    ( (extended_call) ? (KNO_CALL_XCALL) : (0) );
  f->fcn_call_width = f->fcn_arity = arity;
  f->fcn_trace = f->fcn_other = f->fcn_free = 0;
  f->fcn_min_arity = min_arity;
  f->fcn_arginfo_len = arginfo_len;
  f->fcn_argnames = NULL;
  f->fcn_typeinfo = typeinfo;
  f->fcn_defaults = defaults;
  f->cprim_name = cname;
  if ( (arity>=0) && (min_arity>arity)) {
    u8_log(LOG_CRIT,_("Bad primitive definition"),
	   "Fixing primitive %s%s%s%s with min_arity=%d > arity=%d",
	   name,U8OPTSTR(" (",filename,") "),arity,min_arity);
    f->fcn_min_arity = arity;}
  return f;
}

static struct KNO_CPRIM *make_xcprim(u8_string name,
				     u8_string cname,
				     u8_string filename,
				     u8_string doc,
				     unsigned int flags,
				     int info_len,
				     struct KNO_CPRIM_ARGINFO *arginfo)
{
  int arity = ( (flags&0x80) ? (-1) : ( flags & (0x7f) ) );
  int min_arity = (flags&0x8000) ? ( (flags>>8) & 0x7f) : (arity);
  int non_deterministic = flags & KNO_NDCALL;
  int extended_call = flags & KNO_XCALL;
  int varargs = ( (arity < 0) || (flags & KNO_VAR_ARGS) );
  /* We allocate the type/default info together with the function to
     reduce cache/page misses. We might need to worry about how we're
     figuring out these pointers for non-word-aligned architectures, but
     let's not worry about that for now. */
  if (info_len<0) {
    if (arity<0) info_len=0; else info_len=arity;}
  size_t alloc_size = sizeof(struct KNO_CPRIM) +
    (sizeof(lispval)*info_len) +
    (sizeof(lispval)*info_len) +
    (sizeof(lispval)*info_len);
  void *block = u8_malloc(alloc_size);
  struct KNO_CPRIM *f = (kno_cprim) block;
  KNO_INIT_FRESH_CONS(f,kno_cprim_type);
  lispval *typeinfo = (info_len) ?
    ((lispval *) (block+sizeof(struct KNO_CPRIM))) : (NULL);
  lispval *defaults = (info_len) ?
    ((lispval *) (block+sizeof(struct KNO_CPRIM)+
		  (info_len*sizeof(lispval)) )) :
    (NULL);
  lispval *schema = (info_len) ?
    ((lispval *) (block+sizeof(struct KNO_CPRIM)+
		  (2*(info_len*sizeof(lispval))) )) :
    (NULL);
  int i=0; while (i<info_len) {
    long int typecode  = arginfo[i].argtype;
    if (typecode < 0)
      typeinfo[i]=KNO_VOID;
    else if (typecode < KNO_TYPE_MAX)
      typeinfo[i]=KNO_CTYPE(typecode);
    else {
      lispval use_type = kno_lookup_type_alias(typecode);
      if (KNO_VOIDP(use_type)) {
	u8_log(LOGWARN,"BadTypeAlias",
	       "The type alias 0x%x, used for %s (%s), is not defined",
	       typecode,name,cname);
	typeinfo[i]=KNO_VOID;}
      else  typeinfo[i]=use_type;}

    defaults[i]=arginfo[i].default_value;
    if (arginfo[i].argname)
      schema[i]=kno_intern(arginfo[i].argname);
    else {
      u8_byte buf[32]; u8_sprintf(buf,32,"arg%d",i);
      schema[i]=kno_intern(buf);}
    i++;}
  f->fcn_name = name;
  f->fcn_filename = filename;
  f->fcn_doc = doc;
  f->fcn_moduleid = KNO_VOID;
  f->fcn_call = KNO_CALL_NOTAIL | KNO_CALL_CPRIM |
    ( (varargs) ? (KNO_CALL_VARARGS) : (0) ) |
    ( (non_deterministic) ? (KNO_CALL_NDCALL) : (0) ) |
    ( (extended_call) ? (KNO_CALL_XCALL) : (0) );
  f->fcn_call_width = f->fcn_arity = arity;
  f->fcn_trace = f->fcn_other = f->fcn_free = 0;
  f->fcn_min_arity   = min_arity;
  f->fcn_arginfo_len = info_len;
  f->fcn_argnames    = schema;
  f->fcn_typeinfo    = typeinfo;
  f->fcn_defaults    = defaults;
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
 lispval *typeinfo,
 lispval *defaults)
{
  if (arity >= 0x80) {
    u8_seterr("BadPrimitiveArity","kno_init_cprim",
	      u8_mkstring("Arity for %s/%s in %s is too large: %d",
			  name,cname,filename,arity));
    return NULL;}
  int flag_arity = flags & 0x8F;
  if (flag_arity == 0)
    flags |= KNO_MAX_ARGS(arity);
  else if ( ! ( ( (flag_arity == 0x80) && (arity <= 0) ) ||
	   ( arity == flag_arity ) ) ) {
    u8_log(LOGWARN,"BadArityFlags",
	   "Reparing flags for %s/%s in %s for immediate arity %d",
	   name,cname,filename,arity);
    flags = ( (flags) & (~(0x8F)) ) | ( (arity<0) ? (0x80) : (arity&0x7f) );}
  else NO_ELSE;
  return make_cprim(name,cname,filename,doc,flags,typeinfo,defaults);
}

KNO_EXPORT struct KNO_CPRIM *kno_init_xcprim
(u8_string name,u8_string cname,
 int arity,
 u8_string filename,
 u8_string doc,
 int flags,
 int info_len,
 struct KNO_CPRIM_ARGINFO *info)
{
  if (arity >= 0x80) {
    u8_seterr("BadPrimitiveArity","kno_init_cprim",
	      u8_mkstring("Arity for %s/%s in %s is too large: %d",
			  name,cname,filename,arity));
    return NULL;}
  int flag_arity = flags & 0x8F;
  if (flag_arity == 0)
    flags |= KNO_MAX_ARGS(arity);
  else if ( ! ( ( (flag_arity == 0x80) && (arity <= 0) ) ||
	   ( arity == flag_arity ) ) ) {
    u8_log(LOGWARN,"BadArityFlags",
	   "Reparing flags for %s/%s in %s for immediate arity %d",
	   name,cname,filename,arity);
    flags = ( (flags) & (~(0x8F)) ) | ( (arity<0) ? (0x80) : (arity&0x7f) );}
  else NO_ELSE;
  if (arity < info_len) {
    u8_log(LOGERR,"BadCPrimInfo","For primitive %s/%s in %s, the link arity (%d) is "
	   "less than the provided arginfo",
	   name,cname,filename,arity);
    info_len=arity;}
  return make_xcprim(name,cname,filename,doc,flags,info_len,info);
}

static void link_cprim(struct KNO_CPRIM *cprim,u8_string pname,lispval module)
{
  int rv = kno_store(module,kno_getsym(pname),(lispval)cprim);
  if (rv>0) {
    if ( (KNO_NULLP(cprim->fcn_moduleid)) ||
	 (KNO_VOIDP(cprim->fcn_moduleid)) ) {
      lispval moduleid = kno_get(module,KNOSYM_MODULEID,KNO_VOID);
      if (!(KNO_VOIDP(moduleid)))
	cprim->fcn_moduleid=moduleid;}}
  kno_decref((lispval)cprim);
}

KNO_EXPORT void kno_defcprimN(lispval module,kno_cprimn fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO *arginfo)
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,
     info->docstring,info->flags,info->arginfo_len,arginfo);
  prim->fcn_handler.calln = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim0(lispval module,kno_cprim0 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[0])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,
     info->docstring,info->flags,0,NULL);
  prim->fcn_handler.call0 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim1(lispval module,kno_cprim1 fn,
			     struct KNO_CPRIM_INFO *info,
			     struct KNO_CPRIM_ARGINFO arginfo[1])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     1,arginfo);
  prim->fcn_handler.call1 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim2(lispval module,kno_cprim2 fn,
			     struct KNO_CPRIM_INFO *info,
			     struct KNO_CPRIM_ARGINFO arginfo[2])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     2,arginfo);
  prim->fcn_handler.call2 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim3(lispval module,kno_cprim3 fn,
			     struct KNO_CPRIM_INFO *info,
			     struct KNO_CPRIM_ARGINFO arginfo[3])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     3,arginfo);
  prim->fcn_handler.call3 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim4(lispval module,kno_cprim4 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[4])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     4,arginfo);
  prim->fcn_handler.call4 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim5(lispval module,kno_cprim5 fn,
			     struct KNO_CPRIM_INFO *info,
			     struct KNO_CPRIM_ARGINFO arginfo[5])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     5,arginfo);
  prim->fcn_handler.call5 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim6(lispval module,kno_cprim6 fn,
			     struct KNO_CPRIM_INFO *info,
			     struct KNO_CPRIM_ARGINFO arginfo[6])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     6,arginfo);
  prim->fcn_handler.call6 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim7(lispval module,kno_cprim7 fn,
			     struct KNO_CPRIM_INFO *info,
			     struct KNO_CPRIM_ARGINFO arginfo[7])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     7,arginfo);
  prim->fcn_handler.call7 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim8(lispval module,kno_cprim8 fn,
			     struct KNO_CPRIM_INFO *info,
			     struct KNO_CPRIM_ARGINFO arginfo[8])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     8,arginfo);
  prim->fcn_handler.call8 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim9(lispval module,kno_cprim9 fn,
			     struct KNO_CPRIM_INFO *info,
			     struct KNO_CPRIM_ARGINFO arginfo[9])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     9,arginfo);
  prim->fcn_handler.call9 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim10(lispval module,kno_cprim10 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[10])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     10,arginfo);
  prim->fcn_handler.call10 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim11(lispval module,kno_cprim11 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[11])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     11,arginfo);
  prim->fcn_handler.call11 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim12(lispval module,kno_cprim12 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[12])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     12,arginfo);
  prim->fcn_handler.call12 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim13(lispval module,kno_cprim13 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[13])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     13,arginfo);
  prim->fcn_handler.call13 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim14(lispval module,kno_cprim14 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[14])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     14,arginfo);
  prim->fcn_handler.call14 = fn;
  link_cprim(prim,info->pname,module);
}

KNO_EXPORT void kno_defcprim15(lispval module,kno_cprim15 fn,
			      struct KNO_CPRIM_INFO *info,
			      struct KNO_CPRIM_ARGINFO arginfo[15])
{
  struct KNO_CPRIM *prim = kno_init_xcprim
    (info->pname,info->cname,info->arity,info->filename,info->docstring,info->flags,
     15,arginfo);
  prim->fcn_handler.call15 = fn;
  link_cprim(prim,info->pname,module);
}

/* Providing type info and defaults */

KNO_EXPORT u8_string kno_fcn_sig(struct KNO_FUNCTION *fcn,u8_byte namebuf[100])
{
  u8_string name = NULL;
  struct U8_OUTPUT sigout;
  U8_INIT_FIXED_OUTPUT(&sigout,100,namebuf);
  int i = 0;
  if (fcn->fcn_argnames) {
    lispval *scan = fcn->fcn_argnames;
    lispval *limit = scan+fcn->fcn_arginfo_len;
    while (scan<limit) {
      lispval arg = *scan++;
      if (i>0) u8_putc(&sigout,' '); u8_putc(&sigout,'('); i++;
      if (KNO_SYMBOLP(arg)) {
	u8_puts(&sigout,KNO_SYMBOL_NAME(arg));}
      else u8_puts(&sigout,"??");}
    u8_putc(&sigout,')');}
  else if ( (fcn->fcn_arity>=0) && (fcn->fcn_min_arity>=0) &&
	    (fcn->fcn_arity != fcn->fcn_min_arity) )
    u8_printf(&sigout," [%d,%d] args)",fcn->fcn_min_arity,fcn->fcn_arity);
  else if (fcn->fcn_arity>=0)
    u8_printf(&sigout," %d args)",fcn->fcn_arity);
  else if (fcn->fcn_min_arity>=0)
    u8_printf(&sigout," %d+... args)",fcn->fcn_min_arity);
  else u8_puts(&sigout," args...)");
  name = namebuf;
  return name;
}

KNO_EXPORT void kno_init_cprims_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();

  kno_isfunctionp[kno_cprim_type]=1;

  kno_unparsers[kno_cprim_type]=unparse_cprim;
  kno_recyclers[kno_cprim_type]=recycle_cprim;
  kno_dtype_writers[kno_cprim_type]=cprim_dtype;
}

static void link_local_cprims()
{

}
