/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1

#undef KNO_DEBUG_DTYPEIO
#define KNO_DEBUG_DTYPEIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/dtypeio.h"

#include <libu8/u8elapsed.h>

#include <zlib.h>
#include <errno.h>

#ifndef KNO_DEBUG_DTYPEIO
#define KNO_DEBUG_DTYPEIO 0
#endif

KNO_EXPORT lispval kno_unpack_exception_vector(lispval content);

static u8_mutex dtype_unpacker_lock;

u8_condition kno_UnexpectedEOD=_("Unexpected end of data");
u8_condition kno_DTypeError=_("Malformed DTYPE representation");

int kno_dtype_fixcase;

static lispval error_symbol;

#define newpos(pos,ptr,lim) ((((ptr)+pos) <= lim) ? (pos) : (-1))

static ssize_t validate_dtype(int pos,const unsigned char *ptr,
                              const unsigned char *lim)
{
  if (pos < 0)
    return pos;
  else if (ptr+pos >= lim)
    return -1;
  else {
    int code = ptr[pos];
    switch (code) {
    case dt_empty_list: case dt_void:
    case dt_empty_choice: case dt_default_value:
      return newpos(pos+1,ptr,lim);
    case dt_boolean:
      return newpos(pos+2,ptr,lim);
    case dt_fixnum: case dt_flonum:
      return newpos(pos+5,ptr,lim);
    case dt_oid: return newpos(pos+9,ptr,lim);
    case dt_error: case dt_exception:
      return validate_dtype(newpos(pos+1,ptr,lim),ptr,lim);
    case dt_pair: case dt_compound: case dt_rational: case dt_complex:
      return validate_dtype(validate_dtype(validate_dtype(pos+1,ptr,lim),
                                           ptr,lim),
                            ptr,lim);
    case dt_tiny_symbol: case dt_tiny_string:
      if (ptr+pos+1 >= lim) return -1;
      else return newpos(pos+1+ptr[pos+1],ptr,lim);
    case dt_tiny_choice:
      if (ptr+pos+1 >= lim) return -1;
      else {
        int i = 0, len = ptr[pos+1];
        while (i<len) {
          int i = 0, len = ptr[pos+1], npos = pos+2;
          while ((i < len) && (npos > 0))
            npos = validate_dtype(npos,ptr,lim);
          return npos;}}
    case dt_symbol: case dt_packet: case dt_string: case dt_zstring:
      if (ptr+pos+5 >= lim) return -1;
      else {
        int len = kno_get_4bytes(ptr+pos+1);
        return newpos(pos+5+len,ptr,lim);}
    case dt_vector:
      if (ptr+pos+5 >= lim) return -1;
      else {
        int i = 0, len = kno_get_4bytes(ptr+pos+1), npos = pos+5;
        while ((i < len) && (npos > 0))
          npos = validate_dtype(npos,ptr,lim);
        return npos;}
    default:
      if ((code > 0x40) && (ptr+pos+2 < lim)) {
        int subcode = *(ptr+pos+1), len, npos;
        if (subcode&0x40)
          if (ptr+pos+6 < lim) {
            npos = pos+6;
            len = kno_get_4bytes(ptr+pos+2);}
          else return -1;
        else if (ptr+pos+3 < lim) {
          npos = pos+3;
          len = kno_get_byte(ptr+pos+2);}
        else return -1;
        if (subcode&0x80) {
          int i = 0; while ((i < len) && (npos > 0)) {
            npos = validate_dtype(npos,ptr,lim); i++;}
          return npos;}
        else return newpos(npos+len,ptr,lim);}
      else return -1;}}
}

KNO_EXPORT ssize_t kno_validate_dtype(struct KNO_INBUF *in)
{
  return validate_dtype(0,in->bufread,in->buflim);
}

/* Byte input */

#define nobytes(in,nbytes) (RARELY(!(kno_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (USUALLY(kno_request_bytes(in,nbytes)))

KNO_EXPORT lispval kno_make_mystery_packet(int,int,unsigned int,unsigned char *);
KNO_EXPORT lispval kno_make_mystery_vector(int,int,unsigned int,lispval *);
static lispval read_packaged_dtype(int,struct KNO_INBUF *);

static lispval *read_dtypes(int n,struct KNO_INBUF *in,
                            lispval *why_not,lispval *into)
{
  if (n==0) return NULL;
  else {
    lispval *vec = ((into)?(into):(u8_alloc_n(n,lispval)));
    int i = 0; while (i < n) {
      lispval v = kno_read_dtype(in);
      if (KNO_COOLP(v))
        vec[i++]=v;
      else {
        int j = 0; while (j<i) {kno_decref(vec[j]); j++;}
        u8_free(vec); *why_not = v;
        return NULL;}}
    return vec;}
}

static lispval unexpected_eod()
{
  return kno_err1(kno_UnexpectedEOD);
}

KNO_EXPORT lispval kno_read_dtype(struct KNO_INBUF *in)
{
  if (RARELY(KNO_ISWRITING(in)))
    return kno_lisp_iswritebuf(in);
  else if (havebytes(in,1)) {
    int code = *(in->bufread++);
    long long len=-1;
    switch (code) {
    case dt_empty_list: return NIL;
    case dt_void: return VOID;
    case dt_empty_choice: return EMPTY;
    case dt_default_value:
      return KNO_DEFAULT_VALUE;
    case dt_boolean:
      if (nobytes(in,1))
        return kno_return_errcode(KNO_EOD);
      else if (*(in->bufread++))
        return KNO_TRUE;
      else return KNO_FALSE;
    case dt_fixnum:
      if (havebytes(in,4)) {
        int intval = kno_get_4bytes(in->bufread);
        in->bufread = in->bufread+4;
        return KNO_INT(intval);}
      else return kno_return_errcode(KNO_EOD);
    case dt_flonum: {
      char bytes[4];
      double flonum;
      float *f = (float *)&bytes;
      unsigned int *i = (unsigned int *)&bytes, num;
      long long result=kno_read_4bytes(in);
      if (result<0)
        return unexpected_eod();
      else num=(unsigned int)result;
      *i = num; flonum = *f;
      return _kno_make_double(flonum);}
    case dt_oid: {
      long long hival=kno_read_4bytes(in), loval;
      if (RARELY(hival<0))
        return unexpected_eod();
      else loval=kno_read_4bytes(in);
      if (RARELY(loval<0))
        return unexpected_eod();
      else {
        KNO_OID addr = KNO_NULL_OID_INIT;
        KNO_SET_OID_HI(addr,hival);
        KNO_SET_OID_LO(addr,loval);
        return kno_make_oid(addr);}}
    case dt_error: {
      lispval content = kno_read_dtype(in);
      if (KNO_ABORTP(content))
        return content;
      else return kno_init_compound(NULL,error_symbol,0,1,content);}
    case dt_exception: {
      lispval content = kno_read_dtype(in);
      if (KNO_ABORTP(content))
        return content;
      else return kno_unpack_exception_vector(content);}
    case dt_pair: {
      lispval head = NIL, *tail = &head;
      while (1) {
        lispval car = kno_read_dtype(in);
        if (KNO_ABORTP(car)) {
          kno_decref(head);
          return car;}
        else {
          lispval new_pair=
            kno_init_pair(u8_alloc(struct KNO_PAIR),
                          car,NIL);
          int dtcode = kno_read_byte(in);
          if (dtcode<0) {
            kno_decref(head);
            kno_decref(new_pair);
            return unexpected_eod();}
          *tail = new_pair;
          tail = &(KNO_CDR(new_pair));
          if (dtcode != dt_pair) {
            lispval cdr;
            if (kno_unread_byte(in,dtcode)<0) {
              kno_decref(head);
              return KNO_ERROR;}
            cdr = kno_read_dtype(in);
            if (KNO_ABORTP(cdr)) {
              kno_decref(head);
              return cdr;}
            *tail = cdr;
            return head;}}}}
    case dt_compound: case dt_rational: case dt_complex: {
      lispval car = kno_read_dtype(in), cdr;
      if (KNO_TROUBLEP(car))
        return car;
      else cdr = kno_read_dtype(in);
      if (KNO_TROUBLEP(cdr)) {
        kno_decref(car);
        return cdr;}
      else switch (code) {
        case dt_compound: {
          struct KNO_TYPEINFO *e = kno_use_typeinfo(car);
	  if ((e) && (e->type_restorefn)) {
            lispval result = e->type_restorefn(car,cdr,e);
	    kno_decref(car);
            kno_decref(cdr);
            return result;}
	  else if ((e) && (kno_default_restorefn)) {
            lispval result = kno_default_restorefn(car,cdr,e);
            kno_decref(car);
            kno_decref(cdr);
            return result;}
          else {
            int flags = KNO_COMPOUND_USEREF;
            if (e) {
              if (e->type_isopaque)
                flags |= KNO_COMPOUND_OPAQUE;
              if (e->type_ismutable)
                flags |= KNO_COMPOUND_MUTABLE;}
	    else if (KNO_VECTORP(cdr)) {
	      flags |= KNO_COMPOUND_SEQUENCE;}
	    else NO_ELSE;
	    if (KNO_VECTORP(cdr)) {
              struct KNO_VECTOR *vec = (struct KNO_VECTOR *)cdr;
              lispval result = kno_init_compound_from_elts
                (NULL,car,flags,vec->vec_length,vec->vec_elts);
              vec->vec_length = 0; vec->vec_elts = NULL;
              kno_decref(cdr);
              return result;}
            else return kno_init_compound(NULL,car,flags,1,cdr);}}
        case dt_rational:
          return _kno_make_rational(car,cdr);
        case dt_complex:
          return _kno_make_complex(car,cdr);}}
    case dt_packet: case dt_string:
      len=kno_read_4bytes(in);
      if (len<0)
        return unexpected_eod();
      else if (nobytes(in,len))
        return unexpected_eod();
      else {
        lispval result = VOID;
        switch (code) {
        case dt_string:
          result = kno_make_string(NULL,len,in->bufread); break;
        case dt_packet:
          result = kno_make_packet(NULL,len,in->bufread); break;}
        in->bufread = in->bufread+len;
        return result;}
    case dt_tiny_symbol:
      len = kno_read_byte(in);
      if (len<0)
        return unexpected_eod();
      else if (nobytes(in,len))
        return unexpected_eod();
      else {
        u8_byte data[257];
        memcpy(data,in->bufread,len); data[len]='\0';
        in->bufread += len;
        if ( ( (in->buf_flags) & KNO_FIX_DTSYMS) ||
             ( kno_dtype_fixcase ) )
          return kno_fixcase_symbol(data,len);
        else return kno_make_symbol(data,len);}
    case dt_tiny_string:
      len = kno_read_byte(in);
      if (len<0)
        return unexpected_eod();
      else if (nobytes(in,len))
        return unexpected_eod();
      else {
        lispval result = kno_make_string(NULL,len,in->bufread);
        in->bufread += len;
        return result;}
    case dt_symbol: case dt_zstring:
      len = kno_read_4bytes(in);
      if ( (len<0) || (nobytes(in,len)) )
        return kno_return_errcode(KNO_EOD);
      else {
        unsigned char data[len+1];
        memcpy(data,in->bufread,len); data[len]='\0';
        in->bufread += len;
        if ( ( (in->buf_flags) & KNO_FIX_DTSYMS) ||
             ( kno_dtype_fixcase ) )
          return kno_fixcase_symbol(data,len);
        else return kno_make_symbol(data,len);}
    case dt_vector:
      len = kno_read_4bytes(in);
      if (len < 0)
        return kno_return_errcode(KNO_EOD);
      else if (RARELY(len == 0))
        return kno_empty_vector(0);
      else {
        lispval why_not = KNO_EOD, result = kno_empty_vector(len);
        lispval *elts = KNO_VECTOR_ELTS(result);
        lispval *data = read_dtypes(len,in,&why_not,elts);
        if (USUALLY((data!=NULL)))
          return result;
        else return KNO_ERROR;}
    case dt_tiny_choice:
      len=kno_read_byte(in);
      if (len<0)
        return unexpected_eod();
      else {
        struct KNO_CHOICE *ch = kno_alloc_choice(len);
        lispval *write = (lispval *)KNO_XCHOICE_DATA(ch);
        lispval *limit = write+len;
        while (write<limit) {
          lispval v = kno_read_dtype(in);
          if (KNO_ABORTP(v)) {
            kno_free_choice(ch);
            return v;}
          *write++=v;}
        return kno_init_choice(ch,len,NULL,(KNO_CHOICE_DOSORT|KNO_CHOICE_REALLOC));}
    case dt_block:
      len=kno_read_4bytes(in);
      if ( (len<0) || (nobytes(in,len)) )
        return unexpected_eod();
      else return kno_read_dtype(in);
    case dt_kno_package: {
      int code, lenlen;
      if (nobytes(in,2))
        return unexpected_eod();
      code = *(in->bufread++); lenlen = ((code&0x40) ? 4 : 1);
      if (lenlen==4)
        len = kno_read_4bytes(in);
      else len = kno_read_byte(in);
      if (len < 0)
        return unexpected_eod();
      else switch (code) {
        case dt_qchoice: case dt_small_qchoice:
          if (len==0)
            return kno_init_qchoice(u8_alloc(struct KNO_QCHOICE),EMPTY);
        case dt_choice: case dt_small_choice:
          if (len==0)
            return EMPTY;
          else {
            lispval result;
            struct KNO_CHOICE *ch = kno_alloc_choice(len);
            lispval *write = (lispval *)KNO_XCHOICE_DATA(ch);
            lispval *limit = write+len;
            while (write<limit) {
              lispval v=kno_read_dtype(in);
              if (KNO_ABORTP(v)) {
                lispval *elts=(lispval *)KNO_XCHOICE_DATA(ch);
                kno_decref_elts(elts,write-elts);
                u8_big_free(ch);
                return v;}
              else *write++=v;}
            result = kno_init_choice(ch,len,NULL,(KNO_CHOICE_DOSORT|KNO_CHOICE_REALLOC));
            if (CHOICEP(result))
              if ((code == dt_qchoice) || (code == dt_small_qchoice))
                return kno_init_qchoice(u8_alloc(struct KNO_QCHOICE),result);
              else return result;
            else return result;}
        case dt_slotmap: case dt_small_slotmap:
          if (len==0)
            return kno_empty_slotmap();
          else {
            int n_slots = len/2;
            struct KNO_KEYVAL *keyvals = u8_alloc_n(n_slots,struct KNO_KEYVAL);
            struct KNO_KEYVAL *write = keyvals, *limit = keyvals+n_slots;
            while (write<limit) {
              lispval key = kno_read_dtype(in), value;
              if (KNO_ABORTP(key)) value=key;
              else value = kno_read_dtype(in);
              if (KNO_ABORTP(value)) {
                struct KNO_KEYVAL *scan=keyvals;
                if (!(KNO_ABORTP(key))) kno_decref(key);
                while (scan<write) {
                  kno_decref(scan->kv_key);
                  kno_decref(scan->kv_val);
                  scan++;}
                u8_free(keyvals);
                return value;}
              else {
                write->kv_key = key;
                write->kv_val = value;
                write++;}}
            return kno_init_slotmap(NULL,n_slots,keyvals);}
        case dt_hashtable: case dt_small_hashtable:
          if (len==0)
            return kno_init_hashtable(NULL,0,NULL);
          else {
            int n_keys = len/2, n_read = 0;
            struct KNO_KEYVAL *keyvals = u8_big_alloc_n(n_keys,struct KNO_KEYVAL);
            while (n_read<n_keys) {
              lispval key = kno_read_dtype(in), value;
              if (KNO_ABORTP(key)) value=key;
              else value = kno_read_dtype(in);
              if (KNO_ABORTP(value)) {
                if (!(KNO_ABORTP(key))) kno_decref(key);
                int j = 0; while (j<n_read) {
                  kno_decref(keyvals[j].kv_key);
                  kno_decref(keyvals[j].kv_val);
                  j++;}
                u8_big_free(keyvals);
                return value;}
              else if ( (EMPTYP(value)) || (EMPTYP(key)) ) {
                kno_decref(key);
                kno_decref(value);}
              else {
                keyvals[n_read].kv_key = key;
                keyvals[n_read].kv_val = value;
                n_read++;}}
            lispval table = kno_initialize_hashtable(NULL,keyvals,n_read);
            u8_big_free(keyvals);
            return table;}
        case dt_hashset: case dt_small_hashset: {
          int i = 0;
          struct KNO_HASHSET *h = u8_alloc(struct KNO_HASHSET);
          kno_init_hashset(h,len,KNO_MALLOCD_CONS);
          while (i<len) {
            lispval v = kno_read_dtype(in);
            if (KNO_ABORTP(v)) {
              kno_decref((lispval)h);
              return v;}
            kno_hashset_add_raw(h,v);
            i++;}
          return LISP_CONS(h);}
        default: {
          int i = 0; lispval *data = u8_alloc_n(len,lispval);
          while (i<len) {
            lispval v=kno_read_dtype(in);
            if (KNO_ABORTP(v)) {
              kno_decref_elts(data,i);
              u8_free(data);
              return v;}
            else data[i++]=kno_read_dtype(in);}
          return kno_make_mystery_vector(dt_kno_package,code,len,data);}}}
    default:
      if ((code >= 0x40) && (code < 0x80))
        return read_packaged_dtype(code,in);
      else return KNO_DTYPE_ERROR;}}
  else return kno_return_errcode(KNO_EOD);
}

/* Vector and packet unpackers */

struct  KNO_DTYPE_PACKAGE {
  kno_packet_unpacker packetfns[64];
  kno_vector_unpacker vectorfns[64];};
struct KNO_DTYPE_PACKAGE *kno_dtype_packages[64];

KNO_EXPORT int kno_register_vector_unpacker
(unsigned int package,unsigned int code,kno_vector_unpacker f)
{
  int package_offset = package-0x40, code_offset = ((code-0x80)&(~(0x40)));
  int replaced = 0;
  if ((package<0x40) || (package>0x80)) return -1;
  else if ((code&0x80)==0) return -2;
  u8_lock_mutex(&dtype_unpacker_lock);
  if (kno_dtype_packages[package_offset]==NULL) {
    struct KNO_DTYPE_PACKAGE *pkg=
      kno_dtype_packages[package_offset]=
      u8_alloc(struct KNO_DTYPE_PACKAGE);
    memset(pkg,0,sizeof(struct KNO_DTYPE_PACKAGE));
    replaced = 2;}
  else if (kno_dtype_packages[package_offset]->vectorfns[code_offset])
    replaced = 1;
  kno_dtype_packages[package_offset]->vectorfns[code_offset]=f;
  u8_unlock_mutex(&dtype_unpacker_lock);
  return replaced;
}

KNO_EXPORT int kno_register_packet_unpacker
(unsigned int package,unsigned int code,kno_packet_unpacker f)
{
  int package_offset = package-0x40, code_offset = ((code)&(~(0x40)));
  int replaced = 0;
  if ((package<0x40) || (package>0x80)) return -1;
  else if ((code&0x80)) return -2;
  u8_lock_mutex(&dtype_unpacker_lock);
  if (kno_dtype_packages[package_offset]==NULL) {
    struct KNO_DTYPE_PACKAGE *pkg=
      kno_dtype_packages[package_offset]=
      u8_alloc(struct KNO_DTYPE_PACKAGE);
    memset(pkg,0,sizeof(struct KNO_DTYPE_PACKAGE));
    replaced = 2;}
  else if (kno_dtype_packages[package_offset]->packetfns[code_offset])
    replaced = 1;
  kno_dtype_packages[package_offset]->packetfns[code_offset]=f;
  u8_unlock_mutex(&dtype_unpacker_lock);
  return replaced;
}

/* Reading packaged dtypes */

static lispval make_character_type(int code,int len,unsigned char *data,int);

static lispval read_packaged_dtype
(int package,struct KNO_INBUF *in)
{
  lispval *vector = NULL; unsigned char *packet = NULL;
  long long len;
  unsigned int code, lenlen, vectorp;
  if (nobytes(in,2))
    return unexpected_eod();
  code = *(in->bufread++);
  lenlen = ((code&0x40) ? 4 : 1);
  vectorp = (code&0x80);
  if (nobytes(in,lenlen))
    return unexpected_eod();
  else if (lenlen==4)
    len = kno_read_4bytes(in);
  else len = kno_read_byte(in);
  if (len<0)
    return unexpected_eod();
  else if (vectorp) {
    lispval why_not;
    vector = read_dtypes(len,in,&why_not,NULL);
    if ( (len>0) && (vector == NULL))
      return why_not;}
  else if (nobytes(in,len))
    return unexpected_eod();
  else {
    packet = u8_malloc(len);
    memcpy(packet,in->bufread,len);
    in->bufread = in->bufread+len;}
  switch (package) {
  case dt_character_package:
    if (vectorp)
      return kno_make_mystery_vector(package,code,len,vector);
    else return make_character_type(code,len,packet,in->buf_flags);
  default:
    if (vectorp)
      return kno_make_mystery_vector(package,code,len,vector);
    else return kno_make_mystery_packet(package,code,len,packet);
  }
}

static lispval make_character_type(int code,
                                   int len,unsigned char *bytes,
                                   int dtflags)
{
  switch (code) {
  case dt_ascii_char: {
    int c = bytes[0]; u8_free(bytes);
    return KNO_CODE2CHAR(c);}
  case dt_unicode_char: {
    int c = 0, i = 0; while (i<len) {
      c = c<<8|bytes[i]; i++;}
    u8_free(bytes);
    return KNO_CODE2CHAR(c);}
  case dt_unicode_short_string: case dt_unicode_string: {
    u8_byte buf[256];
    struct U8_OUTPUT os; unsigned char *scan, *limit; lispval result;
    U8_INIT_OUTPUT_X(&os,256,buf,0);
    scan = bytes; limit = bytes+len;
    while (scan < limit) {
      int c = scan[0]<<8|scan[1]; scan = scan+2;
      u8_putc(&os,c);}
    u8_free(bytes);
    result = kno_make_string(NULL,os.u8_write-os.u8_outbuf,os.u8_outbuf);
    u8_close_output(&os);
    return result;}
  case dt_secret_packet: case dt_short_secret_packet: {
    lispval result = kno_make_packet(NULL,len,bytes);
    KNO_SET_CONS_TYPE(result,kno_secret_type);
    u8_free(bytes);
    return result;}
  case dt_unicode_short_symbol: case dt_unicode_symbol: {
    lispval sym;
    struct U8_OUTPUT os; unsigned char *scan, *limit;
    U8_INIT_OUTPUT(&os,len);
    scan = bytes; limit = bytes+len;
    while (scan < limit) {
      int c = scan[0]<<8|scan[1]; scan = scan+2;
      u8_putc(&os,c);}
    if ( ( (dtflags) & KNO_FIX_DTSYMS) ||
         ( kno_dtype_fixcase ) )
      sym = kno_fixcase_symbol(os.u8_outbuf,os.u8_write-os.u8_outbuf);
    else sym = kno_make_symbol(os.u8_outbuf,os.u8_write-os.u8_outbuf);
    u8_free(bytes);
    u8_free(os.u8_outbuf);
    return sym;}
  default:
    return kno_make_mystery_packet(dt_character_package,code,len,bytes);
  }
}

KNO_EXPORT lispval kno_make_mystery_packet
(int package,int typecode,unsigned int len,unsigned char *bytes)
{
  struct KNO_MYSTERY_DTYPE *myst;
  int pkg_offset = package-0x40;
  int code_offset = (typecode&0x3F);
  if ((pkg_offset<0x40) && (kno_dtype_packages[pkg_offset]) &&
      (kno_dtype_packages[pkg_offset]->packetfns[code_offset]))
    return (kno_dtype_packages[pkg_offset]->packetfns[code_offset])(len,bytes);
  myst = u8_alloc(struct KNO_MYSTERY_DTYPE);
  KNO_INIT_CONS(myst,kno_mystery_type);
  myst->myst_dtpackage = package;
  myst->myst_dtcode = typecode;
  myst->mystery_payload.bytes = bytes;
  myst->myst_dtsize = len;
  return LISP_CONS(myst);
}

KNO_EXPORT lispval kno_make_mystery_vector
(int package,int typecode,unsigned int len,lispval *elts)
{
  struct KNO_MYSTERY_DTYPE *myst;
  int pkg_offset = package-0x40;
  int code_offset = (typecode&0x3F);
  if ((pkg_offset<0x40) && (kno_dtype_packages[pkg_offset]) &&
      (kno_dtype_packages[pkg_offset]->vectorfns[code_offset]))
    return (kno_dtype_packages[pkg_offset]->vectorfns[code_offset])(len,elts);
  myst = u8_alloc(struct KNO_MYSTERY_DTYPE);
  KNO_INIT_CONS(myst,kno_mystery_type);
  myst->myst_dtpackage = package; myst->myst_dtcode = typecode;
  myst->mystery_payload.elts = elts; myst->myst_dtsize = len;
  return LISP_CONS(myst);
}

/* Arith stubs */

static lispval default_make_rational(lispval car,lispval cdr)
{
  struct KNO_PAIR *p = u8_alloc(struct KNO_PAIR);
  KNO_INIT_CONS(p,kno_rational_type);
  p->car = car; p->cdr = cdr;
  return LISP_CONS(p);
}

static void default_unpack_rational
(lispval x,lispval *car,lispval *cdr)
{
  struct KNO_PAIR *p = kno_consptr(struct KNO_PAIR *,x,kno_rational_type);
  *car = p->car; *cdr = p->cdr;
}

static lispval default_make_complex(lispval car,lispval cdr)
{
  struct KNO_PAIR *p = u8_alloc(struct KNO_PAIR);
  KNO_INIT_CONS(p,kno_complex_type);
  p->car = car;
  p->cdr = cdr;
  return LISP_CONS(p);
}

static void default_unpack_complex
(lispval x,lispval *car,lispval *cdr)
{
  struct KNO_PAIR *p = kno_consptr(struct KNO_PAIR *,x,kno_complex_type);
  *car = p->car;
  *cdr = p->cdr;
}

static lispval default_make_double(double d)
{
  struct KNO_FLONUM *ds = u8_alloc(struct KNO_FLONUM);
  KNO_INIT_CONS(ds,kno_flonum_type);
  ds->floval = d;
  return LISP_CONS(ds);
}

lispval(*_kno_make_rational)(lispval car,lispval cdr) = default_make_rational;
void
(*_kno_unpack_rational)(lispval,lispval *,lispval *) = default_unpack_rational;
lispval (*_kno_make_complex)(lispval car,lispval cdr) = default_make_complex;
void (*_kno_unpack_complex)(lispval,lispval *,lispval *) = default_unpack_complex;
lispval (*_kno_make_double)(double) = default_make_double;

/* Reading and writing compressed dtypes */

static unsigned char *do_uncompress
(Bytef *fdata,size_t n_bytes,ssize_t *dbytes,Bytef *init_xbuf,ssize_t buflen)
{
  Bytef *xbuf=init_xbuf;
  int error=0;
  if (init_xbuf==NULL) {
    if (buflen<0) buflen=4*n_bytes;
    xbuf=u8_malloc(buflen);}
  uLongf xbuf_size=buflen;
  while ((error = uncompress(xbuf,&xbuf_size,fdata,n_bytes)) < Z_OK)
    if (error == Z_MEM_ERROR) {
      if (xbuf!=init_xbuf) u8_free(xbuf);
      return KNO_ERR1(NULL,"ZLIB Out of Memory");}
    else if (error == Z_BUF_ERROR) {
      Bytef *newbuf;
      if (xbuf == init_xbuf)
        newbuf = u8_malloc(buflen*2);
      else newbuf = u8_realloc(xbuf,buflen*2);
      if (newbuf == NULL) {
        u8_seterr(kno_MallocFailed,"do_uncompress",NULL);
        if (xbuf == init_xbuf) u8_free(xbuf);
        return NULL;}
      xbuf=newbuf;
      buflen=buflen*2;
      xbuf_size=buflen;}
    else if (error == Z_DATA_ERROR) {
      if (xbuf == init_xbuf) u8_free(xbuf);
      return KNO_ERR1(NULL,"ZLIB Data error");}
    else if (error == Z_STREAM_ERROR) {
      if (xbuf == init_xbuf) u8_free(xbuf);
      return KNO_ERR1(NULL,"ZLIB Data error");}
    else {
      if (xbuf == init_xbuf) u8_free(xbuf);
      return KNO_ERR1(NULL,"Bad ZLIB return code");}
  *dbytes = xbuf_size;
  return xbuf;
}

/* This reads a non frame value with compression. */
KNO_EXPORT lispval kno_zread_dtype(struct KNO_INBUF *in)
{
  lispval result;
  ssize_t n_bytes = kno_read_varint(in), dbytes;
  unsigned char *bytes = u8_malloc(n_bytes);
  int retval = kno_read_bytes(bytes,in,n_bytes);
  struct KNO_INBUF tmp = { 0 };
  if (retval<n_bytes) {
    u8_free(bytes);
    return KNO_ERROR;}
  tmp.bufread = tmp.buffer = do_uncompress(bytes,n_bytes,&dbytes,NULL,-1);
  tmp.buf_flags = KNO_HEAP_BUFFER;
  tmp.buflim = tmp.buffer+dbytes;
  result = kno_read_dtype(&tmp);
  u8_free(bytes);
  u8_free(tmp.buffer);
  return result;
}
/* File initialization */

KNO_EXPORT void kno_init_dtread_c()
{
  u8_register_source_file(_FILEINFO);

  error_symbol = kno_intern("%error");

  kno_register_config
    ("USEDTBLOCK",_("Use the DTBLOCK dtype code when appropriate"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_use_dtblock);
  kno_register_config
    ("CHECKDTSIZE",_("whether to check returned and real dtype sizes"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_check_dtsize);

  u8_init_mutex(&(dtype_unpacker_lock));
}

