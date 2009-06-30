/* -*- Mode: C; character-encoding: utf-8; -*- */

/* qrencode.c
   This implements FramerD bindings to the libqrencode library.
  
   Copyright (C) 2009 beingmeta, inc.
*/

static char versionid[] =
  "$Id: sqlite.c 3265 2008-11-19 17:13:31Z haase $";

#define U8_INLINE_IO 1

#include <png.h>
#include <qrencode.h>

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/numbers.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8digestfns.h>

#if FD_THREADS_ENABLED
static u8_mutex qrencode_lock;
#endif

FD_EXPORT int fd_init_qrcode(void) FD_LIBINIT_FN;

static fdtype l_sym, m_sym, q_sym, h_sym;

static int geteclevel(fdtype level_arg)
{
  if (FD_FIXNUMP(level_arg)) 
    switch (FD_FIX2INT(level_arg)) {
    case 0: return QR_ECLEVEL_L;
    case 1: return QR_ECLEVEL_M;
    case 2: return QR_ECLEVEL_Q; 
    case 3: return QR_ECLEVEL_H;
    default: return -1;}
  else if (FD_SYMBOLP(level_arg)) 
    if (FD_EQ(level_arg,l_sym)) return QR_ECLEVEL_L;
    else if (FD_EQ(level_arg,m_sym)) return QR_ECLEVEL_M;
    else if (FD_EQ(level_arg,q_sym)) return QR_ECLEVEL_Q;
    else if (FD_EQ(level_arg,h_sym)) return QR_ECLEVEL_H;
    else return -1;
  else return -1;
}

static void packet_write_data(png_structp pngptr,png_bytep data,png_size_t len)
{
  struct FD_BYTE_OUTPUT *out=(struct FD_BYTE_OUTPUT *)png_get_io_ptr(pngptr);
  int retval=fd_write_bytes(out,(unsigned char *)data,(size_t)len);
  if (retval<0)
    u8_log(LOG_CRIT,"PNG write error","Error writing PNG file");
}

static void packet_flush_data(png_structp pngptr)
{
}

static void errfn(char *string)
{
  u8_log(LOG_ERR,"LIBPNG","%s",string);
}
static void warnfn(char *string)
{
  u8_log(LOG_WARN,"LIBPNG","%s",string);
}

int dotsize=3;
int imgmargin=3;

static fdtype write_png_packet(QRcode *qrcode)
{
  png_infop info_ptr;
  png_structp png_ptr=png_create_write_struct
    (PNG_LIBPNG_VER_STRING,(png_voidp)NULL,NULL,NULL);
  if (png_ptr==NULL)
    return fd_err("PNG problem","write_png_packet",NULL,FD_VOID);
  info_ptr = png_create_info_struct(png_ptr);
  if (info_ptr==NULL)
    return fd_err("PNG problem","write_png_packet",NULL,FD_VOID);
  else if (setjmp(png_jmpbuf(png_ptr))) {
    png_destroy_write_struct(&png_ptr, &info_ptr);
    return fd_err("PNG problem","write_png_packet",NULL,FD_VOID);}
  else {
    struct FD_BYTE_OUTPUT buf;
    int qrwidth=qrcode->width, qrheight=qrwidth;
    int fullwidth=(qrwidth+(imgmargin*2))*dotsize;
    int rowlen=(fullwidth+7)/8;
    unsigned char *row=u8_malloc(rowlen);
    FD_INIT_BYTE_OUTPUT(&buf,2048);
    png_set_write_fn(png_ptr,(void *)&buf,packet_write_data,packet_flush_data);
    png_set_IHDR(png_ptr, info_ptr,
		 fullwidth,fullwidth,1,
		 PNG_COLOR_TYPE_GRAY,
		 PNG_INTERLACE_NONE,
		 PNG_COMPRESSION_TYPE_DEFAULT,
		 PNG_FILTER_TYPE_DEFAULT);
    png_write_info(png_ptr, info_ptr);

    /* Write top margin */
    memset(row,0xFF,rowlen);
    {int i=0; while (i<(imgmargin*dotsize)) {
	png_write_row(png_ptr,row); i++;}}

    /* Write the content */
    {int vscan=0;
      unsigned char *read=qrcode->data, *readlim=read+qrwidth*qrwidth;
      char *write=row+((imgmargin*dotsize)/8); /* margin offset */
      int bitoff=7-((imgmargin*dotsize)%8);
      while (vscan<qrheight) {
	char *write=row+((imgmargin*dotsize)/8); /* margin offset */
	int bitoff=7-((imgmargin*dotsize)%8), dotscan=0;
	int hscan=0;
	memset(row,0xFF,(fullwidth+7)/8);
	while (hscan<qrwidth) {
	  unsigned char qrdot=*read++;
	  dotscan=0; while (dotscan<dotsize) {
	    *write=(*write)^((qrdot&0x01)<<bitoff);
	    bitoff--; if (bitoff<0) {*write++; bitoff=7;}
	    dotscan++;}
	  hscan++;}
	dotscan=0; while (dotscan<dotsize) {
	  png_write_row(png_ptr, row); dotscan++;}
	vscan++;}}
	
    /* Write bottom margin */
    memset(row,0xFF,rowlen);
    {int i=0; while (i<(imgmargin*dotsize)) {
	png_write_row(png_ptr,row); i++;}}
    
    png_write_end(png_ptr, NULL);
    png_destroy_write_struct(&png_ptr, &info_ptr);

    u8_free(row);
    
    return fd_init_packet(NULL,buf.ptr-buf.start,buf.start);}
}

static fdtype qrencode_prim(fdtype string,fdtype level_arg,fdtype version)
{
  QRecLevel eclevel=geteclevel(level_arg);
  {
    fdtype result;
    QRcode *qrcode=
      ((fd_lock_mutex(&qrencode_lock)),
       QRcode_encodeString8bit
       (FD_STRDATA(string),FD_FIX2INT(version),eclevel));
    u8_unlock_mutex(&qrencode_lock);
    result=write_png_packet(qrcode);
    QRcode_free(qrcode);
    return result;
  }
}

/* Initialization */

static int qrencode_init=0;

FD_EXPORT int fd_init_qrcode()
{
  fdtype module;
  if (qrencode_init) return 0;
  module=fd_new_module("QRCODE",0);
  l_sym=fd_intern("L");
  m_sym=fd_intern("M");
  q_sym=fd_intern("Q");
  h_sym=fd_intern("H");


  fd_defn(module,
	  fd_make_cprim3x("QRENCODE",qrencode_prim,1,
			  fd_string_type,FD_VOID,
			  -1,FD_INT2DTYPE(0),
			  fd_fixnum_type,FD_INT2DTYPE(0)));
  qrencode_init=1;

  fd_finish_module(module);

  fd_register_source_file(versionid);

  return 1;
}
