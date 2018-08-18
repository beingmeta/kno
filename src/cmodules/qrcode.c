/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* qrencode.c
   This implements FramerD bindings to the libqrencode library.

   Copyright (C) 2009-2018 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include <png.h>
#include <qrencode.h>

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

static lispval dotsize_symbol, margin_symbol, version_symbol, robustness_symbol;
static lispval l_sym, m_sym, q_sym, h_sym;

static u8_mutex qrencode_lock;

FD_EXPORT int fd_init_qrcode(void) FD_LIBINIT_FN;

static int geteclevel(lispval level_arg)
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
  else return QR_ECLEVEL_Q;
}

static void packet_write_data(png_structp pngptr,png_bytep data,png_size_t len)
{
  struct FD_OUTBUF *out = (struct FD_OUTBUF *)png_get_io_ptr(pngptr);
  int retval = fd_write_bytes(out,(unsigned char *)data,(size_t)len);
  if (retval<0)
    u8_log(LOG_CRIT,"PNG write error","Error writing PNG file");
}

static void packet_flush_data(png_structp pngptr)
{
}

int default_dotsize = 3;
int default_margin = 3;

static lispval write_png_packet(QRcode *qrcode,lispval opts)
{
  png_infop info_ptr;
  png_structp png_ptr = png_create_write_struct
    (PNG_LIBPNG_VER_STRING,(png_voidp)NULL,NULL,NULL);
  lispval dotsize_arg = fd_getopt(opts,dotsize_symbol,FD_INT(default_dotsize));
  lispval margin_arg = fd_getopt(opts,margin_symbol,FD_INT(default_margin));
  int dotsize, margin;
  /* Check for errors */
  if (png_ptr == NULL)
    return fd_err("PNG problem","write_png_packet",NULL,FD_VOID);
  else info_ptr = png_create_info_struct(png_ptr);
  if (info_ptr == NULL)
    return fd_err("PNG problem","write_png_packet",NULL,FD_VOID);
  if ((FD_INTP(dotsize_arg)) && ((FD_FIX2INT(dotsize_arg))>0))
    dotsize = FD_FIX2INT(dotsize_arg);
  else return fd_type_error("positive fixnum","write_png_packet",dotsize_arg);
  if ((FD_INTP(margin_arg)) && ((FD_FIX2INT(margin_arg))>=0))
    margin = FD_FIX2INT(margin_arg);
  else return fd_type_error("positive fixnum","write_png_packet",margin_arg);
  /* Start building the PNG, using setjmp */
  if (setjmp(png_jmpbuf(png_ptr))) {
    png_destroy_write_struct(&png_ptr, &info_ptr);
    return fd_err("PNG problem","write_png_packet",NULL,FD_VOID);}
  else {
    struct FD_OUTBUF buf = { 0 };
    int qrwidth = qrcode->width, qrheight = qrwidth;
    int fullwidth = (qrwidth+(margin*2))*dotsize;
    int rowlen = (fullwidth+7)/8;
    unsigned char *row = u8_malloc(rowlen);
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
    {int i = 0; while (i<(margin*dotsize)) {
        png_write_row(png_ptr,row); i++;}}

    /* Write the content */
    {int vscan = 0;
      unsigned char *read = qrcode->data;
      while (vscan<qrheight) {
        char *write = row+((margin*dotsize)/8); /* margin offset */
        int bitoff = 7-((margin*dotsize)%8), dotscan = 0;
        int hscan = 0;
        memset(row,0xFF,(fullwidth+7)/8);
        while (hscan<qrwidth) {
          unsigned char qrdot = *read++;
          dotscan = 0; while (dotscan<dotsize) {
            *write = (*write)^((qrdot&0x01)<<bitoff);
            bitoff--; if (bitoff<0) {write++; bitoff = 7;}
            dotscan++;}
          hscan++;}
        dotscan = 0; while (dotscan<dotsize) {
          png_write_row(png_ptr, row); dotscan++;}
        vscan++;}}

    /* Write bottom margin */
    memset(row,0xFF,rowlen);
    {int i = 0; while (i<(margin*dotsize)) {
        png_write_row(png_ptr,row); i++;}}

    png_write_end(png_ptr, NULL);
    png_destroy_write_struct(&png_ptr, &info_ptr);

    u8_free(row);

    if ( (BUFIO_ALLOC(&buf)) == FD_HEAP_BUFFER )
      return fd_init_packet(NULL,buf.bufwrite-buf.buffer,buf.buffer);
    else {
      lispval packet = fd_make_packet(NULL,buf.bufwrite-buf.buffer,buf.buffer);
      fd_close_outbuf(&buf);
      return packet;}}
}

static lispval qrencode_prim(lispval string,lispval opts)
{
  lispval level_arg = fd_getopt(opts,robustness_symbol,FD_FALSE);
  lispval version_arg = fd_getopt(opts,version_symbol,FD_INT(0));
  if (!(FD_UINTP(version_arg))) {
    fd_decref(level_arg);
    return fd_type_error("uint","qrencode_prim",version_arg);}
  QRecLevel eclevel = geteclevel(level_arg);
  {
    lispval result;
    QRcode *qrcode=
      ((u8_lock_mutex(&qrencode_lock)),
       QRcode_encodeString8bit
       (FD_CSTRING(string),FD_FIX2INT(version_arg),eclevel));
    u8_unlock_mutex(&qrencode_lock);
    result = write_png_packet(qrcode,opts);
    QRcode_free(qrcode);
    fd_decref(level_arg);
    return result;
  }
}

/* Initialization */

static long long int qrencode_init = 0;

FD_EXPORT int fd_init_qrcode()
{
  lispval module;
  if (qrencode_init) return 0;
  module = fd_new_cmodule("QRCODE",0,fd_init_qrcode);
  l_sym = fd_intern("L");
  m_sym = fd_intern("M");
  q_sym = fd_intern("Q");
  h_sym = fd_intern("H");
  dotsize_symbol = fd_intern("DOTSIZE");
  margin_symbol = fd_intern("MARGIN");
  version_symbol = fd_intern("VERSION");
  robustness_symbol = fd_intern("ROBUSTNESS");


  fd_defn(module,
          fd_make_cprim2x("QRENCODE",qrencode_prim,1,
                          fd_string_type,FD_VOID,-1,FD_VOID));
  qrencode_init = u8_millitime();

  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
