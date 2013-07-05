/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* imagemagick.c
   This implements FramerD bindings to the MagickWand API
   Copyright (C) 2012-2013 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>
#include <libu8/u8pathfns.h>

#include <wand/magick_wand.h>
#include <limits.h>
#include <ctype.h>

u8_condition ImageMagickError="ImageMagic error";
FD_EXPORT u8_condition fd_ImageMagickError=ImageMagickError;
fd_ptr_type fd_imagick_type=0;

FD_EXPORT int fd_init_magickwand(void) FD_LIBINIT_FN;

typedef struct FD_IMAGICK {
  FD_CONS_HEADER;
  MagickWand *wand;} FD_IMAGICK;
typedef struct FD_IMAGICK *fd_imagick;

void magickwand_atexit()
{
  MagickWandTerminus();
}

void getmagickerr(u8_context cxt,MagickWand *wand)
{
  ExceptionType severity;
  char *description=MagickGetException(wand,&severity);
  u8_seterr(MagickWandError,cxt,u8_strdup(description));
  MagickRelinquishMemory(description);
}

fdtype file2imagick(fdtype arg)
{
  MagicWand *wand;
  MagickBooleanType retval;
  struct FD_IMAGICK *fdwand=u8_alloc(struct FD_IMAGICK);
  FD_INIT_CONS(fdwand,fd_imagick_type);
  fdwand->wand=wand=NewMagicWand();
  retval=MagickReadImage(wand,FD_STRDATA(arg));
  if (retval==MagickFalse) {
    grabmagickerr("file2imagick",wand);
    u8_free(wand); u8_free(fdwand);
    return FD_ERROR_VALUE;}
  else return (fdtype)fdwand;
}

fdtype packet2imagick(fdtype arg)
{
  MagicWand *wand;
  MagickBooleanType retval;
  struct FD_IMAGICK *fdwand=u8_alloc(struct FD_IMAGICK);
  FD_INIT_CONS(fdwand,fd_imagick_type);
  fdwand->wand=wand=NewMagicWand();
  retval=MagickReadImageBlob
    (fdwand->wand,FD_PACKET_DATA(arg),FD_PACKET_LENGTH(arg));
  if (retval==MagickFalse) {
    grabmagickerr("file2imagick",wand);
    u8_free(wand); u8_free(fdwand);
    return FD_ERROR_VALUE;}
  else return (fdtype)fdwand;
}

fdtype imagick2file(fdtype fdwand,fdtype filename)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *fdwand=FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=fdwand->wand;
  retval=MagickReadImage(wand,FD_STRDATA(filename));
  if (retval==MagickFalse) {
    grabmagickerr("imagick2file",wand);
    u8_free(wand); u8_free(fdwand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

fdtype imagick2packet(fdtype fdwand,fdtype filename)
{
  unsigned char *data=NULL; size_t n_bytes;
  MagickBooleanType retval;
  struct FD_IMAGICK *fdwand=FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=fdwand->wand;
  MagickResetIterator(wand);
  data=MagickGetImageBlob(wand,&n_bytes);
  if (data==NULL) {
    grabmagickerr("imagick2packet",wand);
    u8_free(wand); u8_free(fdwand);
    return FD_ERROR_VALUE;}
  else {
    fdtype packet=fd_make_packet(NULL,n_bytes,data);
    MagickRelinquishMemory(data);
    return packet;}
}

static int imagick_init=0;

int fd_init_magickwand()
{
  fdtype imagick_module;
  if (imagick_init) return 0;
  else imagick_init=1;
  imagick_module=fd_new_module("IMAGICK",(FD_MODULE_SAFE));


  MagickWandGenesis();
  atexit(magickwand_atexit);
  fd_imagick_type=fd_get_ptrtype();
  
}
