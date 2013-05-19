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
#include <math.h>

u8_condition MagickWandError="ImageMagicWand error";
fd_ptr_type fd_imagick_type=0;

FD_EXPORT int fd_init_imagick(void) FD_LIBINIT_FN;

typedef struct FD_IMAGICK {
  FD_CONS_HEADER;
  MagickWand *wand;} FD_IMAGICK;
typedef struct FD_IMAGICK *fd_imagick;

void magickwand_atexit()
{
  MagickWandTerminus();
}

void grabmagickerr(u8_context cxt,MagickWand *wand)
{
  ExceptionType severity;
  char *description=MagickGetException(wand,&severity);
  u8_seterr(MagickWandError,cxt,u8_strdup(description));
  MagickRelinquishMemory(description);
}

static int unparse_imagick(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_IMAGICK *wrapper=(struct FD_IMAGICK *)x;
  u8_printf(out,"#<IMAGICK %lx>",((unsigned long)(wrapper->wand)));
  return 1;
}

static void recycle_imagick(struct FD_CONS *c)
{
  struct FD_IMAGICK *wrapper=(struct FD_IMAGICK *)c;
  DestroyMagickWand(wrapper->wand);
  u8_free(c);
}

fdtype file2imagick(fdtype arg)
{
  MagickWand *wand;
  MagickBooleanType retval;
  struct FD_IMAGICK *fdwand=u8_alloc(struct FD_IMAGICK);
  FD_INIT_CONS(fdwand,fd_imagick_type);
  fdwand->wand=wand=NewMagickWand();
  retval=MagickReadImage(wand,FD_STRDATA(arg));
  if (retval==MagickFalse) {
    grabmagickerr("file2imagick",wand);
    u8_free(wand); u8_free(fdwand);
    return FD_ERROR_VALUE;}
  else return (fdtype)fdwand;
}

fdtype packet2imagick(fdtype arg)
{
  MagickWand *wand;
  MagickBooleanType retval;
  struct FD_IMAGICK *fdwand=u8_alloc(struct FD_IMAGICK);
  FD_INIT_CONS(fdwand,fd_imagick_type);
  fdwand->wand=wand=NewMagickWand();
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
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  retval=MagickWriteImage(wand,FD_STRDATA(filename));
  if (retval==MagickFalse) {
    grabmagickerr("imagick2file",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

fdtype imagick2packet(fdtype fdwand)
{
  unsigned char *data=NULL; size_t n_bytes;
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  MagickResetIterator(wand);
  data=MagickGetImageBlob(wand,&n_bytes);
  if (data==NULL) {
    grabmagickerr("imagick2packet",wand);
    return FD_ERROR_VALUE;}
  else {
    fdtype packet=fd_make_packet(NULL,n_bytes,data);
    MagickRelinquishMemory(data);
    return packet;}
}

static fdtype imagick_getformat(fdtype fdwand)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  char *fmt=MagickGetImageFormat(wand);
  if (fmt) return fd_make_string(NULL,-1,fmt);
  else {
    grabmagickerr("imagick_getformat",wand);
    return FD_ERROR_VALUE;}
}

static fdtype imagick_setformat(fdtype fdwand,fdtype format)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  retval=MagickSetImageFormat(wand,FD_STRDATA(format));
  if (retval==MagickFalse) {
    grabmagickerr("imagick_setformat",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_getwidth(fdtype fdwand)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  size_t iwidth=MagickGetImageWidth(wand);
  return FD_INT2DTYPE(iwidth);
}

static fdtype imagick_getheight(fdtype fdwand)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  size_t iheight=MagickGetImageHeight(wand);
  return FD_INT2DTYPE(iheight);
}

static fdtype imagick_fit(fdtype fdwand,fdtype w_arg,fdtype h_arg)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  int width=FD_FIX2INT(w_arg), height=FD_FIX2INT(h_arg);
  size_t iwidth=MagickGetImageWidth(wand);
  size_t iheight=MagickGetImageHeight(wand);
  size_t target_width, target_height;
  double xscale=((double)width)/((double)iwidth);
  double yscale=((double)height)/((double)iheight);
  double scale=((xscale<yscale)?(xscale):(yscale));
  target_width=(int)floor(iwidth*scale);
  target_height=(int)floor(iheight*scale);
  retval=MagickAdaptiveResizeImage(wand,target_width,target_height);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_fit",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_get(fdtype fdwand,fdtype property,fdtype dflt)
{
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  char *pname=((FD_SYMBOLP(property))?(FD_SYMBOL_NAME(property)):
	       (FD_STRINGP(property))?(FD_STRDATA(property)):(NULL));
  char *value=MagickGetImageProperty(wand,pname);
  if (value) {
    fdtype stringval=fd_make_string(NULL,-1,value);
    MagickRelinquishMemory(value);
    return stringval;}
  else return fd_incref(dflt);;
}

static fdtype imagick_getkeys(fdtype fdwand)
{
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  size_t n_keys=0;
  char **properties=MagickGetImageProperties(wand,"",&n_keys);
  if (properties) {
    fdtype results=FD_EMPTY_CHOICE;
    int i=0; while (i<n_keys) {
      char *pname=properties[i++];
      fdtype key=fd_make_string(NULL,-1,pname);
      FD_ADD_TO_CHOICE(results,key);}
    MagickRelinquishMemory(properties);
    return results;}
  else return FD_VOID;
}

static int imagick_init=0;

int fd_init_imagick()
{
  fdtype imagick_module;
  if (imagick_init) return 0;
  else imagick_init=1;
  imagick_module=fd_new_module("IMAGICK",(FD_MODULE_SAFE));

  fd_imagick_type=fd_register_cons_type("IMAGICK");
  fd_unparsers[fd_imagick_type]=unparse_imagick;
  fd_recyclers[fd_imagick_type]=recycle_imagick;

  fd_tablefns[fd_imagick_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_imagick_type]->get=(fd_table_get_fn)imagick_get;
  fd_tablefns[fd_imagick_type]->add=NULL;
  fd_tablefns[fd_imagick_type]->drop=NULL;
  fd_tablefns[fd_imagick_type]->store=NULL;
  fd_tablefns[fd_imagick_type]->test=NULL;
  fd_tablefns[fd_imagick_type]->getsize=NULL;
  fd_tablefns[fd_imagick_type]->keys=imagick_getkeys;

  fd_idefn(imagick_module,
	   fd_make_cprim1x("FILE->IMAGICK",file2imagick,1,fd_string_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("PACKET->IMAGICK",packet2imagick,1,fd_packet_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim2x("IMAGICK->FILE",imagick2file,1,
			   fd_imagick_type,FD_VOID,
			   fd_string_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK->PACKET",imagick2packet,1,
			   fd_imagick_type,FD_VOID));


  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK/FORMAT",imagick_getformat,1,
			   fd_imagick_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim2x("IMAGICK/SET-FORMAT!",imagick_setformat,2,
			   fd_imagick_type,FD_VOID,fd_string_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK/HEIGHT",imagick_getheight,1,
			   fd_imagick_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK/WIDTH",imagick_getwidth,1,
			   fd_imagick_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim3x("IMAGICK/FIT",imagick_fit,3,
			   fd_imagick_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   fd_fixnum_type,FD_VOID));

  
  MagickWandGenesis();
  atexit(magickwand_atexit);

}
