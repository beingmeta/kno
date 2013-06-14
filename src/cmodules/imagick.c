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

#include <wand/MagickWand.h>
#include <wand/pixel-wand.h>
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

/* Data for conversions */

static struct CTYPEMAP {
  CompressionType ct;
  char *cname; } compression_types[]={
  {NoCompression,"None"},
  {BZipCompression,"BZip"},
  {DXT1Compression,"DXT1"},
  {DXT3Compression,"DXT3"},
  {DXT5Compression,"DXT5"},
  {FaxCompression,"Fax"},
  {Group4Compression,"Group4"},
  {JPEGCompression,"JPEG"},
  {JPEG2000Compression,"JPEG2000"}, /* ISO/IEC std 15444-1 */
  {LosslessJPEGCompression,"LosslessJPEG"},
  {LZWCompression,"LZW"},
  {RLECompression,"RLE"},
  {ZipCompression,"Zip"},
  {ZipSCompression,"ZipS"},
  {PizCompression,"Piz"},
  {Pxr24Compression,"Pxr24"},
  {B44Compression,"B44"},
  {B44ACompression,"B44A"},
  {LZMACompression,"LZMA"},             /* Lempel-Ziv-Markov chain algorithm */
  {JBIG1Compression,"JBIG1"},           /* ISO/IEC std 11544 / ITU-T rec T.82 */
  {JBIG2Compression,"JBIG2"},           /* ISO/IEC std 14492 / ITU-T rec T.88 */
  {UndefinedCompression,"Undefined"}};

static struct CSMAP {
  ColorspaceType cs;
  char *csname;} csmap[]={
  {RGBColorspace,"RGB"},
  {GRAYColorspace,"GRAY"},
  {TransparentColorspace,"Transparent"},
  {OHTAColorspace,"OHTA"},
  {LabColorspace,"Lab"},
  {XYZColorspace,"XYZ"},
  {YCbCrColorspace,"YCbCr"},
  {YCCColorspace,"YCC"},
  {YIQColorspace,"YIQ"},
  {YPbPrColorspace,"YPbPr"},
  {YUVColorspace,"YUV"},
  {CMYKColorspace,"CMYK"},
  {sRGBColorspace,"sRGB"},
  {HSBColorspace,"HSB"},
  {HSLColorspace,"HSL"},
  {HWBColorspace,"HWB"},
  {Rec601LumaColorspace,"Rec601Luma"},
  {Rec601YCbCrColorspace,"Rec601YCbCr"},
  {Rec709LumaColorspace,"Rec709Luma"},
  {Rec709YCbCrColorspace,"Rec709YCbCr"},
  {LogColorspace,"Log"},
  {CMYColorspace,"CMY"},
  /*
  {LuvColorspace,"Luv"},
  {HCLColorspace,"HCL"},
  {LCHColorspace,"LCH"},
  {LMSColorspace,"LMS"},
  {LCHabColorspace,"LCHab"},
  {LCHuvColorspace,"LCHuv"},
  {scRGBColorspace,"scRGB"},
  */
  {UndefinedColorspace,"Undefined"}};


char *ctype2string(CompressionType ct)
{
  struct CTYPEMAP *scan=compression_types;
  while (scan->ct!=UndefinedCompression)
    if (ct==scan->ct) return scan->cname;
    else scan++;
  return NULL;
}

char *cspace2string(ColorspaceType cs)
{
  struct CSMAP *scan=csmap;
  while (scan->cs!=UndefinedColorspace)
    if (cs==scan->cs) return scan->csname;
    else scan++;
  return NULL;
}


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
  MagickClearException(wand);
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
  FD_INIT_FRESH_CONS(fdwand,fd_imagick_type);
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
  FD_INIT_FRESH_CONS(fdwand,fd_imagick_type);
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

fdtype imagick2imagick(fdtype fdwand)
{
  unsigned char *data=NULL; size_t n_bytes;
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  struct FD_IMAGICK *fresh=u8_alloc(struct FD_IMAGICK);
  MagickWand *wand=CloneMagickWand(wrapper->wand);
  FD_INIT_FRESH_CONS(fresh,fd_imagick_type);
  fresh->wand=wand;
  return (fdtype)fresh;
}

/* Getting properties */

static fdtype format, resolution, size, width, height;

static fdtype imagick_table_get(fdtype fdwand,fdtype field,fdtype dflt)
{
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  enum result_type {imbool,imint,imdouble,imsize,imbox,imtrans} rt;
  if (FD_EQ(field,format)) {
    const char *fmt=MagickGetImageFormat(wand);
    return fdtype_string((char *)fmt);}
  else if (FD_EQ(field,resolution)) {
    double x=0, y=0;
    MagickBooleanType rv=MagickGetResolution(wand,&x,&y);
    return fd_init_pair(NULL,fd_make_double(x),fd_make_double(y));}
  else if (FD_EQ(field,size)) {
    size_t w=MagickGetImageWidth(wand);
    size_t h=MagickGetImageHeight(wand);
    return fd_init_pair(NULL,FD_INT2DTYPE(w),FD_INT2DTYPE(h));}
  else if (FD_EQ(field,width)) {
    size_t w=MagickGetImageWidth(wand);
    return FD_INT2DTYPE(w);}
  else if (FD_EQ(field,height)) {
    size_t h=MagickGetImageHeight(wand);
    return FD_INT2DTYPE(h);}
  else return FD_VOID;
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

static fdtype imagick_charcoal(fdtype fdwand,fdtype radius,fdtype sigma)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  double r=FD_FLONUM(radius), s=FD_FLONUM(sigma);
  retval=MagickCharcoalImage(wand,r,s);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_charcoal",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_emboss(fdtype fdwand,fdtype radius,fdtype sigma)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  double r=FD_FLONUM(radius), s=FD_FLONUM(sigma);
  retval=MagickEmbossImage(wand,r,s);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_emboss",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_blur(fdtype fdwand,fdtype radius,fdtype sigma)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  double r=FD_FLONUM(radius), s=FD_FLONUM(sigma);
  retval=MagickGaussianBlurImage(wand,r,s);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_blur",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_edge(fdtype fdwand,fdtype radius)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  double r=FD_FLONUM(radius);
  retval=MagickEdgeImage(wand,r);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_edge",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}


static fdtype imagick_crop(fdtype fdwand,fdtype width,fdtype height,fdtype xoff,fdtype yoff)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  size_t w=fd_getint(width), h=fd_getint(height);
  ssize_t x=fd_getint(xoff), y=fd_getint(yoff);
  retval=MagickCropImage(wand,w,h,x,y);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_crop",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_flip(fdtype fdwand)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  retval=MagickFlipImage(wand);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_flip",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_flop(fdtype fdwand)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  retval=MagickFlopImage(wand);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_flip",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_equalize(fdtype fdwand)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  retval=MagickEqualizeImage(wand);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_equalize",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_despeckle(fdtype fdwand)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  retval=MagickDespeckleImage(wand);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_despeckle",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_enhance(fdtype fdwand)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  retval=MagickEnhanceImage(wand);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_enhance",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_deskew(fdtype fdwand,fdtype threshold)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  double t=FD_FLONUM(threshold);
  retval=MagickEdgeImage(wand,t);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_deskew",wand);
    return FD_ERROR_VALUE;}
  else return fd_incref(fdwand);
}

static fdtype imagick_display(fdtype fdwand,fdtype display_name)
{
  MagickBooleanType retval;
  struct FD_IMAGICK *wrapper=
    FD_GET_CONS(fdwand,fd_imagick_type,struct FD_IMAGICK *);
  MagickWand *wand=wrapper->wand;
  u8_string display=
    ((FD_VOIDP(display_name))?((u8_string)":0.0"):(FD_STRDATA(display_name)));
  retval=MagickDisplayImage(wand,display);
  if (retval==MagickFalse) {
    grabmagickerr("imagick_deskew",wand);
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

static void init_symbols()
{
  format=fd_intern("FORMAT");
  resolution=fd_intern("RESOLUTION");
  
  size=fd_intern("SIZE");
  width=fd_intern("WIDTH");
  height=fd_intern("HEIGHT");
}

int fd_init_imagick()
{
  fdtype imagick_module;
  if (imagick_init) return 0;
  else imagick_init=1;
  imagick_module=fd_new_module("IMAGICK",(FD_MODULE_SAFE));

  fd_imagick_type=fd_register_cons_type("IMAGICK");
  fd_unparsers[fd_imagick_type]=unparse_imagick;
  fd_recyclers[fd_imagick_type]=recycle_imagick;

  init_symbols();

  fd_tablefns[fd_imagick_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_imagick_type]->get=(fd_table_get_fn)imagick_table_get;
  fd_tablefns[fd_imagick_type]->add=NULL;
  fd_tablefns[fd_imagick_type]->drop=NULL;
  fd_tablefns[fd_imagick_type]->store=NULL;
  fd_tablefns[fd_imagick_type]->test=NULL;
  fd_tablefns[fd_imagick_type]->getsize=NULL;
  fd_tablefns[fd_imagick_type]->keys=NULL;

  fd_idefn(imagick_module,
	   fd_make_cprim1x("FILE->IMAGICK",file2imagick,1,
			   fd_string_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("PACKET->IMAGICK",packet2imagick,1,
			   fd_packet_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim2x("IMAGICK->FILE",imagick2file,1,
			   fd_imagick_type,FD_VOID,
			   fd_string_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK->PACKET",imagick2packet,1,
			   fd_imagick_type,FD_VOID));

  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK/CLONE",imagick2imagick,1,
			   fd_imagick_type,FD_VOID));

  fd_idefn(imagick_module,
	   fd_make_cprim3x("IMAGICK/FIT",imagick_fit,3,
			   fd_imagick_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   fd_fixnum_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim3x("IMAGICK/CHARCOAL",imagick_charcoal,3,
			   fd_imagick_type,FD_VOID,
			   fd_double_type,FD_VOID,
			   fd_double_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim3x("IMAGICK/EMBOSS",imagick_emboss,3,
			   fd_imagick_type,FD_VOID,
			   fd_double_type,FD_VOID,
			   fd_double_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim3x("IMAGICK/BLUR",imagick_blur,3,
			   fd_imagick_type,FD_VOID,
			   fd_double_type,FD_VOID,
			   fd_double_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim2x("IMAGICK/EDGE",imagick_edge,2,
			   fd_imagick_type,FD_VOID,
			   fd_double_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim5x("IMAGICK/CROP",imagick_crop,3,
			   fd_imagick_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(0),
			   fd_fixnum_type,FD_INT2DTYPE(0)));
  fd_idefn(imagick_module,
	   fd_make_cprim2x("IMAGICK/DESKEW",imagick_deskew,2,
			   fd_imagick_type,FD_VOID,
			   fd_double_type,FD_VOID));

  fd_idefn(imagick_module,
	   fd_make_cprim2x("IMAGICK/DISPLAY",imagick_display,1,
			   fd_imagick_type,FD_VOID,
			   fd_string_type,FD_VOID));

  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK/FLIP",imagick_flip,1,
			   fd_imagick_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK/FLOP",imagick_flop,1,
			   fd_imagick_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK/EQUALIZE",imagick_equalize,1,
			   fd_imagick_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK/DESPECKLE",imagick_despeckle,1,
			   fd_imagick_type,FD_VOID));
  fd_idefn(imagick_module,
	   fd_make_cprim1x("IMAGICK/ENHANCE",imagick_enhance,1,
			   fd_imagick_type,FD_VOID));
  
  MagickWandGenesis();
  atexit(magickwand_atexit);

  return 1;

}
