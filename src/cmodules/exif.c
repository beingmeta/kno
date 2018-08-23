/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/eval.h"

#include <libu8/libu8io.h>

#include <libexif/exif-utils.h>
#include <libexif/exif-data.h>
#include <libexif/exif-tag.h>

FD_EXPORT int fd_init_exif(void) FD_LIBINIT_FN;

static lispval exif2lisp(ExifEntry *exentry)
{
  switch (exentry->format) {
  case EXIF_FORMAT_ASCII:
    if (exentry->size<8) return lispval_string(exentry->data);
    else if (memcmp(exentry->data,"ASCII\0\0\0",8)==0)
      return lispval_string(exentry->data+8);
    else if (memcmp(exentry->data,"\0\0\0\0\0\0\0\0",8)==0)
      return fd_make_packet(NULL,exentry->size-8,exentry->data+8);
    else return lispval_string(exentry->data);
  case EXIF_FORMAT_BYTE: case EXIF_FORMAT_SBYTE: {
    int n = exentry->components, i = 0;
    lispval *lispdata = u8_alloc_n(n,lispval);
    /* ExifByteOrder o = exif_data_get_byte_order (exentry->parent->parent); */
    /* int item_size = exif_format_get_size(exentry->format); */
    unsigned char *exifdata = exentry->data;
    if (exentry->format == EXIF_FORMAT_SBYTE)
      while (i < n) {
        char ival = exifdata[i];
        lispdata[i]=FD_SHORT2DTYPE(ival);
        i++;}
    else while (i < n) {
      unsigned char ival = exifdata[i];
      lispdata[i]=FD_USHORT2DTYPE(ival);
      i++;}
    if (n==1) {
      lispval retval = lispdata[0]; u8_free(lispdata);
      return retval;}
    else return fd_wrap_vector(n,lispdata);}
  case EXIF_FORMAT_SHORT: case EXIF_FORMAT_SSHORT: {
    int n = exentry->components, i = 0;
    lispval *lispdata = u8_alloc_n(n,lispval);
    ExifByteOrder o = exif_data_get_byte_order (exentry->parent->parent);
    int item_size = exif_format_get_size(exentry->format);
    unsigned char *exifdata = exentry->data;
    if (exentry->format == EXIF_FORMAT_SSHORT)
      while (i < n) {
        short ival = exif_get_short(exifdata+(i*item_size),o);
        lispdata[i]=FD_SHORT2DTYPE(ival);
        i++;}
    else while (i < n) {
      unsigned short ival = exif_get_short(exifdata+(i*item_size),o);
      lispdata[i]=FD_USHORT2DTYPE(ival);
      i++;}
    if (n==1) {
      lispval retval = lispdata[0]; u8_free(lispdata);
      return retval;}
    else return fd_wrap_vector(n,lispdata);}
  case EXIF_FORMAT_LONG: case EXIF_FORMAT_SLONG: {
    int n = exentry->components, i = 0;
    lispval *lispdata = u8_alloc_n(n,lispval);
    ExifByteOrder o = exif_data_get_byte_order (exentry->parent->parent);
    int item_size = exif_format_get_size(exentry->format);
    unsigned char *exifdata = exentry->data;
    if (exentry->format == EXIF_FORMAT_SLONG)
      while (i < n) {
        long int ival = exif_get_long(exifdata+(i*item_size),o);
        lispdata[i]=FD_INT(ival);
        i++;}
    else while (i < n) {
      unsigned long int ival = exif_get_long(exifdata+(i*item_size),o);
      lispdata[i]=FD_INT(ival);
      i++;}
    if (n==1) {
      lispval retval = lispdata[0]; u8_free(lispdata);
      return retval;}
    else return fd_wrap_vector(n,lispdata);}
  case EXIF_FORMAT_RATIONAL: case EXIF_FORMAT_SRATIONAL: {
    int n = exentry->components, i = 0;
    lispval *lispdata = u8_alloc_n(n,lispval);
    ExifByteOrder o = exif_data_get_byte_order (exentry->parent->parent);
    int item_size = exif_format_get_size(exentry->format);
    unsigned char *exifdata = exentry->data;
    if (exentry->format == EXIF_FORMAT_SRATIONAL)
      while (i < n) {
        ExifSRational v = exif_get_srational(exifdata+(i*item_size),o);
        double ratio = ((double)(v.numerator))/((double)(v.denominator));
        lispdata[i]=fd_init_double(NULL,ratio);
        i++;}
    else while (i < n) {
      ExifRational v = exif_get_rational(exifdata+(i*item_size),o);
        double ratio = ((double)(v.numerator))/((double)(v.denominator));
        lispdata[i]=fd_init_double(NULL,ratio);
        i++;}
    if (n==1) {
      lispval retval = lispdata[0]; int i = 1;
      while (i<n) {fd_decref(lispdata[i]); i++;}
      u8_free(lispdata);
      return retval;}
    else return fd_wrap_vector(n,lispdata);}
  default: return FD_EMPTY_CHOICE;
  }
}

struct FD_HASHTABLE exif_tagmap;

static struct TAGINFO {
  int tagid; char *tagname; lispval tagsym;} taginfo[]= {
    /* {EXIF_TAG_NEW_SUBFILE_TYPE, "NewSubfileType",FD_VOID}, */
    {EXIF_TAG_INTEROPERABILITY_INDEX, "InteroperabilityIndex",FD_VOID},
    {EXIF_TAG_INTEROPERABILITY_VERSION, "InteroperabilityVersion",FD_VOID},
    {EXIF_TAG_IMAGE_WIDTH, "ImageWidth",FD_VOID},
    {EXIF_TAG_IMAGE_LENGTH, "ImageLength",FD_VOID},
    {EXIF_TAG_FILL_ORDER, "FillOrder",FD_VOID},
    {EXIF_TAG_DOCUMENT_NAME, "DocumentName",FD_VOID},
    {EXIF_TAG_IMAGE_DESCRIPTION, "ImageDescription",FD_VOID},
    {EXIF_TAG_MAKE, "Make",FD_VOID},
    {EXIF_TAG_MODEL, "Model",FD_VOID},
    {EXIF_TAG_STRIP_OFFSETS, "StripOffsets",FD_VOID},
    {EXIF_TAG_ORIENTATION, "Orientation",FD_VOID},
    {EXIF_TAG_SAMPLES_PER_PIXEL, "SamplesPerPixel",FD_VOID},
    {EXIF_TAG_ROWS_PER_STRIP, "RowsPerStrip",FD_VOID},
    {EXIF_TAG_STRIP_BYTE_COUNTS, "StripByteCounts",FD_VOID},
    {EXIF_TAG_X_RESOLUTION, "XResolution",FD_VOID},
    {EXIF_TAG_Y_RESOLUTION, "YResolution",FD_VOID},
    {EXIF_TAG_PLANAR_CONFIGURATION, "PlanarConfiguration",FD_VOID},
    {EXIF_TAG_RESOLUTION_UNIT, "ResolutionUnit",FD_VOID},
    {EXIF_TAG_TRANSFER_FUNCTION, "TransferFunction",FD_VOID},
    {EXIF_TAG_SOFTWARE, "Software",FD_VOID},
    {EXIF_TAG_DATE_TIME, "DateTime",FD_VOID},
    {EXIF_TAG_ARTIST, "Artist",FD_VOID},
    {EXIF_TAG_WHITE_POINT, "WhitePoint",FD_VOID},
    {EXIF_TAG_PRIMARY_CHROMATICITIES, "PrimaryChromaticities",FD_VOID},
    {EXIF_TAG_TRANSFER_RANGE, "TransferRange",FD_VOID},
    /* {EXIF_TAG_SUB_IFDS, "SubIFDs",FD_VOID}, */
    {EXIF_TAG_JPEG_PROC, "JPEGProc",FD_VOID},
    {EXIF_TAG_JPEG_INTERCHANGE_FORMAT, "JPEGInterchangeFormat",FD_VOID},
    {EXIF_TAG_JPEG_INTERCHANGE_FORMAT_LENGTH,
     "JPEGInterchangeFormatLength",FD_VOID},
    {EXIF_TAG_YCBCR_COEFFICIENTS, "YCbCrCoefficients",FD_VOID},
    {EXIF_TAG_YCBCR_SUB_SAMPLING, "YCbCrSubSampling",FD_VOID},
    {EXIF_TAG_YCBCR_POSITIONING, "YCbCrPositioning",FD_VOID},
    {EXIF_TAG_REFERENCE_BLACK_WHITE, "ReferenceBlackWhite",FD_VOID},
    /* {EXIF_TAG_XML_PACKET, "XMLPacket",FD_VOID}, */
    {EXIF_TAG_RELATED_IMAGE_FILE_FORMAT, "RelatedImageFileFormat",FD_VOID},
    {EXIF_TAG_RELATED_IMAGE_WIDTH, "RelatedImageWidth",FD_VOID},
    {EXIF_TAG_RELATED_IMAGE_LENGTH, "RelatedImageLength",FD_VOID},
    {EXIF_TAG_CFA_REPEAT_PATTERN_DIM, "CFARepeatPatternDim",FD_VOID},
    {EXIF_TAG_CFA_PATTERN, "CFAPattern",FD_VOID},
    {EXIF_TAG_BATTERY_LEVEL, "BatteryLevel",FD_VOID},
    {EXIF_TAG_COPYRIGHT, "Copyright",FD_VOID},
    {EXIF_TAG_EXPOSURE_TIME, "ExposureTime",FD_VOID},
    {EXIF_TAG_FNUMBER, "FNumber",FD_VOID},
    {EXIF_TAG_IPTC_NAA, "IPTC/NAA",FD_VOID},
    /* {EXIF_TAG_IMAGE_RESOURCES, "ImageResources",FD_VOID}, */
    {EXIF_TAG_EXIF_IFD_POINTER, "ExifIFDPointer",FD_VOID},
    {EXIF_TAG_INTER_COLOR_PROFILE, "InterColorProfile",FD_VOID},
    {EXIF_TAG_EXPOSURE_PROGRAM, "ExposureProgram",FD_VOID},
    {EXIF_TAG_SPECTRAL_SENSITIVITY, "SpectralSensitivity",FD_VOID},
    {EXIF_TAG_GPS_INFO_IFD_POINTER, "GPSInfoIFDPointer",FD_VOID},
#if 0
    {EXIF_TAG_GPS_LATITUDE_REF, "GPSLatitudeRef",FD_VOID},
    {EXIF_TAG_GPS_LATITUDE, "GPSLatitude",FD_VOID},
    {EXIF_TAG_GPS_LONGITUDE_REF, "GPSLongitudeRef",FD_VOID},
    {EXIF_TAG_GPS_LONGITUDE, "GPSLongitude",FD_VOID},
#endif
    {EXIF_TAG_ISO_SPEED_RATINGS, "ISOSpeedRatings",FD_VOID},
    {EXIF_TAG_EXIF_VERSION, "ExifVersion",FD_VOID},
    {EXIF_TAG_DATE_TIME_ORIGINAL, "DateTimeOriginal",FD_VOID},
    {EXIF_TAG_DATE_TIME_DIGITIZED, "DateTimeDigitized",FD_VOID},
    {EXIF_TAG_COMPONENTS_CONFIGURATION, "ComponentsConfiguration",FD_VOID},
    {EXIF_TAG_COMPRESSED_BITS_PER_PIXEL, "CompressedBitsPerPixel",FD_VOID},
    {EXIF_TAG_SHUTTER_SPEED_VALUE, "ShutterSpeedValue",FD_VOID},
    {EXIF_TAG_APERTURE_VALUE, "ApertureValue",FD_VOID},
    {EXIF_TAG_BRIGHTNESS_VALUE, "BrightnessValue",FD_VOID},
    {EXIF_TAG_EXPOSURE_BIAS_VALUE, "ExposureBiasValue",FD_VOID},
    {EXIF_TAG_MAX_APERTURE_VALUE, "MaxApertureValue",FD_VOID},
    {EXIF_TAG_SUBJECT_DISTANCE, "SubjectDistance",FD_VOID},
    {EXIF_TAG_METERING_MODE, "MeteringMode",FD_VOID},
    {EXIF_TAG_LIGHT_SOURCE, "LightSource",FD_VOID},
    {EXIF_TAG_FLASH, "Flash",FD_VOID},
    {EXIF_TAG_FOCAL_LENGTH, "FocalLength",FD_VOID},
    {EXIF_TAG_MAKER_NOTE, "MakerNote",FD_VOID},
    {EXIF_TAG_USER_COMMENT, "UserComment",FD_VOID},
    {EXIF_TAG_SUB_SEC_TIME, "SubsecTime",FD_VOID},
    {EXIF_TAG_SUB_SEC_TIME_ORIGINAL, "SubSecTimeOriginal",FD_VOID},
    {EXIF_TAG_SUB_SEC_TIME_DIGITIZED, "SubSecTimeDigitized",FD_VOID},
    {EXIF_TAG_FLASH_PIX_VERSION, "FlashPixVersion",FD_VOID},
    {EXIF_TAG_COLOR_SPACE, "ColorSpace",FD_VOID},
    {EXIF_TAG_PIXEL_X_DIMENSION, "PixelXDimension",FD_VOID},
    {EXIF_TAG_PIXEL_Y_DIMENSION, "PixelYDimension",FD_VOID},
    {EXIF_TAG_RELATED_SOUND_FILE, "RelatedSoundFile",FD_VOID},
    {EXIF_TAG_INTEROPERABILITY_IFD_POINTER, "InteroperabilityIFDPointer",FD_VOID},
    {EXIF_TAG_FLASH_ENERGY, "FlashEnergy",FD_VOID},
    {EXIF_TAG_SPATIAL_FREQUENCY_RESPONSE, "SpatialFrequencyResponse",FD_VOID},
    {EXIF_TAG_FOCAL_PLANE_X_RESOLUTION, "FocalPlaneXResolution",FD_VOID},
    {EXIF_TAG_FOCAL_PLANE_Y_RESOLUTION, "FocalPlaneYResolution",FD_VOID},
    {EXIF_TAG_FOCAL_PLANE_RESOLUTION_UNIT, "FocalPlaneResolutionUnit",FD_VOID},
    {EXIF_TAG_SUBJECT_LOCATION, "SubjectLocation",FD_VOID},
    {EXIF_TAG_EXPOSURE_INDEX, "ExposureIndex",FD_VOID},
    {EXIF_TAG_SENSING_METHOD, "SensingMethod",FD_VOID},
    {EXIF_TAG_FILE_SOURCE, "FileSource",FD_VOID},
    {EXIF_TAG_SCENE_TYPE, "SceneType",FD_VOID},
    {EXIF_TAG_NEW_CFA_PATTERN, "CFAPattern",FD_VOID},
    {EXIF_TAG_SUBJECT_AREA, "SubjectArea",FD_VOID},
#if 0
    {EXIF_TAG_TIFF_EP_STANDARD_ID, "TIFF/EPStandardID",FD_VOID},
#endif
    {EXIF_TAG_CUSTOM_RENDERED, "CustomRendered",FD_VOID},
    {EXIF_TAG_EXPOSURE_MODE, "ExposureMode",FD_VOID},
    {EXIF_TAG_WHITE_BALANCE, "WhiteBalance",FD_VOID},
    {EXIF_TAG_DIGITAL_ZOOM_RATIO, "DigitalZoomRatio",FD_VOID},
    {EXIF_TAG_FOCAL_LENGTH_IN_35MM_FILM, "FocalLengthIn35mmFilm",FD_VOID},
    {EXIF_TAG_SCENE_CAPTURE_TYPE, "SceneCaptureType",FD_VOID},
    {EXIF_TAG_GAIN_CONTROL, "GainControl",FD_VOID},
    {EXIF_TAG_CONTRAST, "Contrast",FD_VOID},
    {EXIF_TAG_SATURATION, "Saturation",FD_VOID},
    {EXIF_TAG_SHARPNESS, "Sharpness",FD_VOID},
    {EXIF_TAG_DEVICE_SETTING_DESCRIPTION, "DeviceSettingDescription",FD_VOID},
    {EXIF_TAG_SUBJECT_DISTANCE_RANGE, "SubjectDistanceRange",FD_VOID},
    {EXIF_TAG_IMAGE_UNIQUE_ID, "ImageUniqueID",FD_VOID},
    {0, NULL,FD_VOID}};

static lispval exif_get(lispval x,lispval prop)
{
  ExifData *exdata;
  if (FD_PACKETP(x))
    exdata = exif_data_new_from_data(FD_PACKET_DATA(x),FD_PACKET_LENGTH(x));
  else if (FD_STRINGP(x)) {
    int n_bytes;
    unsigned char *data = u8_filedata(FD_CSTRING(x),&n_bytes);
    exdata = exif_data_new_from_data(FD_PACKET_DATA(x),FD_PACKET_LENGTH(x));
    u8_free(data);}
  else return fd_type_error(_("filename or packet"),"exif_get",x);
  if (FD_VOIDP(prop)) {
    lispval slotmap = fd_empty_slotmap();
    struct TAGINFO *scan = taginfo;

    while (scan->tagname) {
      ExifEntry *exentry = exif_data_get_entry(exdata,scan->tagid);
      if (exentry) {
        lispval val = exif2lisp(exentry);
        fd_add(slotmap,scan->tagsym,val);
        fd_decref(val);}
      scan++;}
    return slotmap;}
  else {
    ExifEntry *exentry; ExifTag tag;
    lispval tagval = fd_hashtable_get(&exif_tagmap,prop,FD_VOID);
    if (!(FD_FIXNUMP(tagval)))
      return fd_type_error(_("exif tag"),"exif_get",prop);
    tag = (ExifTag)FD_FIX2INT(tagval);
    exentry = exif_data_get_entry(exdata,tag);
    if (exentry) return exif2lisp(exentry);
    else return FD_EMPTY_CHOICE;}
}

static long long int exif_init = 0;

FD_EXPORT int fd_init_exif()
{
  lispval exif_module;
  struct TAGINFO *scan = taginfo;
  if (exif_init) return 0;
  /* u8_register_source_file(_FILEINFO); */
  exif_init = u8_millitime();
  exif_module = fd_new_cmodule("EXIF",(FD_MODULE_SAFE),fd_init_exif);
  FD_INIT_STATIC_CONS(&exif_tagmap,fd_hashtable_type);
  fd_make_hashtable(&exif_tagmap,139);
  while (scan->tagname) {
    lispval symbol = fd_intern(scan->tagname);
    fd_hashtable_store(&exif_tagmap,symbol,FD_INT(scan->tagid));
    scan->tagsym = symbol;
    scan++;}
  fd_idefn(exif_module,fd_make_cprim2("EXIF-GET",exif_get,1));

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
