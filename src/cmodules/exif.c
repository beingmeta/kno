/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/eval.h"

#include <libu8/libu8io.h>

#include <libexif/exif-utils.h>
#include <libexif/exif-data.h>
#include <libexif/exif-tag.h>

KNO_EXPORT int kno_init_exif(void) KNO_LIBINIT_FN;

static lispval exif2lisp(ExifEntry *exentry)
{
  switch (exentry->format) {
  case EXIF_FORMAT_ASCII:
    if (exentry->size<8) return kno_mkstring(exentry->data);
    else if (memcmp(exentry->data,"ASCII\0\0\0",8)==0)
      return kno_mkstring(exentry->data+8);
    else if (memcmp(exentry->data,"\0\0\0\0\0\0\0\0",8)==0)
      return kno_make_packet(NULL,exentry->size-8,exentry->data+8);
    else return kno_mkstring(exentry->data);
  case EXIF_FORMAT_BYTE: case EXIF_FORMAT_SBYTE: {
    int n = exentry->components, i = 0;
    lispval *lispdata = u8_alloc_n(n,lispval);
    /* ExifByteOrder o = exif_data_get_byte_order (exentry->parent->parent); */
    /* int item_size = exif_format_get_size(exentry->format); */
    unsigned char *exifdata = exentry->data;
    if (exentry->format == EXIF_FORMAT_SBYTE)
      while (i < n) {
        char ival = exifdata[i];
        lispdata[i]=KNO_SHORT2DTYPE(ival);
        i++;}
    else while (i < n) {
      unsigned char ival = exifdata[i];
      lispdata[i]=KNO_USHORT2DTYPE(ival);
      i++;}
    if (n==1) {
      lispval retval = lispdata[0]; u8_free(lispdata);
      return retval;}
    else return kno_wrap_vector(n,lispdata);}
  case EXIF_FORMAT_SHORT: case EXIF_FORMAT_SSHORT: {
    int n = exentry->components, i = 0;
    lispval *lispdata = u8_alloc_n(n,lispval);
    ExifByteOrder o = exif_data_get_byte_order (exentry->parent->parent);
    int item_size = exif_format_get_size(exentry->format);
    unsigned char *exifdata = exentry->data;
    if (exentry->format == EXIF_FORMAT_SSHORT)
      while (i < n) {
        short ival = exif_get_short(exifdata+(i*item_size),o);
        lispdata[i]=KNO_SHORT2DTYPE(ival);
        i++;}
    else while (i < n) {
      unsigned short ival = exif_get_short(exifdata+(i*item_size),o);
      lispdata[i]=KNO_USHORT2DTYPE(ival);
      i++;}
    if (n==1) {
      lispval retval = lispdata[0]; u8_free(lispdata);
      return retval;}
    else return kno_wrap_vector(n,lispdata);}
  case EXIF_FORMAT_LONG: case EXIF_FORMAT_SLONG: {
    int n = exentry->components, i = 0;
    lispval *lispdata = u8_alloc_n(n,lispval);
    ExifByteOrder o = exif_data_get_byte_order (exentry->parent->parent);
    int item_size = exif_format_get_size(exentry->format);
    unsigned char *exifdata = exentry->data;
    if (exentry->format == EXIF_FORMAT_SLONG)
      while (i < n) {
        long int ival = exif_get_long(exifdata+(i*item_size),o);
        lispdata[i]=KNO_INT(ival);
        i++;}
    else while (i < n) {
      unsigned long int ival = exif_get_long(exifdata+(i*item_size),o);
      lispdata[i]=KNO_INT(ival);
      i++;}
    if (n==1) {
      lispval retval = lispdata[0]; u8_free(lispdata);
      return retval;}
    else return kno_wrap_vector(n,lispdata);}
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
        lispdata[i]=kno_init_double(NULL,ratio);
        i++;}
    else while (i < n) {
      ExifRational v = exif_get_rational(exifdata+(i*item_size),o);
        double ratio = ((double)(v.numerator))/((double)(v.denominator));
        lispdata[i]=kno_init_double(NULL,ratio);
        i++;}
    if (n==1) {
      lispval retval = lispdata[0]; int i = 1;
      while (i<n) {kno_decref(lispdata[i]); i++;}
      u8_free(lispdata);
      return retval;}
    else return kno_wrap_vector(n,lispdata);}
  default: return KNO_EMPTY_CHOICE;
  }
}

struct KNO_HASHTABLE exif_tagmap;

static struct TAGINFO {
  int tagid; char *tagname; lispval tagsym;} taginfo[]= {
    /* {EXIF_TAG_NEW_SUBFILE_TYPE, "NewSubfileType",KNO_VOID}, */
    {EXIF_TAG_INTEROPERABILITY_INDEX, "InteroperabilityIndex",KNO_VOID},
    {EXIF_TAG_INTEROPERABILITY_VERSION, "InteroperabilityVersion",KNO_VOID},
    {EXIF_TAG_IMAGE_WIDTH, "ImageWidth",KNO_VOID},
    {EXIF_TAG_IMAGE_LENGTH, "ImageLength",KNO_VOID},
    {EXIF_TAG_FILL_ORDER, "FillOrder",KNO_VOID},
    {EXIF_TAG_DOCUMENT_NAME, "DocumentName",KNO_VOID},
    {EXIF_TAG_IMAGE_DESCRIPTION, "ImageDescription",KNO_VOID},
    {EXIF_TAG_MAKE, "Make",KNO_VOID},
    {EXIF_TAG_MODEL, "Model",KNO_VOID},
    {EXIF_TAG_STRIP_OFFSETS, "StripOffsets",KNO_VOID},
    {EXIF_TAG_ORIENTATION, "Orientation",KNO_VOID},
    {EXIF_TAG_SAMPLES_PER_PIXEL, "SamplesPerPixel",KNO_VOID},
    {EXIF_TAG_ROWS_PER_STRIP, "RowsPerStrip",KNO_VOID},
    {EXIF_TAG_STRIP_BYTE_COUNTS, "StripByteCounts",KNO_VOID},
    {EXIF_TAG_X_RESOLUTION, "XResolution",KNO_VOID},
    {EXIF_TAG_Y_RESOLUTION, "YResolution",KNO_VOID},
    {EXIF_TAG_PLANAR_CONFIGURATION, "PlanarConfiguration",KNO_VOID},
    {EXIF_TAG_RESOLUTION_UNIT, "ResolutionUnit",KNO_VOID},
    {EXIF_TAG_TRANSFER_FUNCTION, "TransferFunction",KNO_VOID},
    {EXIF_TAG_SOFTWARE, "Software",KNO_VOID},
    {EXIF_TAG_DATE_TIME, "DateTime",KNO_VOID},
    {EXIF_TAG_ARTIST, "Artist",KNO_VOID},
    {EXIF_TAG_WHITE_POINT, "WhitePoint",KNO_VOID},
    {EXIF_TAG_PRIMARY_CHROMATICITIES, "PrimaryChromaticities",KNO_VOID},
    {EXIF_TAG_TRANSFER_RANGE, "TransferRange",KNO_VOID},
    /* {EXIF_TAG_SUB_IFDS, "SubIFDs",KNO_VOID}, */
    {EXIF_TAG_JPEG_PROC, "JPEGProc",KNO_VOID},
    {EXIF_TAG_JPEG_INTERCHANGE_FORMAT, "JPEGInterchangeFormat",KNO_VOID},
    {EXIF_TAG_JPEG_INTERCHANGE_FORMAT_LENGTH,
     "JPEGInterchangeFormatLength",KNO_VOID},
    {EXIF_TAG_YCBCR_COEFFICIENTS, "YCbCrCoefficients",KNO_VOID},
    {EXIF_TAG_YCBCR_SUB_SAMPLING, "YCbCrSubSampling",KNO_VOID},
    {EXIF_TAG_YCBCR_POSITIONING, "YCbCrPositioning",KNO_VOID},
    {EXIF_TAG_REFERENCE_BLACK_WHITE, "ReferenceBlackWhite",KNO_VOID},
    /* {EXIF_TAG_XML_PACKET, "XMLPacket",KNO_VOID}, */
    {EXIF_TAG_RELATED_IMAGE_FILE_FORMAT, "RelatedImageFileFormat",KNO_VOID},
    {EXIF_TAG_RELATED_IMAGE_WIDTH, "RelatedImageWidth",KNO_VOID},
    {EXIF_TAG_RELATED_IMAGE_LENGTH, "RelatedImageLength",KNO_VOID},
    {EXIF_TAG_CFA_REPEAT_PATTERN_DIM, "CFARepeatPatternDim",KNO_VOID},
    {EXIF_TAG_CFA_PATTERN, "CFAPattern",KNO_VOID},
    {EXIF_TAG_BATTERY_LEVEL, "BatteryLevel",KNO_VOID},
    {EXIF_TAG_COPYRIGHT, "Copyright",KNO_VOID},
    {EXIF_TAG_EXPOSURE_TIME, "ExposureTime",KNO_VOID},
    {EXIF_TAG_FNUMBER, "FNumber",KNO_VOID},
    {EXIF_TAG_IPTC_NAA, "IPTC/NAA",KNO_VOID},
    /* {EXIF_TAG_IMAGE_RESOURCES, "ImageResources",KNO_VOID}, */
    {EXIF_TAG_EXIF_IFD_POINTER, "ExifIFDPointer",KNO_VOID},
    {EXIF_TAG_INTER_COLOR_PROFILE, "InterColorProfile",KNO_VOID},
    {EXIF_TAG_EXPOSURE_PROGRAM, "ExposureProgram",KNO_VOID},
    {EXIF_TAG_SPECTRAL_SENSITIVITY, "SpectralSensitivity",KNO_VOID},
    {EXIF_TAG_GPS_INFO_IFD_POINTER, "GPSInfoIFDPointer",KNO_VOID},
#if 0
    {EXIF_TAG_GPS_LATITUDE_REF, "GPSLatitudeRef",KNO_VOID},
    {EXIF_TAG_GPS_LATITUDE, "GPSLatitude",KNO_VOID},
    {EXIF_TAG_GPS_LONGITUDE_REF, "GPSLongitudeRef",KNO_VOID},
    {EXIF_TAG_GPS_LONGITUDE, "GPSLongitude",KNO_VOID},
#endif
    {EXIF_TAG_ISO_SPEED_RATINGS, "ISOSpeedRatings",KNO_VOID},
    {EXIF_TAG_EXIF_VERSION, "ExifVersion",KNO_VOID},
    {EXIF_TAG_DATE_TIME_ORIGINAL, "DateTimeOriginal",KNO_VOID},
    {EXIF_TAG_DATE_TIME_DIGITIZED, "DateTimeDigitized",KNO_VOID},
    {EXIF_TAG_COMPONENTS_CONFIGURATION, "ComponentsConfiguration",KNO_VOID},
    {EXIF_TAG_COMPRESSED_BITS_PER_PIXEL, "CompressedBitsPerPixel",KNO_VOID},
    {EXIF_TAG_SHUTTER_SPEED_VALUE, "ShutterSpeedValue",KNO_VOID},
    {EXIF_TAG_APERTURE_VALUE, "ApertureValue",KNO_VOID},
    {EXIF_TAG_BRIGHTNESS_VALUE, "BrightnessValue",KNO_VOID},
    {EXIF_TAG_EXPOSURE_BIAS_VALUE, "ExposureBiasValue",KNO_VOID},
    {EXIF_TAG_MAX_APERTURE_VALUE, "MaxApertureValue",KNO_VOID},
    {EXIF_TAG_SUBJECT_DISTANCE, "SubjectDistance",KNO_VOID},
    {EXIF_TAG_METERING_MODE, "MeteringMode",KNO_VOID},
    {EXIF_TAG_LIGHT_SOURCE, "LightSource",KNO_VOID},
    {EXIF_TAG_FLASH, "Flash",KNO_VOID},
    {EXIF_TAG_FOCAL_LENGTH, "FocalLength",KNO_VOID},
    {EXIF_TAG_MAKER_NOTE, "MakerNote",KNO_VOID},
    {EXIF_TAG_USER_COMMENT, "UserComment",KNO_VOID},
    {EXIF_TAG_SUB_SEC_TIME, "SubsecTime",KNO_VOID},
    {EXIF_TAG_SUB_SEC_TIME_ORIGINAL, "SubSecTimeOriginal",KNO_VOID},
    {EXIF_TAG_SUB_SEC_TIME_DIGITIZED, "SubSecTimeDigitized",KNO_VOID},
    {EXIF_TAG_FLASH_PIX_VERSION, "FlashPixVersion",KNO_VOID},
    {EXIF_TAG_COLOR_SPACE, "ColorSpace",KNO_VOID},
    {EXIF_TAG_PIXEL_X_DIMENSION, "PixelXDimension",KNO_VOID},
    {EXIF_TAG_PIXEL_Y_DIMENSION, "PixelYDimension",KNO_VOID},
    {EXIF_TAG_RELATED_SOUND_FILE, "RelatedSoundFile",KNO_VOID},
    {EXIF_TAG_INTEROPERABILITY_IFD_POINTER, "InteroperabilityIFDPointer",KNO_VOID},
    {EXIF_TAG_FLASH_ENERGY, "FlashEnergy",KNO_VOID},
    {EXIF_TAG_SPATIAL_FREQUENCY_RESPONSE, "SpatialFrequencyResponse",KNO_VOID},
    {EXIF_TAG_FOCAL_PLANE_X_RESOLUTION, "FocalPlaneXResolution",KNO_VOID},
    {EXIF_TAG_FOCAL_PLANE_Y_RESOLUTION, "FocalPlaneYResolution",KNO_VOID},
    {EXIF_TAG_FOCAL_PLANE_RESOLUTION_UNIT, "FocalPlaneResolutionUnit",KNO_VOID},
    {EXIF_TAG_SUBJECT_LOCATION, "SubjectLocation",KNO_VOID},
    {EXIF_TAG_EXPOSURE_INDEX, "ExposureIndex",KNO_VOID},
    {EXIF_TAG_SENSING_METHOD, "SensingMethod",KNO_VOID},
    {EXIF_TAG_FILE_SOURCE, "FileSource",KNO_VOID},
    {EXIF_TAG_SCENE_TYPE, "SceneType",KNO_VOID},
    {EXIF_TAG_NEW_CFA_PATTERN, "CFAPattern",KNO_VOID},
    {EXIF_TAG_SUBJECT_AREA, "SubjectArea",KNO_VOID},
#if 0
    {EXIF_TAG_TIFF_EP_STANDARD_ID, "TIFF/EPStandardID",KNO_VOID},
#endif
    {EXIF_TAG_CUSTOM_RENDERED, "CustomRendered",KNO_VOID},
    {EXIF_TAG_EXPOSURE_MODE, "ExposureMode",KNO_VOID},
    {EXIF_TAG_WHITE_BALANCE, "WhiteBalance",KNO_VOID},
    {EXIF_TAG_DIGITAL_ZOOM_RATIO, "DigitalZoomRatio",KNO_VOID},
    {EXIF_TAG_FOCAL_LENGTH_IN_35MM_FILM, "FocalLengthIn35mmFilm",KNO_VOID},
    {EXIF_TAG_SCENE_CAPTURE_TYPE, "SceneCaptureType",KNO_VOID},
    {EXIF_TAG_GAIN_CONTROL, "GainControl",KNO_VOID},
    {EXIF_TAG_CONTRAST, "Contrast",KNO_VOID},
    {EXIF_TAG_SATURATION, "Saturation",KNO_VOID},
    {EXIF_TAG_SHARPNESS, "Sharpness",KNO_VOID},
    {EXIF_TAG_DEVICE_SETTING_DESCRIPTION, "DeviceSettingDescription",KNO_VOID},
    {EXIF_TAG_SUBJECT_DISTANCE_RANGE, "SubjectDistanceRange",KNO_VOID},
    {EXIF_TAG_IMAGE_UNIQUE_ID, "ImageUniqueID",KNO_VOID},
    {0, NULL,KNO_VOID}};

static lispval exif_get(lispval x,lispval prop)
{
  ExifData *exdata;
  if (KNO_PACKETP(x))
    exdata = exif_data_new_from_data(KNO_PACKET_DATA(x),KNO_PACKET_LENGTH(x));
  else if (KNO_STRINGP(x)) {
    int n_bytes;
    unsigned char *data = u8_filedata(KNO_CSTRING(x),&n_bytes);
    exdata = exif_data_new_from_data(KNO_PACKET_DATA(x),KNO_PACKET_LENGTH(x));
    u8_free(data);}
  else return kno_type_error(_("filename or packet"),"exif_get",x);
  if (KNO_VOIDP(prop)) {
    lispval slotmap = kno_empty_slotmap();
    struct TAGINFO *scan = taginfo;

    while (scan->tagname) {
      ExifEntry *exentry = exif_data_get_entry(exdata,scan->tagid);
      if (exentry) {
        lispval val = exif2lisp(exentry);
        kno_add(slotmap,scan->tagsym,val);
        kno_decref(val);}
      scan++;}
    return slotmap;}
  else {
    ExifEntry *exentry; ExifTag tag;
    lispval tagval = kno_hashtable_get(&exif_tagmap,prop,KNO_VOID);
    if (!(KNO_FIXNUMP(tagval)))
      return kno_type_error(_("exif tag"),"exif_get",prop);
    tag = (ExifTag)KNO_FIX2INT(tagval);
    exentry = exif_data_get_entry(exdata,tag);
    if (exentry) return exif2lisp(exentry);
    else return KNO_EMPTY_CHOICE;}
}

static long long int exif_init = 0;

KNO_EXPORT int kno_init_exif()
{
  lispval exif_module;
  struct TAGINFO *scan = taginfo;
  if (exif_init) return 0;
  /* u8_register_source_file(_FILEINFO); */
  exif_init = u8_millitime();
  exif_module = kno_new_cmodule("exif",0,kno_init_exif);
  KNO_INIT_STATIC_CONS(&exif_tagmap,kno_hashtable_type);
  kno_make_hashtable(&exif_tagmap,139);
  while (scan->tagname) {
    lispval symbol = kno_intern(scan->tagname);
    kno_hashtable_store(&exif_tagmap,symbol,KNO_INT(scan->tagid));
    scan->tagsym = symbol;
    scan++;}
  kno_idefn(exif_module,kno_make_cprim2("EXIF-GET",exif_get,1));

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
