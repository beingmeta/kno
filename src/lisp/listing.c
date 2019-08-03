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
#include "kno/compounds.h"
#include "kno/sequences.h"
#include "kno/history.h"
#include "kno/storage.h"

#include "libu8/u8printf.h"
#include "libu8/u8strings.h"


#define LISTABLEP(x)                                                    \
  ( (KNO_CHOICEP(x)) || (KNO_VECTORP(x)) || (KNO_PAIRP(x)) ||           \
    (KNO_SLOTMAPP(x)) || (KNO_SCHEMAPP(x)) || (KNO_PRECHOICEP(x)) ||    \
    (KNO_COMPOUND_VECTORP(x)) )
#define SHORTP(x)                          \
  ((KNO_SYMBOLP(x)) || (KNO_NUMBERP(x)) ||              \
   ( (KNO_STRINGP(x)) && (KNO_STRLEN(x) < 17) ) )
#define UNLISTABLEP(x)                          \
  ( (! (LISTABLEP(x)) ) ||                                              \
    ( (KNO_PAIRP(x)) && (SHORTP(KNO_CAR(x))) && (SHORTP(KNO_CDR(x))) ) )
#define ODDPAIRP(x)                                                     \
  ( (KNO_PAIRP(x)) && (SHORTP(KNO_CAR(x))) && (SHORTP(KNO_CDR(x))) )


static int list_length(lispval scan)
{
  int len = 0;
  while (1)
    if (NILP(scan)) return len;
    else if (KNO_PAIRP(scan)) {
      scan = KNO_CDR(scan); len++;}
    else return len+1;
}

static int list_elements(u8_output out,
                         lispval result,
                         u8_string label,
                         u8_string pathref,
                         u8_string indent,
                         kno_listobj_fn eltfn,
                         int width,
                         int detail,
                         int depth);
static void output_key(u8_output out,lispval key,kno_listobj_fn eltfn);

static void custom_eltfn_error(lispval item)
{
  u8_exception ex = u8_erreify();
  if (ex)
    u8_log(LOGNOTICE,"CustomListError",
           "Exception %m <%s> (%s) %q",
           ex->u8x_cond,ex->u8x_context,
           ex->u8x_details,
           item);
  else u8_log(LOGNOTICE,"CustomListError","%q",item);
  if (ex) u8_free_exception(ex,0);
}

static int list_item(u8_output out,lispval item,kno_listobj_fn eltfn)
{
  if (eltfn == NULL)
    kno_unparse(out,item);
  else if ( (KNO_OIDP(item)) || (KNO_COMPOUNDP(item))  ) {
    lispval alt = eltfn(item);
    if (KNO_VOIDP(alt))
      kno_unparse(out,item);
    else if (KNO_ABORTP(alt)) {
      custom_eltfn_error(item);
      kno_unparse(out,alt);}
    else {
      kno_unparse(out,alt);
      kno_decref(alt);}}
  else {
    kno_unparse(out,item);}
  return 0;
}

static void list_table(u8_output out,lispval table,
                       u8_string label,u8_string pathref,int path,
                       u8_string indent,
                       kno_listobj_fn eltfn,
                       int width,
                       int detail,
                       int depth)
{
  lispval keys = kno_getkeys(table);
  int count = 0, n_keys = KNO_CHOICE_SIZE(keys);
  int show_keys = ( detail <= 0) ? (n_keys) :
    (n_keys < (detail+5) ) ? (n_keys) :
    (n_keys >= detail) ? (detail) : (n_keys);
  if (show_keys == 0) show_keys = 1;
  u8_byte indentbuf[64] = { 0 }, pathbuf[64] = { 0 };
  u8_string val_indent = u8_sprintf(indentbuf,64,"%s    ",indent);
  u8_string full_pathref = ( path < 0) ? (pathref) :
    (pathref) ? (u8_sprintf(pathbuf,64,"%s.%d",pathref,path)) :
    (NULL);
  int val_detail = (detail == 0) ? (detail) :
    (detail > 0) ? ((detail/2)+1) :
    (((-detail)/2)+1);
  if (label == NULL) label = full_pathref;
  u8_string prefix = (KNO_SCHEMAPP(table)) ? ("") : ("#");
  if (n_keys == 0) {
    u8_printf(out,"%s[]",prefix);
    return;}
  else if (show_keys < n_keys)
    u8_printf(out,"%s[;; %s (%d/%d slots)",prefix,U8S(label),show_keys,n_keys);
  else u8_printf(out,"%s[;; %s (%d slots)",prefix,U8S(label),n_keys);
  DO_CHOICES(key,keys) {
    if (count >= show_keys) {
      KNO_STOP_DO_CHOICES;
      break;}
    u8_byte key_buf[64] = { 0 };
    u8_byte label_buf[256] = { 0 };
    u8_byte val_pathbuf[256] = { 0 };
    u8_string keystring =
      ( (KNO_OIDP(key)) ? (kno_oid2string(key,key_buf,64)) :
        (u8_bprintf(key_buf,"%q",key)) );
    u8_string val_pathref = (full_pathref) ?
      (u8_bprintf(val_pathbuf,"%s.%s",full_pathref,keystring)) :
      (NULL);
    u8_string val_label = (full_pathref) ?
      (u8_bprintf(label_buf,"%s.%s",full_pathref,keystring)) :
      (NULL);
    lispval val = kno_get(table,key,KNO_EMPTY_CHOICE);
    if (EMPTYP(val)) {
      u8_printf(out,"\n%s  ",indent);
      output_key(out,key,eltfn);
      u8_puts(out," \t#> {} \t;;no values");}
    else {
      if ( (KNO_CHOICEP(val)) || (KNO_VECTORP(val)) ||
           ( (KNO_PAIRP(val)) &&
             (!(KNO_SYMBOLP(KNO_CAR(val)))) &&
             (!(ODDPAIRP(val)) ) ) ) {
        u8_printf(out,"\n%s  ",indent);
        output_key(out,key,eltfn);
        u8_puts(out," #> ");
        list_elements(out,val,val_label,val_pathref,
                      val_indent,eltfn,width,val_detail,depth+1);}
      else if ( (KNO_SLOTMAPP(val)) || (KNO_SCHEMAPP(val)) ) {
        u8_printf(out,"\n%s  ",indent);
        output_key(out,key,eltfn);
        u8_puts(out," #> ");
        list_table(out,val,val_label,val_pathref,-1,
                   val_indent,eltfn,width,val_detail,depth+1);}
      else {
        U8_SUB_STREAM(tmp,1000,out);
        output_key(tmpout,key,eltfn);
        u8_puts(tmpout," \t#> ");
        list_item(tmpout,val,eltfn);
        if ((tmp.u8_write-tmp.u8_outbuf)<width) {
          if (full_pathref)
            u8_printf(out,"\n%s  %s \t;;=%s.%s",
                      indent,tmp.u8_outbuf,full_pathref,keystring);
          else u8_printf(out,"\n%s  %s \t;; (%s)",
                         indent,tmp.u8_outbuf,keystring);}
        else {
          /* Reset to zero */
          tmp.u8_write=tmp.u8_outbuf; tmp.u8_outbuf[0]='\0';
          kno_pprint(tmpout,val,val_indent,3,3,width);
          if (full_pathref)
            u8_printf(out,"\n%s  %s #> ;;=%s.%s\n%s%s",
                      indent,keystring,full_pathref,keystring,
                      val_indent,tmp.u8_outbuf);
          else u8_printf(out,"\n%s  %s #> ;;\n%s%s",
                         indent,keystring,val_indent,tmp.u8_outbuf);}
        u8_close_output(tmpout);}
      u8_flush(out);}
    kno_decref(val);
    count++;}
  if ( (label) && (n_keys > show_keys) )
    u8_printf(out,"\n%s  ] ;;... %d/%d more slots in %s ....",
              indent,(n_keys-show_keys),n_keys,label,indent);
  else if (n_keys > show_keys)
    u8_printf(out,"\n%s  ] ;;... %d/%d more slots ....",
              indent,(n_keys-show_keys),n_keys,indent);
  else if (label)
    u8_printf(out,"\n%s] ;; %s (%d slots)",indent,label,n_keys);
  else u8_puts(out," ]");
  kno_decref(keys);
}

static void output_key(u8_output out,lispval key,kno_listobj_fn eltfn)
{
  if ( (eltfn) && ( (KNO_OIDP(key)) || (KNO_COMPOUNDP(key)) ) ) {
    lispval alt = eltfn(key);
    if (KNO_VOIDP(alt))
      kno_unparse(out,key);
    else if (KNO_ABORTP(alt)) {
      custom_eltfn_error(key);
      kno_unparse(out,alt);}
    else {
      kno_unparse(out,alt);
      kno_decref(alt);}}
  else kno_unparse(out,key);
}

static void list_element(u8_output out,lispval elt,
                         u8_string pathref,int path,
                         u8_string indent,
                         kno_listobj_fn eltfn,
                         int width,
                         int detail,
                         int depth)
{
  U8_SUB_STREAM(tmp,1000,out);
  list_item(tmpout,elt,eltfn);
  if ((tmp.u8_write-tmp.u8_outbuf)<width) {
    if ((pathref) && (path>=0))
      u8_printf(out,"\n%s%s \t;;=%s.%d",indent,tmp.u8_outbuf,pathref,path);
    else if (pathref)
      u8_printf(out,"\n%s%s \t;;=%s",indent,tmp.u8_outbuf,pathref);
    else u8_printf(out,"\n%s%s",indent,tmp.u8_outbuf);
    u8_close_output(tmpout);
    return;}
  tmp.u8_write = tmp.u8_outbuf; tmp.u8_outbuf[0]='\0';
  u8_byte pathbuf[64];
  u8_string sub_path = u8_bprintf(pathbuf,"%s.%d",pathref,path);
  /* Output the element preamble, since we're putting it on a new line */
  if (UNLISTABLEP(elt)) {
    if ((pathref) && (path>=0))
      u8_printf(out,"\n%s;; %s.%d=\n%s",indent,pathref,path,indent);
    else if (pathref)
      u8_printf(out,"\n%s;; %s=\n%s",indent,pathref,indent);
    else u8_printf(out,"\n%s",indent);
    kno_pprint(out,elt,indent,3,3,width);}
  else if ( (KNO_SLOTMAPP(elt)) || (KNO_SCHEMAPP(elt)) ) {
    u8_printf(out,"\n%s",indent);
    list_table(out,elt,NULL,pathref,path,indent,eltfn,width,3,depth);}
  else {
    u8_printf(out,"\n%s",indent);
    list_elements(out,elt,sub_path,sub_path,indent,
                  eltfn,width,detail,depth+1);}
  u8_close_output(tmpout);
  u8_flush(out);
}

static int list_elements(u8_output out,
                         lispval result,
                         u8_string label,
                         u8_string pathref,
                         u8_string indent,
                         kno_listobj_fn eltfn,
                         int width,
                         int detail,
                         int depth)
{
  int count = 0, show_elts, n_elts = 0;
  u8_byte indentbuf[64] = { 0 }, startbuf[128] = { 0 };
  u8_string start, end, elt_indent;

  if (KNO_CHOICEP(result)) {
    n_elts = KNO_CHOICE_SIZE(result);
    elt_indent = u8_sprintf(indentbuf,64,"%s ",indent);
    start = "{"; end="}";}
  else if (KNO_VECTORP(result)) {
    n_elts = KNO_VECTOR_LENGTH(result);
    elt_indent = u8_sprintf(indentbuf,64,"%s  ",indent);
    start = "#("; end = ")";}
  else if (KNO_COMPOUND_VECTORP(result))  {
    lispval tag = KNO_COMPOUND_TAG(result);
    n_elts = KNO_COMPOUND_LENGTH(result);
    elt_indent = u8_sprintf(indentbuf,64,"%s    ",indent);
    start = u8_bprintf(startbuf,"#%%(%q",tag);
    end = ")";}
  else if (KNO_PAIRP(result)) {
    n_elts = list_length(result);
    elt_indent = u8_sprintf(indentbuf,64,"%s ",indent);
    start = "("; end = ")";}
  else {}

  if ((detail>0) && (n_elts>(detail*2)))
    show_elts = detail;
  else show_elts = n_elts;

  if (label == NULL) label=pathref;

  if (n_elts == show_elts)
    u8_printf(out,"%s;; %s (%d items)",start,U8S(label),n_elts);
  else u8_printf(out,"%s;; %s (%d/%d items)",
                 start,U8S(label),show_elts,n_elts);

  if (CHOICEP(result)) {
    KNO_DO_CHOICES(elt,result) {
      if ((show_elts>0) && (count<show_elts)) {
        list_element(out,elt,pathref,count,elt_indent,
                     eltfn,width,detail,depth+1);
        count++;}
      else {KNO_STOP_DO_CHOICES; break;}}}
  else if (VECTORP(result)) {
    lispval *elts = VEC_DATA(result);
    while (count<show_elts) {
      list_element(out,elts[count],pathref,count,elt_indent,
                   eltfn,width,detail,depth+1);
      count++;}}
  else if (KNO_COMPOUND_VECTORP(result)) {
    lispval *elts = KNO_COMPOUND_ELTS(result);
    while (count<show_elts) {
      list_element(out,elts[count],pathref,count,elt_indent,
                   eltfn,width,detail,depth+1);
      count++;}}
  else if (PAIRP(result)) {
    lispval scan = result;
    while (count<show_elts)
      if (PAIRP(scan)) {
        list_element(out,KNO_CAR(scan),pathref,count,elt_indent,
                     eltfn,width,detail,depth+1);
        scan = KNO_CDR(scan);
        count++;}
      else {
        u8_printf(out,"\n%s. ;; improper list",elt_indent);
        list_element(out,scan,pathref,count,elt_indent,
                     eltfn,width,detail,depth+1);
        scan = VOID;
        count++;
        break;}}
  else {}

  if (show_elts<n_elts) {
    if (label)
      u8_printf(out,"\n%s%s ;;... %d/%d more items in %s ....",
                elt_indent,end,n_elts-show_elts,n_elts,label);
    else u8_printf(out,"\n%s%s ;;... %d/%d more items ....",
                   elt_indent,end,n_elts-show_elts,n_elts);}
  else if (label)
    u8_printf(out,"\n%s%s ;;==%s (%d items)",indent,end,label,n_elts);
  else u8_printf(out,"\n%s %s ;; (%d items)",indent,end,n_elts);

  return show_elts;
}

KNO_EXPORT int kno_list_object(u8_output out,
                               lispval result,
                               u8_string label,
                               u8_string pathref,
                               u8_string indent,
                               kno_listobj_fn eltfn,
                               int width,
                               int detail)
{
  if (VOIDP(result)) return 0;
  if ((KNO_CHOICEP(result)) ||
      (KNO_VECTORP(result)) ||
      (KNO_COMPOUND_VECTORP(result)) ||
      (PAIRP(result))) {
    list_elements(out,result,label,pathref,indent,eltfn,width,detail,0);
    return 1;}
  else if ( (KNO_SLOTMAPP(result)) || (KNO_SCHEMAPP(result)) ) {
    list_table(out,result,label,pathref,-1,indent,eltfn,width,detail,0);
    return 1;}
  else if (KNO_STRINGP(result)) {
    if (detail <= 0) {
      if (KNO_STRLEN(result) > width)
        u8_printf(out,";; %s (%d chars)\n%s%lq",
                  U8S(pathref),KNO_STRLEN(result),indent,result);
      else u8_printf(out,"%lq ;; %s (%d chars)",
                     result,U8ALT(pathref,""),
                     KNO_STRLEN(result));}
    else if (KNO_STRLEN(result) > width)
      u8_printf(out,";; %s (%d chars)\n%s%q",
                U8S(pathref),KNO_STRLEN(result),indent,result);
    else u8_printf(out,"%q ;; %s (%d chars)",
                   result,U8S(pathref),KNO_STRLEN(result));
    return 1;}
  else {
    if ( (width <= 0) && (pathref) )
      u8_printf(out,";;=%s\n%q",pathref,result);
    else if (width <= 0)
      u8_printf(out,"%q",result);
    else {
      U8_SUB_STREAM(tmp,1000,out);
      kno_pprint(tmpout,result,indent,4,4,width);
      if (pathref) {
        if (strchr(tmp.u8_outbuf,'\n'))
          u8_printf(out,"%s\n%s%s",pathref,indent,tmp.u8_outbuf);
        else u8_printf(out,"%s ;; %s",tmp.u8_outbuf,pathref);}
      else u8_printf(out,"%s",tmp.u8_outbuf);
      u8_close_output(tmpout);
      u8_flush(out);}
    return 1;}
}


static int scheme_listing_initialized = 0;

KNO_EXPORT void kno_init_listing_c()
{
  if (scheme_listing_initialized) return;
  else scheme_listing_initialized = 1;
  u8_register_source_file(_FILEINFO);

}

