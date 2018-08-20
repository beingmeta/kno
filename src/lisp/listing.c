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
#include "framerd/compounds.h"
#include "framerd/sequences.h"
#include "framerd/history.h"
#include "framerd/storage.h"

#include "libu8/u8printf.h"
#include "libu8/u8strings.h"


static int list_length(lispval scan)
{
  int len = 0;
  while (1)
    if (NILP(scan)) return len;
    else if (FD_PAIRP(scan)) {
      scan = FD_CDR(scan); len++;}
    else return len+1;
}

static int list_elements(u8_output out,
                         lispval result,
                         u8_string label,
                         u8_string pathref,
                         u8_string indent,
                         fd_listobj_fn listfn,
                         int width,
                         int detail,
                         int depth);

static int list_item(u8_output out,lispval item,fd_listobj_fn listfn)
{
  if (listfn == NULL)
    fd_unparse(out,item);
  else if ( (FD_OIDP(item)) || (FD_COMPOUNDP(item))  ) {
    int rv = listfn(out,item);
    if (rv<0) {
      u8_exception ex = u8_erreify();
      if (ex)
        u8_log(LOGNOTICE,"CustomListError",
               "Exception %m <%s> (%s) %q",
               ex->u8x_cond,ex->u8x_context,
               ex->u8x_details,
               item);
      else u8_log(LOGNOTICE,"CustomListError",
                  "%q",item);
      if (ex) u8_free_exception(ex,0);}
    if ( rv <= 0 )
      fd_unparse(out,item);
    else return 1;}
  else fd_unparse(out,item);
  return 0;
}

static void list_table(u8_output out,lispval table,
                       u8_string label,u8_string pathref,int path,
                       u8_string indent,
                       fd_listobj_fn listfn,
                       int width,
                       int detail,
                       int depth)
{
  lispval keys = fd_getkeys(table);
  int count = 0, n_keys = FD_CHOICE_SIZE(keys);
  int show_keys = ( detail <= 0) ? (n_keys) :
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
  if (n_keys == 0) {
    u8_printf(out,"#[]");
    return;}
  else if (show_keys < n_keys)
    u8_printf(out,"#[;; %s (%d/%d slots)",U8S(label),show_keys,n_keys);
  else u8_printf(out,"#[;; %s (%d slots)",U8S(label),n_keys);
  DO_CHOICES(key,keys) {
    if (count >= show_keys) {
      FD_STOP_DO_CHOICES;
      break;}
    u8_byte label_buf[64] = { 0 };
    u8_byte val_pathbuf[64] = { 0 };
    u8_string val_pathref = (full_pathref) ?
      (u8_sprintf(val_pathbuf,64,"%s.%q",full_pathref,key)) :
      (NULL);
    u8_string val_label = (full_pathref) ?
      (u8_sprintf(label_buf,64,"%s.%q",full_pathref,key)) :
      (NULL);
    lispval val = fd_get(table,key,FD_EMPTY_CHOICE);
    if (EMPTYP(val)) {
      u8_printf(out,"\n%s  %q #> {} ;;no values",indent,key);}
    else {
      U8_SUB_STREAM(tmp,1000,out);
      fd_unparse(tmpout,key);
      u8_puts(tmpout," #> ");
      int custom = list_item(tmpout,val,listfn);
      if ((tmp.u8_write-tmp.u8_outbuf)<width) {
        if (full_pathref)
          u8_printf(out,"\n%s  %s ;;=%s.%q",indent,tmp.u8_outbuf,full_pathref,key);
        else u8_printf(out,"\n%s  %s ;; (%q)",indent,tmp.u8_outbuf,key);}
      else if ( (FD_CHOICEP(val)) || (FD_VECTORP(val)) ||
                ( (FD_PAIRP(val)) && (!(FD_SYMBOLP(FD_CAR(val)))) ) ) {
        u8_printf(out,"\n%s  %q #> ",indent,key);
        list_elements(out,val,val_label,val_pathref,
                      val_indent,listfn,width,val_detail,depth+1);}
      else if ( (FD_SLOTMAPP(val)) || (FD_SCHEMAPP(val)) ) {
        u8_printf(out,"\n%s  %q #> ",indent,key);
        list_table(out,val,val_label,val_pathref,-1,
                   val_indent,listfn,width,val_detail,depth+1);}
      else {
        /* Reset to zero */
        tmp.u8_write=tmp.u8_outbuf; tmp.u8_outbuf[0]='\0';
        if (custom)
          list_item(tmpout,val,listfn);
        else fd_pprint(tmpout,val,val_indent,3,3,width);
        if (full_pathref)
          u8_printf(out,"\n%s  %q #> ;;=%s.%q\n%s%s",
                    indent,key,full_pathref,key,
                    val_indent,tmp.u8_outbuf);
        else u8_printf(out,"\n%s  %q #> ;; #%d\n%s%s",
                       indent,key,count,
                       val_indent,tmp.u8_outbuf);}
      u8_close_output(tmpout);
      u8_flush(out);}
    fd_decref(val);
    count++;}
  if ( (label) && (n_keys > show_keys) )
    u8_printf(out,"\n%s  ;;... %d/%d more slots in %s ....\n%s ]",
              indent,(n_keys-show_keys),n_keys,label,indent);
  else if (n_keys > show_keys)
    u8_printf(out,"\n%s  ;;... %d/%d more slots ....\n%s ]",
              indent,(n_keys-show_keys),n_keys,indent);
  else if (label)
    u8_printf(out,"\n%s ] ;; %s (%d slots)",indent,label,n_keys);
  else u8_puts(out," ]");
}

static void list_element(u8_output out,lispval elt,
                         u8_string pathref,int path,
                         u8_string indent,
                         fd_listobj_fn listfn,
                         int width,
                         int depth)
{
  if (pathref) {
    U8_SUB_STREAM(tmp,1000,out);
    int custom = list_item(tmpout,elt,listfn);
    if ((tmp.u8_write-tmp.u8_outbuf)<width) {
      if ((pathref) && (path>=0))
        u8_printf(out,"\n%s%s ;;=%s.%d",indent,tmp.u8_outbuf,pathref,path);
      else if (pathref)
        u8_printf(out,"\n%s%s ;;=%s",indent,tmp.u8_outbuf,pathref);
      else u8_printf(out,"\n%s%s",indent,tmp.u8_outbuf);
      u8_close_output(tmpout);
      return;}
    /* Output the element preamble, since we're putting it on a new line */
    if ((pathref) && (path>=0))
      u8_printf(out,"\n%s;; %s.%d=\n%s",indent,pathref,path,indent);
    else if (pathref)
      u8_printf(out,"\n%s;; %s=\n%s",indent,pathref,indent);
    else u8_printf(out,"\n%s",indent);
    /* Reset tmpout before pretty printing */
    tmp.u8_write=tmp.u8_outbuf; tmp.u8_outbuf[0]='\0';
    if (custom)
      list_item(out,elt,listfn);
    else fd_pprint(tmpout,elt,indent,3,3,width);
    u8_puts(out,tmp.u8_outbuf);
    u8_close_output(tmpout);
    u8_flush(out);}
  else u8_printf(out,"\n%s%Q",indent,elt);
}

static int list_elements(u8_output out,
                         lispval result,
                         u8_string label,
                         u8_string pathref,
                         u8_string indent,
                         fd_listobj_fn listfn,
                         int width,
                         int detail,
                         int depth)
{
  int count = 0, show_elts, n_elts = 0;
  u8_byte indentbuf[64] = { 0 };
  u8_string start, end, elt_indent;

  if (FD_CHOICEP(result)) {
    n_elts = FD_CHOICE_SIZE(result);
    elt_indent = u8_sprintf(indentbuf,64,"%s ",indent);
    start = "{"; end="}";}
  else if (FD_VECTORP(result)) {
    n_elts = FD_VECTOR_LENGTH(result);
    elt_indent = u8_sprintf(indentbuf,64,"%s  ",indent);
    start = "#("; end = ")";}
  else if (FD_PAIRP(result)) {
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
    FD_DO_CHOICES(elt,result) {
      if ((show_elts>0) && (count<show_elts)) {
        list_element(out,elt,pathref,count,elt_indent,listfn,width,depth+1);
        count++;}
      else {FD_STOP_DO_CHOICES; break;}}}
  else if (VECTORP(result)) {
    lispval *elts = VEC_DATA(result);
    while (count<show_elts) {
      list_element(out,elts[count],pathref,count,elt_indent,
                   listfn,width,depth+1);
      count++;}}
  else if (PAIRP(result)) {
    lispval scan = result;
    while (count<show_elts)
      if (PAIRP(scan)) {
        list_element(out,FD_CAR(scan),pathref,count,elt_indent,
                     listfn,width,depth+1);
        scan = FD_CDR(scan);
        count++;}
      else {
        u8_printf(out,"\n  . ;; improper list");
        list_element(out,scan,pathref,count,elt_indent,listfn,width,depth+1);
        scan = VOID;
        count++;
        break;}}
  else {}

  if (show_elts<n_elts) {
    if (label)
      u8_printf(out,"\n%s;;... %d/%d more items in %s ....\n%s %s",
                elt_indent,n_elts-show_elts,n_elts,label,
                indent,end);
    else u8_printf(out,"\n%s;;... %d/%d more items ....\n%s %s",
                   elt_indent,n_elts-show_elts,n_elts,indent,end);}
  else if (label)
    u8_printf(out,"\n%s %s ;; ==%s (%d items)",indent,end,label,n_elts);
  else u8_printf(out,"\n%s %s ;; (%d items)",indent,end,n_elts);

  return show_elts;
}

FD_EXPORT int fd_list_object(u8_output out,
                             lispval result,
                             u8_string label,
                             u8_string pathref,
                             u8_string indent,
                             fd_listobj_fn listfn,
                             int width,
                             int detail)
{
  if (VOIDP(result)) return 0;
  if ((FD_CHOICEP(result)) ||
      (FD_VECTORP(result)) ||
      (PAIRP(result))) {
    list_elements(out,result,label,pathref,indent,listfn,width,detail,0);
    return 1;}
  else if ( (FD_SLOTMAPP(result)) || (FD_SCHEMAPP(result)) ) {
    list_table(out,result,label,pathref,-1,indent,listfn,width,detail,0);
    return 1;}
  else if (FD_STRINGP(result)) {
    if (detail <= 0) {
      if (FD_STRLEN(result) > width)
        u8_printf(out,";; %s (%d chars)\n%s%lq",
                  U8S(pathref),FD_STRLEN(result),indent,result);
      else u8_printf(out,"%lq ;; %s (%d chars)",
                     result,U8ALT(pathref,""),
                     FD_STRLEN(result));}
    else if (FD_STRLEN(result) > width)
      u8_printf(out,";; %s (%d chars)\n%s%q",
                U8S(pathref),FD_STRLEN(result),indent,result);
    else u8_printf(out,"%q ;; %s (%d chars)",
                   result,U8S(pathref),FD_STRLEN(result));
    return 1;}
  else {
    if ( (width <= 0) && (pathref) )
      u8_printf(out,";; =%s\n%q",pathref,result);
    else if (width <= 0)
      u8_printf(out,"%q",result);
    else {
      U8_SUB_STREAM(tmp,1000,out);
      fd_pprint(tmpout,result,indent,4,4,width);
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

FD_EXPORT void fd_init_listing_c()
{
  if (scheme_listing_initialized) return;
  else scheme_listing_initialized = 1;
  u8_register_source_file(_FILEINFO);

}

/* Emacs local variables
   ;;;	Local variables: ***
   ;;;	compile-command: "make -C ../.. debug;" ***
   ;;;	indent-tabs-mode: nil ***
   ;;;	End: ***
*/
