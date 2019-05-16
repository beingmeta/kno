/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2012-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <hunspell/hunspell.h>

KNO_EXPORT int kno_init_hunspell(void) KNO_LIBINIT_FN;

static u8_string hunspell_dictionaries = "/usr/share/hunspell";

static void free_suggestions(Hunhandle *hh,char ***suggestions,int n)
{
  Hunspell_free_list(hh,suggestions,n);
}

static lispval hunspell_symbol;

static void recycle_hunspell(void *rawptrval)
{
  Hunhandle *hh = (Hunhandle *) rawptrval;
  if (hh) Hunspell_destroy(hh);
}

static Hunhandle *gethunspell(lispval x)
{
  if (KNO_PTR_TYPEP(x,kno_rawptr_type)) {
    struct KNO_RAWPTR *rawptr = (kno_rawptr) x;
    if (rawptr->raw_typespec == hunspell_symbol)
      return  (Hunhandle *) rawptr->ptrval;}
  kno_seterr("NotASpellChecker","getunspell",NULL,x);
  return NULL;
}

static lispval hunspell_open(lispval path,lispval keyboard)
{
  u8_string base = KNO_CSTRING(path);
  u8_string aff_path = u8_string_append(base,".aff",NULL);
  u8_string dic_path = u8_string_append(base,".dic",NULL);
  if (!(u8_file_existsp(aff_path))) {
    u8_string try_path = u8_mkpath(hunspell_dictionaries,aff_path);
    if (u8_file_existsp(try_path)) {
      u8_string try_dic = u8_mkpath(hunspell_dictionaries,dic_path);
      u8_free(aff_path);
      u8_free(dic_path);
      aff_path = try_path;
      dic_path = try_dic;}
    else {
      kno_seterr("NoHunspellDictionaries","hunspell_open",base,KNO_VOID);
      return KNO_ERROR_VALUE;}}
  Hunhandle *hh = (KNO_VOIDP(keyboard)) ?
    (Hunspell_create(aff_path,dic_path)) :
    (Hunspell_create_key(aff_path,dic_path,KNO_CSTRING(keyboard)));
  if (hh == NULL) {
    if (errno) u8_graberrno("hunspell_open",u8_strdup(dic_path));
    u8_free(aff_path); u8_free(dic_path);
    return kno_err("HunspellOpenFailed","hunspell_open",NULL,KNO_VOID);}
  lispval result =
    kno_wrap_pointer(hh,recycle_hunspell,
                    hunspell_symbol,
                    dic_path);
  u8_free(aff_path);
  u8_free(dic_path);
  return result;
}

#if HAVE_HUNSPELL_ADD_DIC
static lispval hunspell_add_dictionary(lispval h,lispval dictpath)
{
  Hunhandle *hh = gethunspell(h);
  if (hh == NULL) return KNO_ERROR;
  int status = Hunspell_add_dic(hh,KNO_CSTRING(dictpath));
  if (status == 1)
    return KNO_FALSE;
  else return kno_incref(h);
}
#endif


static lispval hunspell_check(lispval word,lispval hunspell)
{
  Hunhandle *hh = gethunspell(hunspell);
  if (hh == NULL) return KNO_ERROR_VALUE;
  int rv = Hunspell_spell(hh,KNO_CSTRING(word));
  if (rv)
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval hunspell_suggest(lispval word,lispval hunspell)
{
  Hunhandle *hh = gethunspell(hunspell);
  if (hh == NULL) return KNO_ERROR_VALUE;
  char **suggestions = NULL;
  int n = 0;
  if ( (n=(Hunspell_suggest(hh,&suggestions,KNO_CSTRING(word)))) > 0) {
    if (n == 0) return KNO_FALSE;
    int i = 0;
    lispval result = kno_make_vector(n,NULL);
    char **scan = suggestions, **limit = suggestions+n;
    while (scan < limit) {
      char *suggestion = *scan;
      lispval string = kno_make_string(NULL,-1,suggestion);
      KNO_VECTOR_SET(result,i,string);
      i++; scan++;}
    free_suggestions(hh,&suggestions,n);
    suggestions = NULL;
    return result;}
  return kno_empty_vector(0);
}

static lispval hunspell_add_word(lispval h,lispval word,lispval example)
{
  Hunhandle *hh = gethunspell(h);
  if (hh == NULL) return KNO_ERROR;
  int status = (KNO_VOIDP(example)) ?
    (Hunspell_add(hh,KNO_CSTRING(word))) :
    (Hunspell_add_with_affix(hh,KNO_CSTRING(word),KNO_CSTRING(example)));
  return KNO_INT(status);
}

static lispval hunspell_remove_word(lispval h,lispval word)
{
  Hunhandle *hh = gethunspell(h);
  if (hh == NULL) return KNO_ERROR;
  int status = Hunspell_remove(hh,KNO_CSTRING(word));
  return KNO_INT(status);
}

/* Initialization */

static int hunspell_init = 0;

KNO_EXPORT int kno_init_hunspell()
{
  if (hunspell_init)
    return 0;
  else hunspell_init = u8_millitime();
  lispval hunspell_module =
    kno_new_cmodule("HUNSPELL",(KNO_MODULE_SAFE),kno_init_hunspell);;

  kno_idefn2(hunspell_module,"HUNSPELL/OPEN",hunspell_open,1,
            "(hunspell/open *affixes* *dictionary* [*key*]) "
            "Opens (creates) a spellchecker",
            kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);

#if HAVE_HUNSPELL_ADD_DIC
  kno_idefn2(hunspell_module,"HUNSPELL/DICTIONARY!",hunspell_add_dictionary,1,
            "(hunspell/dictionary! *hunspeller* *dictionary*) "
            "Opens (creates) a spellchecker",
            kno_rawptr_type,KNO_VOID,kno_string_type,KNO_VOID);
#endif

  kno_idefn2(hunspell_module,"HUNSPELL/CHECK",hunspell_check,2,
            "(hunspell/check *hunspell* *word*) "
            "Opens (creates) a spellchecker",
            kno_string_type,KNO_VOID,
            kno_rawptr_type,KNO_VOID);

  kno_idefn2(hunspell_module,"HUNSPELL/SUGGEST",hunspell_suggest,2,
            "(hunspell/check *hunspell* *word*) "
            "Opens (creates) a spellchecker",
            kno_string_type,KNO_VOID,
            kno_rawptr_type,KNO_VOID);

  kno_idefn3(hunspell_module,"HUNSPELL/ADD!",hunspell_add_word,2,
            "(hunspell/add! *hunspell* *word* *justlike*) ",
            kno_rawptr_type,KNO_VOID,
            kno_string_type,KNO_VOID,
            kno_string_type,KNO_VOID);

  kno_idefn2(hunspell_module,"HUNSPELL/REMOVE!",hunspell_remove_word,2,
            "(hunspell/remove! *hunspell* *word*) ",
            kno_rawptr_type,KNO_VOID,
            kno_string_type,KNO_VOID);

  hunspell_symbol = kno_intern("HUNSPELL");

  kno_finish_module(hunspell_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
