/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/dtype.h"
#include "kno/numbers.h"
#include "kno/eval.h"

#include <libu8/libu8io.h>

#include "ext/sundown/markdown.h"
#include "ext/sundown/buffer.h"
#include "ext/sundown/autolink.h"
#include "ext/sundown/html.h"

#define OUTPUT_BUF_UNIT 1024
#define HTML_RENDER_FLAGS (HTML_USE_XHTML|HTML_ESCAPE|HTML_SAFELINK)

KNO_EXPORT int kno_init_sundown(void) KNO_LIBINIT_FN;

static int sundown_init = 0;

static lispval markdown2html_prim(lispval mdstring,lispval opts)
{
  lispval result = KNO_VOID;

  struct sd_callbacks callbacks;
  struct html_renderopt options;
  struct sd_markdown *markdown;
  struct buf *ob;

  ob = bufnew(OUTPUT_BUF_UNIT);

  sdhtml_renderer(&callbacks, &options, HTML_RENDER_FLAGS);
  markdown = sd_markdown_new(0, 16, &callbacks, &options);

  sd_markdown_render(ob, KNO_CSTRING(mdstring), KNO_STRLEN(mdstring), markdown);
  sd_markdown_free(markdown);

  result = kno_make_string(NULL,ob->size,ob->data);

  bufrelease(ob);

  return result;
}

static lispval markout_prim(lispval mdstring,lispval opts)
{
  U8_OUTPUT *out = u8_current_output;

  struct sd_callbacks callbacks;
  struct html_renderopt options;
  struct sd_markdown *markdown;
  struct buf *ob;

  ob = bufnew(OUTPUT_BUF_UNIT);

  sdhtml_renderer(&callbacks, &options, HTML_RENDER_FLAGS);
  markdown = sd_markdown_new(0, 16, &callbacks, &options);

  sd_markdown_render(ob, KNO_CSTRING(mdstring), KNO_STRLEN(mdstring), markdown);
  sd_markdown_free(markdown);

  u8_putn(out,ob->data,ob->size);

  bufrelease(ob);

  return KNO_VOID;
}

KNO_EXPORT int kno_init_sundown()
{
  lispval sundown_module;
  if (sundown_init) return 0;
  /* u8_register_source_file(_FILEINFO); */
  sundown_init = 1;
  sundown_module = kno_new_cmodule("SUNDOWN",(KNO_MODULE_SAFE),kno_init_sundown);

  kno_idefn(sundown_module,
           kno_make_cprim2x("MARKDOWN->HTML",markdown2html_prim,1,
                           kno_string_type,KNO_VOID,-1,KNO_VOID));
  kno_defalias(sundown_module,"MD->HTML","MARKDOWN->HTML");

  kno_idefn(sundown_module,
           kno_make_cprim2x("MARKOUT",markout_prim,1,
                           kno_string_type,KNO_VOID,-1,KNO_VOID));

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
