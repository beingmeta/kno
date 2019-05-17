#include <libu8/u8stdio.h>
#include <errno.h>

KNO_EXPORT void kno_init_webtools(void);
KNO_EXPORT void kno_init_texttools(void);

KNO_EXPORT int *kno_main_errno_ptr;

int *kno_main_errno_ptr=NULL;

static U8_MAYBE_UNUSED u8_byte _kno_dbg_outbuf[KNO_DEBUG_OUTBUF_SIZE];

static U8_MAYBE_UNUSED u8_string _kno_debug_out(lispval x)
{
  return kno_lisp2buf(x,KNO_DEBUG_OUTBUF_SIZE,_kno_dbg_outbuf);
}

static U8_MAYBE_UNUSED u8_string _kno_debug_outn(lispval x,int n)
{
  if (n<(KNO_DEBUG_OUTBUF_SIZE+7))
    return kno_lisp2buf(x,n,_kno_dbg_outbuf);
  else return kno_lisp2buf(x,n,NULL);
}

static U8_MAYBE_UNUSED void _kno_lisp2stderr(lispval x)
{
  u8_fprintf(stderr,"%q\n",x);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
