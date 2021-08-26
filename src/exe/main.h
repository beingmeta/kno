#include <libu8/u8stdio.h>
#include <errno.h>
#include <locale.h>

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

KNO_EXPORT void _kno_finish_threads(void);

#ifdef KNO_DEFAULT_LIBSCM
static u8_string default_libscm = KNO_DEFAULT_LIBSCM;
#else
static u8_string default_libscm = NULL;
#endif

static void init_libraries()
{
  setlocale(LC_ALL,"");
#if (!(HAVE_CONSTRUCTOR_ATTRIBUTES))
  u8_initialize_u8stdio();
  u8_init_chardata_c();
#endif

  if (default_libscm) kno_default_libscm = default_libscm;

#if (KNO_STATIC) || (!(HAVE_CONSTRUCTOR_ATTRIBUTES))
  kno_init_lisp_types();
  kno_init_storage();
  kno_init_scheme();
#if KNO_TESTCONFIG
  kno_init_texttools();
  kno_init_webtools();
#endif
#endif
  KNO_INIT_SCHEME_BUILTINS();
}
