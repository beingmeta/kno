#include <libu8/u8stdio.h>
#include <errno.h>

FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);

FD_EXPORT int *fd_main_errno_ptr;

int *fd_main_errno_ptr=NULL;

static U8_MAYBE_UNUSED u8_byte _fd_dbg_outbuf[FD_DEBUG_OUTBUF_SIZE];

static U8_MAYBE_UNUSED u8_string _fd_debug_out(lispval x)
{
  return fd_lisp2buf(x,FD_DEBUG_OUTBUF_SIZE,_fd_dbg_outbuf);
}

static U8_MAYBE_UNUSED u8_string _fd_debug_outn(lispval x,int n)
{
  if (n<(FD_DEBUG_OUTBUF_SIZE+7))
    return fd_lisp2buf(x,n,_fd_dbg_outbuf);
  else return fd_lisp2buf(x,n,NULL);
}

static U8_MAYBE_UNUSED void _fd_lisp2stderr(lispval x)
{
  u8_fprintf(stderr,"%q\n",x);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
