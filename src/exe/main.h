#include <libu8/u8stdio.h>
#include <errno.h>

FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);

FD_EXPORT int *fd_main_errno_ptr;

int *fd_main_errno_ptr=NULL;

static u8_byte _fd_dbg_outbuf[FD_DEBUG_OUTBUF_SIZE];

static u8_string _fd_debug_out(fdtype x)
{
  return fd_dtype2buf(x,FD_DEBUG_OUTBUF_SIZE,_fd_dbg_outbuf);
}

static u8_string _fd_debug_outn(fdtype x,int n)
{
  if (n<(FD_DEBUG_OUTBUF_SIZE+7))
    return fd_dtype2buf(x,n,_fd_dbg_outbuf);
  else return fd_dtype2buf(x,n,NULL);
}

static void _fd_dtype2stderr(fdtype x)
{
  u8_fprintf(stderr,"%q\n",x);
}
