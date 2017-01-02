#include <libu8/u8stdio.h>
#include <errno.h>

FD_EXPORT void fd_init_fdweb(void);
FD_EXPORT void fd_init_texttools(void);

FD_EXPORT int *fd_main_errno_ptr;

int *fd_main_errno_ptr=NULL;

