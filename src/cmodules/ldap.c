/* C Mode */

/* ldap.c
   This implements FramerD bindings to ldap.
   Copyright (C) 2007-2015 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#if 0

FD_EXPORT void fd_init_ldap_c(void) FD_LIBINIT_FN;

static fdtype entry2slotmap(LDAP *ld,LDAPMessage *entry)
{
  BerElement *berp=NULL;
  char *result=ldap_first_attribute(ld,entry,&berp);
  while (result) {
    char **data=ldap_get_values(ld,entry,result);
    result=ldap_next_attribute(ld,entry,berp);}
}

FD_EXPORT void fd_init_ldap_c()
{
  ldap_initialize();
}


#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
