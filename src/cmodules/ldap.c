/* C Mode */

/* ldap.c
   This implements FramerD bindings to ldap.
   Copyright (C) 2007-2008 beingmeta, inc.
*/

static char versionid[] =
  "$Id: texttools.c 2312 2008-02-23 23:49:10Z haase $";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8digestfns.h>

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
