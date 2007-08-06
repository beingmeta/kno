/* C Mode */

/* phonetic.c
   Copyright (C) 2007 beingmeta, inc.

   This file implements various phonetic and quasi-phonetic hashing
   algorithms for spelling correction etc.
*/

static char versionid[] =
  "$Id:$";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"

#include <libu8/libu8.h>
#include <libu8/u8streamio.h>
#include <libu8/u8stringfns.h>

#define VOWELS "AEIOUY"

static int soundex_class(int c)
{
  switch (c) {
  case 'H': case 'W': return -1;
  case 'B': case 'F': case 'P': case 'V': return '1';
  case 'C': case 'G': case 'J': case 'K': case 'Q':
  case 'S': case 'X': case 'Z':
    return '2';
  case 'D': case 'T': return '3';
  case 'L': return '4';
  case 'M': case 'N': return '5';
  case 'R': return '6';
  default:
    return 0;}
}

FD_EXPORT u8_string fd_soundex(u8_string string)
{
  struct U8_OUTPUT out; int c, lastc;
  u8_byte *s=string, buf[8];
  U8_INIT_FIXED_OUTPUT(&out,8,buf);
  c=u8_sgetc(&s);
  c=u8_toupper(u8_base_char(c));
  u8_putc(&out,c);
  lastc=c; c=u8_sgetc(&s);
  while (c>=0) {
    if ((out.u8_outptr-out.u8_outbuf)>=4)
      return u8_strdup(out.u8_outbuf);
    c=soundex_class(u8_toupper(u8_base_char(c)));
    if ((c>0) && (c!=lastc)) u8_putc(&out,c);
    if (c>=0) lastc=c;
    c=u8_sgetc(&s);}
  while ((out.u8_outptr-out.u8_outbuf)<4) u8_putc(&out,'0');
  return u8_strdup(out.u8_outbuf);
}

FD_EXPORT u8_string fd_metaphone(u8_string string)
{
  struct U8_OUTPUT out; char buf[32], *start, *scan;
  u8_byte *s=string; int c=u8_sgetc(&s), lastc=-1, len=u8_strlen(string);
  U8_INIT_OUTPUT(&out,32);
  /* First we write an uppercase ASCII version of the string to a buffer. */
  if (len>32)
    scan=start=u8_malloc(FD_STRLEN(string)+1);
  else scan=start=buf;
  while (c>0)  {
    if (u8_isspace(c)) c=' ';
    else c=u8_toupper(u8_base_char(c));
    if ((c<0x80) && (c!=lastc)) *scan++=c;
    lastc=c;
    c=u8_sgetc(&s);}
  *scan='\0';
  scan=start;
  if ((strncmp(scan,"AE",2)==0) || (strncmp(scan,"GN",2)==0)  ||
      (strncmp(scan,"KN",2)==0) || (strncmp(scan,"PN",2)==0) ||
      (strncmp(scan,"WR",2)==0))
    scan++;
  else if (*scan=='X') *scan='S';
  else if (strncmp(scan,"WH",2)==0) {
    scan++; *scan='W';}
  while (*scan) {
    switch (*scan) {
    case ' ': case 'F': case 'J': case 'L': case 'M': case 'N': case 'R': 
      u8_putc(&out,*scan); break;
    case 'Q': u8_putc(&out,'K'); break;
    case 'V': u8_putc(&out,'F'); break;
    case 'Z': u8_putc(&out,'S'); break;
    case 'B':
      if ((scan>start) && (scan[1]=='\0') && (scan[-1]!='M')) s++;
      else {u8_putc(&out,'B'); s++;}
      break;
    case 'C':
      if (strncmp(scan,"CIA",3)==0) {
	u8_putc(&out,'X'); scan=scan+3; continue;}
      else if (strncmp(scan,"CH",2)==0) {
	u8_putc(&out,'X'); scan=scan+2; continue;}
      else if ((strncmp(scan,"CI",3)==0) || (strncmp(scan,"CE",2)==0) || (strncmp(s,"CY",2)==0)) {
	u8_putc(&out,'S'); scan=scan+2; continue;}
      else if ((scan>start) &&
	       ((strncmp(scan-1,"SCI",3)==0) || (strncmp(scan-1,"SCE",3)==0) || (strncmp(scan-1,"SCY",3)==0))) {
	scan=scan+2; continue;}
      else if ((scan>start) && (strncmp(scan-1,"SCH",3)==0)) {
	u8_putc(&out,'K'); scan=scan+2; continue;}
      else {u8_putc(&out,'K'); break;}
    case 'D':
      if ((strncasecmp(scan,"DGY",3)) || (strncasecmp(scan,"DGI",3)) || (strncasecmp(scan,"DGE",3))) {
	u8_putc(&out,'J'); scan=scan+3; continue;}
      else u8_putc(&out,'T');
      break;
    case 'G':
      if ((strncmp(scan,"GN",2)==0) || (strncmp(scan,"GNED",4)==0)) break;
      else if ((strncmp(scan,"GH",2)==0) && (scan[2]!='\0') && (strchr(VOWELS,scan[2])==NULL)) break;
      else if (((scan[1]=='I') || (scan[1]=='E') || (scan[1]=='Y')) && (scan>start) && (scan[-1]!='G'))
	u8_putc(&out,'J');
      else u8_putc(&out,'K');
      break;
    case 'H':
      if ((scan>start) && (strchr(VOWELS,scan[-1])) && (strchr(VOWELS,scan[1])==NULL)) {}
      else u8_putc(&out,'H');
      break;
    case 'K':
      if ((scan>start) && (scan[-1]=='C')) {}
      else u8_putc(&out,'K');
      break;
    case 'P':
      if (scan[1]=='H') {
	u8_putc(&out,'F'); scan=scan+2; continue;}
      else u8_putc(&out,'P');
      break;
    case 'S':
      if (scan[1]=='H') {
	u8_putc(&out,'X'); scan=scan+2; continue;}
      else if (((scan>start) &&
		((strncmp(scan,"sio",3)==0) || (strncmp(scan,"sia",3)==0))))
	u8_putc(&out,'X');
      else u8_putc(&out,'S');
      break;
    case 'T':
      if ((scan>start) &&
	  ((strncmp(scan,"tia",3)==0) || (strncmp(scan,"tio",3)==0)))
	u8_putc(&out,'X');
      else if (scan[1]=='H') {
	u8_putc(&out,'0'); scan=scan+2; continue;}
      else u8_putc(&out,'T');
      break;
    case 'W': case 'Y':
      if (strchr(VOWELS,scan[1])) u8_putc(&out,*scan);
      else {}
      break;
    case 'X':
      u8_puts(&out,"KS");
      break;
    default: if (scan==start) u8_putc(&out,*scan);}
    scan++;}
  if (start!=buf) u8_free(start);
  return out.u8_outbuf;
}




