/* C Mode */

/* phonetic.c
   Copyright (C) 2006-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

   This file implements various phonetic and quasi-phonetic hashing
   algorithms for spelling correction etc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"

#include <libu8/libu8.h>
#include <libu8/u8streamio.h>
#include <libu8/u8stringfns.h>

#include <ctype.h>

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

#define get_base_char(c) ( ((c)>0x80) ? (u8_base_char(c)) : (c) )

KNO_EXPORT u8_string kno_soundex(u8_string string)
{
  struct U8_OUTPUT out; int c, lastc;
  const u8_byte *s = string; u8_byte buf[8];
  U8_INIT_FIXED_OUTPUT(&out,8,buf);
  c = u8_sgetc(&s);
  c = u8_toupper(get_base_char(c));
  u8_putc(&out,c);
  lastc = c; c = u8_sgetc(&s);
  while (c>=0) {
    if ((out.u8_write-out.u8_outbuf)>=4)
      return u8_strdup(out.u8_outbuf);
    c = soundex_class(u8_toupper(get_base_char(c)));
    if ((c>0) && (c!=lastc)) u8_putc(&out,c);
    if (c>=0) lastc = c;
    c = u8_sgetc(&s);}
  while ((out.u8_write-out.u8_outbuf)<4) u8_putc(&out,'0');
  return u8_strdup(out.u8_outbuf);
}

/* This is a minor tweak on the regular metaphone algorithm because it
   preserves case information. */
KNO_EXPORT u8_string kno_metaphone(u8_string string,int sep)
{
  struct U8_OUTPUT out;
  char buf[32], *start, *scan;
  char capbuf[32], *capstart, *capscan;
  const u8_byte *s = string;
  int c = u8_sgetc(&s), lastc = -1, len = strlen(string), lenout = 0, lc = -1;
  U8_INIT_OUTPUT(&out,32);
  /* First we write an uppercase ASCII version of the string to a buffer. */
  if (len>=32) {
    scan = start = u8_malloc(len+1);
    capscan = capstart = u8_malloc(len+1);
    memset(scan,0,len+1); memset(capscan,0,len+1);}
  else {
    scan = start = buf; memset(scan,0,sizeof(buf));
    capscan = capstart = capbuf; memset(capscan,0,sizeof(buf));}
  /* If the string is capitalized, we insert an asterisk,
     which is a modification on the standard metaphone algorithm. */
  /* if (u8_isupper(c)) u8_putc(&out,'*'); */
  while (c>=0)  {
    int gc;
    if (u8_isspace(c)) gc=' ';
    else gc = u8_toupper(get_base_char(c));
    if ((gc<0x80) && (gc!=lastc)) {
      *scan++=gc; *capscan++=u8_isupper(c);}
    lastc = gc;
    c = u8_sgetc(&s);}
  *scan='\0';
  scan = start; capscan = capstart;
  if ((strncmp(scan,"AE",2)==0) || (strncmp(scan,"GN",2)==0)  ||
      (strncmp(scan,"KN",2)==0) || (strncmp(scan,"PN",2)==0) ||
      (strncmp(scan,"WR",2)==0)) {
    scan++; capscan++;}
  else if (*scan=='X') *scan='S';
  else if (strncmp(scan,"WH",2)==0) {
    int cap = *capscan; *capscan++=cap;
    scan++; *scan='W'; }
  lenout = out.u8_write-out.u8_outbuf;
  while (*scan) {
    switch (*scan) {
    case ' ': case 'F': case 'J': case 'L': case 'M': case 'N': case 'R': 
      if (*capscan) lc = *scan; else lc = tolower(((int)(*scan)));
      u8_putc(&out,lc); break;
    case 'Q': {
      if (*capscan) u8_putc(&out,'K');
      else u8_putc(&out,'k');
      break;}
    case 'V': {
      if (*capscan) u8_putc(&out,'F');
      else u8_putc(&out,'f');
      break;}
    case 'Z': {
      if (*capscan) u8_putc(&out,'S');
      else u8_putc(&out,'s');
      break;}
    case 'B': {
      if ((scan>start) && (scan[1]=='\0') && (scan[-1]!='M')) s++;
      else {if (*capscan) u8_putc(&out,'B');
	else {u8_putc(&out,'b');}
	s++;}
      break;}
    case 'C': {
      if (strncmp(scan,"CIA",3)==0) {
	if (*capscan) u8_putc(&out,'X'); else u8_putc(&out,'x');
	capscan = capscan+3;
	scan = scan+3;
	continue;}
      else if (strncmp(scan,"CH",2)==0) {
	if (*capscan) u8_putc(&out,'X');
	else u8_putc(&out,'x');
	capscan = capscan+2;
	scan = scan+2; 
	continue;}
      else if ((strncmp(scan,"CI",3)==0) ||
               (strncmp(scan,"CE",2)==0) ||
               (strncmp(scan,"CY",2)==0)) {
	if (*capscan) u8_putc(&out,'S');
	else u8_putc(&out,'s');
	capscan = capscan+2;
	scan = scan+2;
	continue;}
      else if ((scan>start) &&
	       ((strncmp(scan-1,"SCI",3)==0) ||
                (strncmp(scan-1,"SCE",3)==0) ||
                (strncmp(scan-1,"SCY",3)==0))) {
	capscan = capscan+2;
	scan = scan+2;
	continue;}
      else if ((scan>start) && (strncmp(scan-1,"SCH",3)==0)) {
	if (*capscan) u8_putc(&out,'K');
	else u8_putc(&out,'k');
	capscan = capscan+2;
	scan = scan+2;
	continue;}
      else {
	if (*capscan) u8_putc(&out,'K');
	else u8_putc(&out,'k');
	break;}}
    case 'D': {
      if ((strncasecmp(scan,"DGY",3)==0) || 
	  (strncasecmp(scan,"DGI",3)==0) || 
	  (strncasecmp(scan,"DGE",3)==0)) {
	if (*capscan) u8_putc(&out,'J');
	else u8_putc(&out,'j'); 
	capscan = capscan+3;
	scan = scan+3;
	continue;}
      else if (*capscan) u8_putc(&out,'T');
      else u8_putc(&out,'t'); 
      break;}
    case 'G': {
      if ((strncmp(scan,"GN",2)==0) || (strncmp(scan,"GNED",4)==0))
	break;
      else if ((strncmp(scan,"GH",2)==0) && (scan[2]!='\0') && 
	       (strchr(VOWELS,scan[2]) == NULL))
	break;
      else if (((scan[1]=='I') || (scan[1]=='E') || (scan[1]=='Y')) && 
	       (scan>start) && (scan[-1]!='G')) {
	if (*capscan) u8_putc(&out,'J');
	else u8_putc(&out,'j');}
      else if (*capscan) u8_putc(&out,'K');
      else u8_putc(&out,'k'); 
      break;}
    case 'H': {
      if ((scan>start) && (strchr(VOWELS,scan[-1])) && 
	  (strchr(VOWELS,scan[1]) == NULL))
	{}
      else if (*capscan) u8_putc(&out,'H');
      else u8_putc(&out,'h'); 
      break;}
    case 'K': {
      if ((scan>start) && (scan[-1]=='C'))
	{}
      else if (*capscan)
	u8_putc(&out,'K');
      else u8_putc(&out,'k');
      break;}
    case 'P': {
      if (scan[1]=='H') {
	if (*capscan)
	  u8_putc(&out,'F');
	else u8_putc(&out,'f'); 
	capscan = capscan+2;
	scan = scan+2;
	continue;}
      else if (*capscan)
	u8_putc(&out,'P');
      else u8_putc(&out,'p'); 
      break;}
    case 'S': {
      if (scan[1]=='H') {
	if (*capscan) u8_putc(&out,'X');
	else u8_putc(&out,'x');
	capscan = capscan+2;
	scan = scan+2;
	continue;}
      else if (((scan>start) &&
		((strncmp(scan,"sio",3)==0) ||
		 (strncmp(scan,"sia",3)==0))))
	if (*capscan)
	  u8_putc(&out,'X');
	else u8_putc(&out,'x');
      else if (*capscan)
	u8_putc(&out,'S');
      else u8_putc(&out,'s');
      break;}
    case 'T': {
      if ((scan>start) &&
	  ((strncmp(scan,"tia",3)==0) || (strncmp(scan,"tio",3)==0)))
	if (*capscan)
	  u8_putc(&out,'X');
	else u8_putc(&out,'x');
      else if (scan[1]=='H') {
	if (*capscan) u8_putc(&out,'O');
	else u8_putc(&out,'o');
	capscan = capscan+2;
	scan = scan+2;
	continue;}
      else if (*capscan) u8_putc(&out,'T');
      else u8_putc(&out,'t');
      break;}
    case 'W': case 'Y': {
      if (strchr(VOWELS,scan[1])) {
	if (*capscan) lc = *scan;
	else lc = tolower(((int)(*scan)));
	u8_putc(&out,lc);}
      else {}
      break;}
    case 'X': {
      if (*capscan) u8_puts(&out,"KS");
      else u8_puts(&out,"ks");
      break;}
    default: if (scan == start)  {
	if (*capscan) lc = *scan;
	else lc = tolower(((int)(*scan)));
	u8_putc(&out,lc);}}
    if ((sep) && (lenout) && (lenout == (out.u8_write-out.u8_outbuf)) &&
	(out.u8_outbuf[lenout-1]!='.'))
      u8_putc(&out,'.');
    lenout = out.u8_write-out.u8_outbuf;
    capscan++;
    scan++;}
  if (start!=buf) u8_free(start);
  if (capstart!=capbuf) u8_free(capstart);
  return out.u8_outbuf;
}

/* Init (just register) */

void kno_init_phonetic_c()
{
  u8_register_source_file(_FILEINFO);
}

