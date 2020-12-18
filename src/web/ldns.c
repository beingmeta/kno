/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/eval.h"
#include "kno/webtools.h"
#include "kno/ports.h"
#include "kno/cprims.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8netfns.h>

#include <ldns/ldns.h>


static int dns_initialized = 0;

#ifndef NSBUF_SIZE
#define NSBUF_SIZE 4096
#endif

static lispval rdf2lisp ( ldns_rdf *field )
{
  size_t size = field->_size;
  void *data = ldns_rdf_data( field );
  enum ldns_enum_rdf_type field_type = field->_type;
  switch (field_type) {
  case LDNS_RDF_TYPE_STR: {
    unsigned char *cdata = (unsigned char *) data;
    return kno_make_string(NULL,cdata[0],cdata+1);}
  case LDNS_RDF_TYPE_TIME: {
    time_t tick = ldns_rdf2native_time_t( field );
    return kno_time2timestamp(tick);}
  case LDNS_RDF_TYPE_INT8: {
    long long val = ldns_rdf2native_int8( field );
    return KNO_INT2LISP( val );}
  case LDNS_RDF_TYPE_INT16: {
    long long val = ldns_rdf2native_int16( field );
    return KNO_INT2LISP( val );}
  case LDNS_RDF_TYPE_INT32: {
    long long val = ldns_rdf2native_int32( field );
    return KNO_INT2LISP( val );}
  case LDNS_RDF_TYPE_DNAME: case LDNS_RDF_TYPE_A: case LDNS_RDF_TYPE_AAAA:
  case LDNS_RDF_TYPE_LOC: case LDNS_RDF_TYPE_TAG: case LDNS_RDF_TYPE_LONG_STR: {
    lispval result = VOID;
    ldns_buffer *tmp = ldns_buffer_new( LDNS_MAX_PACKETLEN );
    int rv = -1;
    if (!(tmp)) {}
    else if (field_type == LDNS_RDF_TYPE_DNAME)
      rv = ldns_rdf2buffer_str_dname( tmp, field );
    else if (field_type == LDNS_RDF_TYPE_A)
      rv = ldns_rdf2buffer_str_a( tmp, field );
    else if (field_type == LDNS_RDF_TYPE_AAAA)
      rv = ldns_rdf2buffer_str_aaaa( tmp, field );
    else if (field_type == LDNS_RDF_TYPE_LOC)
      rv = ldns_rdf2buffer_str_loc( tmp, field );
    else if (field_type == LDNS_RDF_TYPE_TAG)
      rv = ldns_rdf2buffer_str_tag( tmp, field );
    else if (field_type == LDNS_RDF_TYPE_LONG_STR)
      rv = ldns_rdf2buffer_str_long_str( tmp, field );
    else {}
    if (rv != LDNS_STATUS_OK)
      result = kno_err("Unexpected LDNS condition","rdf2lisp",NULL,VOID);
    else result = kno_make_string(NULL,tmp->_position,tmp->_data);
    ldns_buffer_free( tmp );
    return result;}
  default:
    return kno_make_packet(NULL,size,(unsigned char *) data);
  }
}

DEFCPRIM("dns/get",dns_query,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(DNS/GET *arg0* [*arg1*])` "
	 "**undocumented**",
	 {"domain_arg",kno_string_type,KNO_VOID},
	 {"type_arg",kno_symbol_type,KNO_VOID})
static lispval dns_query(lispval domain_arg,lispval type_arg)
{
  lispval results = EMPTY;
  ldns_resolver *res;
  ldns_rr_type rr_type = ldns_get_rr_type_by_name( SYM_NAME( type_arg ) );
  ldns_rdf *domain = ldns_dname_new_frm_str( CSTRING(domain_arg) );
  ldns_status s = ldns_resolver_new_frm_file( &res, NULL );
  ldns_pkt *p =
    (s == LDNS_STATUS_OK) ?
    (ldns_resolver_query ( res, domain, rr_type, LDNS_RR_CLASS_IN, LDNS_RD )) :
    (NULL);

  if (!(p)) {}
  else {
    ldns_rr_list *result_list =
      ldns_pkt_rr_list_by_type( p, rr_type, LDNS_SECTION_ANSWER );
    if (!(result_list)) {}
    else {
      size_t i = 0, lim = result_list->_rr_count;
      ldns_rr **records = result_list->_rrs;
      while ( i < lim ) {
	ldns_rr *record = records[i++];
	size_t n_fields = record->_rd_count;
	ldns_rdf **fields = record->_rdata_fields;
	if (n_fields == 0)  {} /* does this ever happen? */
	else if (n_fields == 1)  {
	  lispval value = rdf2lisp( fields[0] );
	  CHOICE_ADD(results,value);}
	else {
	  lispval vec = kno_empty_vector(n_fields);
	  int j = 0; while (j < n_fields) {
	    ldns_rdf *field = fields[j];
	    lispval value = rdf2lisp( field );
	    KNO_VECTOR_SET( vec, j, value);
	    j++;}
	  CHOICE_ADD(results,vec);}}
      ldns_rr_list_deep_free( result_list );}}

  ldns_rdf_deep_free( domain );
  ldns_resolver_deep_free( res );
  if (p) ldns_pkt_free( p );

  return results;
}

KNO_EXPORT void kno_init_dns_c(void) KNO_LIBINIT_FN;

static lispval webtools_module;

KNO_EXPORT void kno_init_dns_c()
{
  if (dns_initialized) return;
  dns_initialized = 1;
  kno_init_scheme();

  webtools_module = kno_new_module("WEBTOOLS",(0));

  link_local_cprims();

  u8_register_source_file(_FILEINFO);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("dns/get",dns_query,2,webtools_module);
}
