#include "ptr.h"

#define N_OID_INITS 4

static struct OID_INIT {
  unsigned int hi, lo, cap;} _kno_oid_inits[N_OID_INITS]={
  {0x1,0x0,300000},
  {0x1,0x1000000,600000},
  {0x1,0x2000000,4100000},
  {0x1,0x3000000,2500000}};



