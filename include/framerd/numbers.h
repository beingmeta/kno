FD_EXPORT fd_exception fd_BigIntException;

FD_EXPORT fdtype fd_make_bigint(long long intval);
FD_EXPORT fdtype fd_init_double(struct FD_FLONUM *ptr,double flonum);
FD_EXPORT fdtype fd_string2number(u8_string string,int base);
FD_EXPORT int fd_output_number(u8_output out,fdtype num,int base);
FD_EXPORT fdtype fd_make_complex(fdtype real,fdtype imag);
FD_EXPORT fdtype fd_make_rational(fdtype num,fdtype denom);
FD_EXPORT int fd_small_bigintp(fd_bigint bi);
FD_EXPORT int fd_modest_bigintp(fd_bigint bi);
FD_EXPORT int fd_builtin_numberp(fdtype x);
FD_EXPORT fdtype fd_plus(fdtype x,fdtype y);
FD_EXPORT fdtype fd_multiply(fdtype x,fdtype y);
FD_EXPORT fdtype fd_subtract(fdtype x,fdtype y);
FD_EXPORT fdtype fd_inexact_divide(fdtype x,fdtype y);
FD_EXPORT fdtype fd_divide(fdtype x,fdtype y);
FD_EXPORT fdtype fd_quotient(fdtype x,fdtype y);
FD_EXPORT fdtype fd_remainder(fdtype x,fdtype y);
FD_EXPORT int fd_numcompare(fdtype x,fdtype y);

FD_EXPORT fdtype fd_gcd(fdtype x,fdtype y);
FD_EXPORT fdtype fd_lcm(fdtype x,fdtype y);

FD_EXPORT fdtype fd_make_exact(fdtype);
FD_EXPORT fdtype fd_make_inexact(fdtype);

FD_EXPORT double fd_todouble(fdtype x);
FD_EXPORT double fd_bigint_to_double(fd_bigint x);

FD_EXPORT fd_bigint fd_long_to_bigint(long x);
FD_EXPORT fd_bigint fd_ulong_to_bigint(unsigned long x);
FD_EXPORT fd_bigint fd_long_long_to_bigint(long long x);
FD_EXPORT fd_bigint fd_ulong_long_to_bigint(unsigned long long x);

FD_EXPORT long fd_bigint_to_long(fd_bigint);
FD_EXPORT unsigned long fd_bigint_to_ulong(fd_bigint);
FD_EXPORT long long fd_bigint_to_long_long(fd_bigint);
FD_EXPORT long long fd_bigint2int64(fd_bigint);
FD_EXPORT unsigned long long fd_bigint_to_ulong_long(fd_bigint);
FD_EXPORT unsigned long long fd_bigint2uint64(fd_bigint);
FD_EXPORT int fd_bigint_negativep(fd_bigint);

FD_EXPORT int fd_bigint_fits_in_word_p(fd_bigint bi,long width,int twosc);
FD_EXPORT unsigned long fd_bigint_bytes(fd_bigint bi);

FD_EXPORT int fd_numcompare(fdtype x,fdtype y);

FD_EXPORT fdtype fd_init_flonum(struct FD_FLONUM *ptr,double flonum);
FD_EXPORT fdtype fd_init_double(struct FD_FLONUM *ptr,double flonum);
#define fd_make_double(dbl) (fd_init_double(NULL,(dbl)))
#define fd_make_flonum(dbl) (fd_init_flonum(NULL,(dbl)))


/* Floating vectors */

typedef struct FD_FLONUM_VECTOR {
  FD_CONS_HEADER;
  unsigned int freedata:1;
  unsigned int length:31;
  double *elts;} FD_FLONUM_VECTOR;
typedef struct FD_FLONUM_VECTOR *fd_float_vector;

FD_EXPORT fdtype fd_make_flonum_vector(int n,double *elts);
FD_EXPORT fdtype fd_init_flonum_vector
  (struct FD_FLONUM_VECTOR *v,int n,double *elts);

#define FD_GET_FLONUMVEC(v) \
  (FD_STRIP_CONS(v,fd_float_vector_type,struct FD_FLONUM_VECTOR *))
#define FD_FLONUMVEC_LENGTH(v) ((FD_GET_FLONUMVEC(v))->length)
#define FD_FLONUMVEC_ELTS(v) ((FD_GET_FLONUMVEC(v))->elts)
#define FD_FLONUMVEC_REF(v,i) ((FD_FLONUMVEC_ELTS(v))[i])

/* Numeric vectors */

struct FD_NUMERIC_VECTOR {
  FD_CONS_HEADER;
  unsigned int freedata:1;
  unsigned int length:31;
  enum fd_num_elt_type { fd_int16, fd_int32, fd_int64, fd_float32, fd_float64 } elt_type;
  union { 
    fd_float *floats;
    fd_double *doubles;
    fd_short *shorts;
    fd_int *ints;
    fd_long *longs;}
    elts;};
typedef struct FD_NUMERIC_VECTOR *fd_numeric_vector;
typedef struct FD_NUMERIC_VECTOR *fd_numvec;

#define FD_NUMERIC_VECTORP(v) (FD_PRIM_TYPEP(v,fd_numeric_vector_type))
#define FD_NUMVECP(v) (FD_PRIM_TYPEP(v,fd_numeric_vector_type))

#define FD_NUMERIC_VECTOR_LENGTH(v) \
  ((FD_STRIP_CONS(v,fd_numeric_vector_type,struct FD_NUMERIC_VECTOR *))->length)
#define FD_NUMVEC_LENGTH(v) \
  ((FD_STRIP_CONS(v,fd_numeric_vector_type,struct FD_NUMERIC_VECTOR *))->length)

#define FD_NUMERIC_VECTOR_TYPE(v) \
  ((FD_STRIP_CONS(v,fd_numeric_vector_type,struct FD_NUMERIC_VECTOR *))->elt_type)
#define FD_NUMVEC_TYPE(v) \
  ((FD_STRIP_CONS(v,fd_numeric_vector_type,struct FD_NUMERIC_VECTOR *))->elt_type)

#define FD_NUMERIC_VECTOR(v) \
  (FD_STRIP_CONS(v,fd_numeric_vector_type,struct FD_NUMERIC_VECTOR *))

#define FD_NUMVEC_FLOATS(v) \
  (((((fd_numvec)v)->elt_type)==fd_float32)?(((fd_numvec)v)->elts.floats):(NULL))
#define FD_NUMVEC_DOUBLES(v) \
  (((((fd_numvec)v)->elt_type)==fd_float64)?(((fd_numvec)v)->elts.doubles):(NULL))
#define FD_NUMVEC_SHORTS(v) \
  (((((fd_numvec)v)->elt_type)==fd_int16)?(((fd_numvec)v)->elts.shorts):(NULL))
#define FD_NUMVEC_INTS(v) \
  (((((fd_numvec)v)->elt_type)==fd_int32)?(((fd_numvec)v)->elts.ints):(NULL))
#define FD_NUMVEC_LONGS(v) \
  (((((fd_numvec)v)->elt_type)==fd_int64)?(((fd_numvec)v)->elts.longs):(NULL))

#define FD_NUMVEC_FLOAT(v,i) (((fd_numvec)v)->elts.floats[i])
#define FD_NUMVEC_DOUBLE(v,i) (((fd_numvec)v)->elts.doubles[i])
#define FD_NUMVEC_SHORT(v,i) (((fd_numvec)v)->elts.shorts[i])
#define FD_NUMVEC_INT(v,i) (((fd_numvec)v)->elts.ints[i])
#define FD_NUMVEC_LONG(v,i) (((fd_numvec)v)->elts.longs[i])

#define FD_NUMVEC_FLOAT_SLICE(v,i) (((fd_numvec)v)->elts.floats+i)
#define FD_NUMVEC_DOUBLE_SLICE(v,i) (((fd_numvec)v)->elts.doubles+i)
#define FD_NUMVEC_SHORT_SLICE(v,i) (((fd_numvec)v)->elts.shorts+i)
#define FD_NUMVEC_INT_SLICE(v,i) (((fd_numvec)v)->elts.ints+i)
#define FD_NUMVEC_LONG_SLICE(v,i) (((fd_numvec)v)->elts.longs+i)

FD_EXPORT fdtype fd_make_long_vector(int n,fd_long *v);
FD_EXPORT fdtype fd_make_int_vector(int n,fd_int *v);
FD_EXPORT fdtype fd_make_short_vector(int n,fd_short *v);
FD_EXPORT fdtype fd_make_float_vector(int n,fd_float *v);
FD_EXPORT fdtype fd_make_double_vector(int n,fd_double *v);

FD_EXPORT fdtype fd_make_numeric_vector(int n,enum fd_num_elt_type t);

