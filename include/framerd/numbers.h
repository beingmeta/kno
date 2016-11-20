FD_EXPORT fd_exception fd_BigIntException;

FD_EXPORT fdtype fd_make_bigint(long long intval);
FD_EXPORT fdtype fd_init_double(struct FD_DOUBLE *ptr,double flonum);
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

FD_EXPORT int fd_bigint_fits(fd_bigint bi,int width,int twoc);
FD_EXPORT unsigned long fd_bigint_bytes(fd_bigint bi);

FD_EXPORT int fd_numcompare(fdtype x,fdtype y);

#define fd_make_double(dbl) (fd_init_double(NULL,(dbl)))


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
