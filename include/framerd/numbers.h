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

FD_EXPORT fdtype fd_pow(fdtype x,fdtype y);

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

FD_EXPORT int fd_exactp(fdtype x);

#define FD_ISNAN(x) ((FD_FLONUMP(x))&&(isnan(FD_FLONUM(x))))
#define FD_ZEROP(x) ((FD_FIXNUMP(x))?((FD_FIX2INT(x))==0): \
		     (FD_FLONUMP(x))?((FD_FLONUM(x))==0):(0))
#define FD_EXACTP(x)					      \
  ((FD_FLONUMP(x))?(0):(!(FD_NUMBERP(x)))?(-1):		      \
   ((FD_FIXNUMP(x))||(FD_BIGINTP(x)))?(1):		      \
   (fd_exactp(x)))

FD_EXPORT int fd_tolonglong(fdtype r,long long *intval);

/* Numeric vectors */

FD_EXPORT int fd_numvec_showmax;

struct FD_NUMERIC_VECTOR {
  FD_CONS_HEADER;
  unsigned int fd_freedata:1;
  unsigned int fd_numvec_len:31;
  enum fd_num_elt_type {
	  fd_short_elt, fd_int_elt, fd_long_elt,
	  fd_float_elt, fd_double_elt }
    fd_numvec_elt_type;
  union { 
    fd_float *floats;
    fd_double *doubles;
    fd_short *shorts;
    fd_int *ints;
    fd_long *longs;}
    fd_numvec_elts;};
typedef struct FD_NUMERIC_VECTOR *fd_numeric_vector;
typedef struct FD_NUMERIC_VECTOR *fd_numvec;

#define FD_NUMERIC_VECTORP(v) (FD_TYPEP(v,fd_numeric_vector_type))
#define FD_XNUMVEC(v) ((fd_numvec)(v))
#define FD_NUMVECP(v) (FD_TYPEP(v,fd_numeric_vector_type))

#define FD_NUMERIC_VECTOR_LENGTH(v) ((FD_XNUMVEC(v))->fd_numvec_len)
#define FD_NUMVEC_LENGTH(v) ((FD_XNUMVEC(v))->fd_numvec_len)

#define FD_NUMERIC_VECTOR_TYPE(v) ((FD_XNUMVEC(v))->fd_numvec_elt_type)
#define FD_NUMVEC_TYPE(v) ((FD_XNUMVEC(v))->fd_numvec_elt_type)
#define FD_NUMVEC_TYPEP(v,t) (((FD_XNUMVEC(v))->fd_numvec_elt_type)==t)

#define FD_NUMERIC_VECTOR(v) \
  (fd_consptr(struct FD_NUMERIC_VECTOR *,v,fd_numeric_vector_type))
#define FD_NUMVEC(v) \
  (fd_consptr(struct FD_NUMERIC_VECTOR *,v,fd_numeric_vector_type))
#define FD_NUMVEC_ELTS(nv,field) (((fd_numvec)nv)->fd_numvec_elts.field)

#define FD_NUMVEC_FLOATS(v)		      \
  ((FD_NUMVEC_TYPEP(v,fd_float_elt))?(FD_NUMVEC_ELTS(v,floats)):(NULL))
#define FD_NUMVEC_DOUBLES(v) \
  ((FD_NUMVEC_TYPEP(v,fd_double_elt))?(FD_NUMVEC_ELTS(v,doubles)):(NULL))
#define FD_NUMVEC_SHORTS(v) \
  ((FD_NUMVEC_TYPEP(v,fd_short_elt))?(FD_NUMVEC_ELTS(v,shorts)):(NULL))
#define FD_NUMVEC_INTS(v) \
  ((FD_NUMVEC_TYPEP(v,fd_int_elt))?(FD_NUMVEC_ELTS(v,ints)):(NULL))
#define FD_NUMVEC_LONGS(v) \
  ((FD_NUMVEC_TYPEP(v,fd_long_elt))?(FD_NUMVEC_ELTS(v,longs)):(NULL))

#define FD_NUMVEC_FLOAT(v,i)  ((FD_NUMVEC_FLOATS(v))[i])
#define FD_NUMVEC_DOUBLE(v,i) ((FD_NUMVEC_DOUBLES(v))[i])
#define FD_NUMVEC_SHORT(v,i)  ((FD_NUMVEC_SHORTS(v))[i])
#define FD_NUMVEC_INT(v,i)    ((FD_NUMVEC_INTS(v))[i])
#define FD_NUMVEC_LONG(v,i)   ((FD_NUMVEC_LONGS(v))[i])

#define FD_NUMVEC_FLOAT_SLICE(v,i)  ((FD_NUMVEC_FLOATS(v))+i)
#define FD_NUMVEC_DOUBLE_SLICE(v,i) ((FD_NUMVEC_DOUBLES(v))+i)
#define FD_NUMVEC_SHORT_SLICE(v,i)  ((FD_NUMVEC_SHORTS(v))+i)
#define FD_NUMVEC_INT_SLICE(v,i)    ((FD_NUMVEC_INTS(v))+i)
#define FD_NUMVEC_LONG_SLICE(v,i)   ((FD_NUMVEC_LONGS(v))+i)

FD_EXPORT fdtype fd_make_long_vector(int n,fd_long *v);
FD_EXPORT fdtype fd_make_int_vector(int n,fd_int *v);
FD_EXPORT fdtype fd_make_short_vector(int n,fd_short *v);
FD_EXPORT fdtype fd_make_float_vector(int n,fd_float *v);
FD_EXPORT fdtype fd_make_double_vector(int n,fd_double *v);

FD_EXPORT fdtype fd_make_numeric_vector(int n,enum fd_num_elt_type t);

