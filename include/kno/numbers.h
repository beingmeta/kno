KNO_EXPORT u8_condition kno_BigIntException;
KNO_EXPORT u8_condition kno_NotANumber;
KNO_EXPORT u8_condition kno_DivideByZero;
KNO_EXPORT u8_condition kno_InvalidNumericLiteral;

KNO_EXPORT lispval kno_max_fixnum;
KNO_EXPORT lispval kno_min_fixnum;

KNO_EXPORT lispval kno_make_bigint(long long intval);
KNO_EXPORT lispval kno_init_double(struct KNO_FLONUM *ptr,double flonum);
KNO_EXPORT lispval kno_string2number(u8_string string,int base);
KNO_EXPORT int kno_output_number(u8_output out,lispval num,int base);
KNO_EXPORT lispval kno_make_complex(lispval real,lispval imag);
KNO_EXPORT lispval kno_make_rational(lispval num,lispval denom);
KNO_EXPORT int kno_small_bigintp(kno_bigint bi);
KNO_EXPORT int kno_modest_bigintp(kno_bigint bi);
KNO_EXPORT int kno_builtin_numberp(lispval x);
KNO_EXPORT lispval kno_plus(lispval x,lispval y);
KNO_EXPORT lispval kno_multiply(lispval x,lispval y);
KNO_EXPORT lispval kno_subtract(lispval x,lispval y);
KNO_EXPORT lispval kno_inexact_divide(lispval x,lispval y);
KNO_EXPORT lispval kno_divide(lispval x,lispval y);
KNO_EXPORT lispval kno_quotient(lispval x,lispval y);
KNO_EXPORT lispval kno_remainder(lispval x,lispval y);
KNO_EXPORT int kno_numcompare(lispval x,lispval y);

KNO_EXPORT lispval kno_gcd(lispval x,lispval y);
KNO_EXPORT lispval kno_lcm(lispval x,lispval y);

KNO_EXPORT lispval kno_pow(lispval x,lispval y);

KNO_EXPORT lispval kno_make_exact(lispval);
KNO_EXPORT lispval kno_make_inexact(lispval);

KNO_EXPORT double kno_todouble(lispval x);
KNO_EXPORT double kno_bigint_to_double(kno_bigint x);

KNO_EXPORT kno_bigint kno_long_to_bigint(long x);
KNO_EXPORT kno_bigint kno_ulong_to_bigint(unsigned long x);
KNO_EXPORT kno_bigint kno_long_long_to_bigint(long long x);
KNO_EXPORT kno_bigint kno_ulong_long_to_bigint(unsigned long long x);

KNO_EXPORT long kno_bigint_to_long(kno_bigint);
KNO_EXPORT unsigned long kno_bigint_to_ulong(kno_bigint);
KNO_EXPORT long long kno_bigint_to_long_long(kno_bigint);
KNO_EXPORT long long kno_bigint2int64(kno_bigint);
KNO_EXPORT unsigned long long kno_bigint_to_ulong_long(kno_bigint);
KNO_EXPORT unsigned long long kno_bigint2uint64(kno_bigint);
KNO_EXPORT int kno_bigint_negativep(kno_bigint);

KNO_EXPORT int kno_bigint_fits_in_word_p(kno_bigint bi,long width,int twosc);
KNO_EXPORT unsigned long kno_bigint_bytes(kno_bigint bi);

KNO_EXPORT kno_bigint kno_bigint_multiply(kno_bigint bx,kno_bigint by);
KNO_EXPORT kno_bigint kno_bigint_substract(kno_bigint bx,kno_bigint by);
KNO_EXPORT kno_bigint kno_bigint_add(kno_bigint bx,kno_bigint by);



KNO_EXPORT int kno_numcompare(lispval x,lispval y);

KNO_EXPORT lispval kno_init_flonum(struct KNO_FLONUM *ptr,double flonum);
KNO_EXPORT lispval kno_init_double(struct KNO_FLONUM *ptr,double flonum);
#define kno_make_double(dbl) (kno_init_double(NULL,(dbl)))
#define kno_make_flonum(dbl) (kno_init_flonum(NULL,(dbl)))

KNO_EXPORT int kno_exactp(lispval x);

#define KNO_ISNAN(x) ((KNO_FLONUMP(x))&&(isnan(KNO_FLONUM(x))))
#define KNO_ZEROP(x) ((KNO_FIXNUMP(x))?((KNO_FIX2INT(x))==0): \
		     (KNO_FLONUMP(x))?((KNO_FLONUM(x))==0):(0))
#define KNO_EXACTP(x)					      \
  ((KNO_FLONUMP(x))?(0):(!(KNO_NUMBERP(x)))?(-1):		      \
   ((KNO_FIXNUMP(x))||(KNO_BIGINTP(x)))?(1):		      \
   (kno_exactp(x)))

#define KNO_POSINTP(x) \
  ( (KNO_FIXNUMP(x)) ? ( (KNO_FIX2INT(x)) > 0 ) : \
    ( (KNO_BIGINTP(x)) && (!(kno_bigint_negativep(x)))) )

KNO_EXPORT int kno_tolonglong(lispval r,long long *intval);

/* Numeric vectors */

KNO_EXPORT int kno_numvec_showmax;

struct KNO_NUMERIC_VECTOR {
  KNO_CONS_HEADER;
  unsigned int numvec_free_elts:1;
  unsigned int numvec_length:31;
  enum kno_num_elt_type {
	  kno_short_elt, kno_int_elt, kno_long_elt,
	  kno_float_elt, kno_double_elt }
    numvec_elt_type;
  union { 
    kno_float *floats;
    kno_double *doubles;
    kno_short *shorts;
    kno_int *ints;
    kno_long *longs;}
    numvec_elts;};
typedef struct KNO_NUMERIC_VECTOR *kno_numeric_vector;
typedef struct KNO_NUMERIC_VECTOR *kno_numvec;

#define KNO_NUMERIC_VECTORP(v) (KNO_TYPEP(v,kno_numeric_vector_type))
#define KNO_XNUMVEC(v) ((kno_numvec)(v))
#define KNO_NUMVECP(v) (KNO_TYPEP(v,kno_numeric_vector_type))

#define KNO_NUMERIC_VECTOR_LENGTH(v) ((KNO_XNUMVEC(v))->numvec_length)
#define KNO_NUMVEC_LENGTH(v) ((KNO_XNUMVEC(v))->numvec_length)

#define KNO_NUMERIC_VECTOR_TYPE(v) ((KNO_XNUMVEC(v))->numvec_elt_type)
#define KNO_NUMVEC_TYPE(v) ((KNO_XNUMVEC(v))->numvec_elt_type)
#define KNO_NUMVEC_TYPEP(v,t) (((KNO_XNUMVEC(v))->numvec_elt_type) == t)

#define KNO_NUMERIC_VECTOR(v) \
  (kno_consptr(struct KNO_NUMERIC_VECTOR *,v,kno_numeric_vector_type))
#define KNO_NUMVEC(v) \
  (kno_consptr(struct KNO_NUMERIC_VECTOR *,v,kno_numeric_vector_type))
#define KNO_NUMVEC_ELTS(nv,field) (((kno_numvec)nv)->numvec_elts.field)

#define KNO_NUMVEC_FLOATS(v)		      \
  ((KNO_NUMVEC_TYPEP(v,kno_float_elt))?(KNO_NUMVEC_ELTS(v,floats)):(NULL))
#define KNO_NUMVEC_DOUBLES(v) \
  ((KNO_NUMVEC_TYPEP(v,kno_double_elt))?(KNO_NUMVEC_ELTS(v,doubles)):(NULL))
#define KNO_NUMVEC_SHORTS(v) \
  ((KNO_NUMVEC_TYPEP(v,kno_short_elt))?(KNO_NUMVEC_ELTS(v,shorts)):(NULL))
#define KNO_NUMVEC_INTS(v) \
  ((KNO_NUMVEC_TYPEP(v,kno_int_elt))?(KNO_NUMVEC_ELTS(v,ints)):(NULL))
#define KNO_NUMVEC_LONGS(v) \
  ((KNO_NUMVEC_TYPEP(v,kno_long_elt))?(KNO_NUMVEC_ELTS(v,longs)):(NULL))

#define KNO_NUMVEC_FLOAT(v,i)  ((KNO_NUMVEC_FLOATS(v))[i])
#define KNO_NUMVEC_DOUBLE(v,i) ((KNO_NUMVEC_DOUBLES(v))[i])
#define KNO_NUMVEC_SHORT(v,i)  ((KNO_NUMVEC_SHORTS(v))[i])
#define KNO_NUMVEC_INT(v,i)    ((KNO_NUMVEC_INTS(v))[i])
#define KNO_NUMVEC_LONG(v,i)   ((KNO_NUMVEC_LONGS(v))[i])

#define KNO_NUMVEC_FLOAT_SLICE(v,i)  ((KNO_NUMVEC_FLOATS(v))+i)
#define KNO_NUMVEC_DOUBLE_SLICE(v,i) ((KNO_NUMVEC_DOUBLES(v))+i)
#define KNO_NUMVEC_SHORT_SLICE(v,i)  ((KNO_NUMVEC_SHORTS(v))+i)
#define KNO_NUMVEC_INT_SLICE(v,i)    ((KNO_NUMVEC_INTS(v))+i)
#define KNO_NUMVEC_LONG_SLICE(v,i)   ((KNO_NUMVEC_LONGS(v))+i)

KNO_EXPORT lispval kno_make_long_vector(int n,kno_long *v);
KNO_EXPORT lispval kno_make_int_vector(int n,kno_int *v);
KNO_EXPORT lispval kno_make_short_vector(int n,kno_short *v);
KNO_EXPORT lispval kno_make_float_vector(int n,kno_float *v);
KNO_EXPORT lispval kno_make_double_vector(int n,kno_double *v);

KNO_EXPORT lispval kno_make_numeric_vector(int n,enum kno_num_elt_type t);

