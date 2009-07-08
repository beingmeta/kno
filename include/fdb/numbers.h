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

FD_EXPORT int fd_numcompare(fdtype x,fdtype y);

#define fd_make_double(dbl) (fd_init_double(NULL,(dbl)))


