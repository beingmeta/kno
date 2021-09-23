#ifndef KNO_EVAL_INTERNALS
#define KNO_EVAL_INTERNALS 0
#endif

#ifndef INIT_ARGBUF_LEN
#define INIT_ARGBUF_LEN 7
#endif

#if KNO_EVAL_INTERNALS
#define KNO_INLINE_CHECKTYPE    (!(KNO_AVOID_INLINE))
#define KNO_INLINE_CHOICES      (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_QONSTS	(!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV       (!(KNO_AVOID_INLINE))
#endif


