#include <kno/lisp.h>
#include <zmq.h>

typedef enum zmq_type { zmq_socket_type } zmq_type;

typedef struct KNO_ZEROMQ {
  KNO_CONS_HEADER;
  u8_string zmq_id;
  pthread_t zmq_threadid;
  enum zmq_type zmq_type;
  unsigned int zmq_flags;
  void *zmq_ptr;} KNO_ZEROMQ;
typedef struct KNO_ZEROMQ *kno_zeromq;

KNO_EXPORT kno_ptr_type kno_zeromq_type;

KNO_EXPORT void *kno_zeromq_ctx;
KNO_EXPORT void *kno_init_zeromq_ctx(void);

#define KNO_ZMQ_CTX \
  ( (kno_zeromq_ctx) ? (kno_zeromq_ctx) : (kno_init_zeromq_ctx()))
