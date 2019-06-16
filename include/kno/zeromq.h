#include <kno/lisp.h>
#include <zmq.h>

typedef enum zmq_type { zmq_socket_type } zmq_type;

typedef struct KNO_ZEROMQ {
  KNO_CONS_HEADER;
  u8_string zmq_id;
  enum zmq_type zmq_type;
  pthread_t zmq_thread;
  int zmq_off; /* Offset in thread table */
  unsigned int zmq_flags;
  void *zmq_ptr;} KNO_ZEROMQ;
typedef struct KNO_ZEROMQ *kno_zeromq;

#define ZMQ_SOCKET_CLOSED 2

KNO_EXPORT kno_ptr_type kno_zeromq_type;

KNO_EXPORT void *kno_zeromq_ctx;
KNO_EXPORT void *kno_init_zeromq_ctx(void);

typedef struct KNO_ZMQ_THREAD_DATA {
  pthread_t data_thread;
  ssize_t sockets_len, sockets_end, open_socket;
  struct KNO_ZEROMQ *zmq_socket0;}
  *kno_zmq_thread_data;

#define ZMQ_SOCK_REF(s,i) ((&(s->zmq_socket0))[i])

#if KNO_USE__THREAD
KNO_EXPORT __thread kno_zmq_thread_socks kno_zeromq_thread_data;
#else
KNO_EXPORT u8_tld_key kno_zeromq_thread_data_key;
#endif

KNO_EXPORT kno_zmq_thread_data kno_thread_zperthread(void);

#define KNO_ZMQ_CTX \
  ( (kno_zeromq_ctx) ? (kno_zeromq_ctx) : (kno_init_zeromq_ctx()))
