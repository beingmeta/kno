#include <kno/lisp.h>
#include <zmq.h>

KNO_EXPORT kno_lisp_type kno_zmqsock_type;

typedef struct KNO_ZMQSOCK {
  KNO_CONS_HEADER;
  u8_string zmq_id;
  lispval zmq_socktype;
  unsigned int zmq_flags;
  pthread_t zmq_thread;
  int zmq_thread_off; /* Offset in thread table */
  void *zmq_ptr;} KNO_ZMQSOCK;
typedef struct KNO_ZMQSOCK *kno_zmqsock;

#define ZMQ_SOCKET_CLOSED 2
#define ZMQ_SOCKPTR(x) ( ( (kno_zmqsock) (x)) -> zmq_ptr)

/* One global context */
KNO_EXPORT void *kno_zeromq_ctx;
KNO_EXPORT void *kno_init_zeromq_ctx(void);
#define KNO_ZMQ_CTX \
  ( (kno_zeromq_ctx) ? (kno_zeromq_ctx) : (kno_init_zeromq_ctx()))

/* Data for each thread to keep track of sockets in that thread. */
typedef struct KNO_ZMQ_THREAD_DATA {
  pthread_t data_for_thread;
  ssize_t sockets_len, last_socket, open_socket;
  struct KNO_ZMQSOCK *zmq_socket0;}
  *kno_zmq_thread_data;

#define KNO_ZMQ_SOCK_REF(s,i) ((&(s->zmq_socket0))[i])
#define KNO_ZMQ_SOCKETS(s)    (&(s->zmq_socket0))

#if KNO_USE__THREAD
KNO_EXPORT __thread kno_zmq_thread_socks kno_zeromq_thread_data;
#else
KNO_EXPORT u8_tld_key kno_zeromq_thread_data_key;
#endif

KNO_EXPORT kno_zmq_thread_data kno_thread_zperthread(void);

