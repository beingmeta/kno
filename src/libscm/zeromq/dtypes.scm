;;; -*- Mode: Scheme; -*-

(in-module 'zeromq/dtypes)

(use-module '{zeromq logger varconfig})

(module-export! '{zmq/evalserver zmq/dtclient zmq/dteval})

(define (zmq/evalserver addr)
  (let* ((socket (zmq/listen addr 'reply))
	 (packet (zmq/recv socket)))
    (while packet
      (let* ((expr (packet->dtype packet))
	     (result (eval expr)))
	(zmq/send! socket (dtype->packet result) ))
      (set! packet (zmq/recv socket)))))

(define (zmq/dtclient addr)
  (zmq/open addr 'request))

(define (zmq/dteval expr client)
  (zmq/send! client (dtype->packet expr))
  (packet->dtype (zmq/recv client)))
