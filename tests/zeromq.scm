(load "common.scm")

(use-module 'zeromq)

(define (test-addr addr (listen #f))
  (let ((client (zmq/open addr 'request))
	(server (zmq/listen (or listen addr) 'reply))
	(send #"hello")
	(received #f))
    (zmq/send! client send)
    (applytest #"hello" zmq/recv server)))

(define (main)
  (test-addr "inproc://build-test")
  (test-addr "tcp://localhost:9999" "tcp://*:9999"))





