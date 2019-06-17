(load "common.scm")

(use-module '{zeromq crypto packetfns})

(define (test-addr addr (listen #f))
  (let ((client (zmq/open addr 'request))
	(server (zmq/listen (or listen addr) 'reply))
	(send #"hello")
	(received #f))
    (applytest #t zeromq? client)
    (applytest #t zeromq? server)
    (applytest #f zeromq? addr)
    (applytest #f zeromq? addr 'poll)
    (applytest #t zeromq? client 'request)
    (applytest #t zeromq? server 'reply)
    (applytest 'request zmq/type client)
    (applytest 'reply zmq/type server)
    (zmq/send! client send)
    (applytest #"hello" zmq/recv server)))

(define (test-send/recv)
  (test-addr "inproc://build-test")
  (test-addr "tcp://localhost:9999" "tcp://*:9999"))

(define (serverfn server (fn #f) (running #t))
  (when (string? server) 
    (set! server (zmq/listen server 'reply)))
  (while running
    (let ((request (zmq/recv server)))
      (if (and (packet? request) (= (length request) 1)
	       (zero? (elt request 0)))
	  (begin (zmq/send! server request)
	    (set! running #f))
	  (if fn
	      (let ((response (fn request)))
		(zmq/send! server response))
	      (zmq/send! server request))))))

(define (sendmsg msg (client "tcp://localhost:9999") (fn #f))
  (when (string? client) (set! client (zmq/open client 'request)))
  (zmq/send! client msg)
  (let ((result (zmq/recv client)))
    (if fn (fn result) result)))

(define (client-loop addr)
  (let ((client (zmq/open addr 'request)))
    (dotimes (i 100)
      (let ((msg (random-packet 32)))
	(zmq/send! client msg)
	(applytest msg (zmq/recv client))))))

(define (test-simple-server (addr "tcp://localhost:8888") (listen))
  (default! listen (string-subst addr "//localhost:" "//127.0.0.1:"))
  (let* ((server (thread/call serverfn listen #f))
	 (clients {}))
    (dotimes (i 8)
      (set+! clients (thread/call client-loop addr)))
    (lognotice |StartedClients|
      "Waiting for " (choice-size clients) " to finish:"
      (do-choices (client clients)
	(printout "\n    " client)))
    (let ((joined (thread/join clients)))
      (applytest #t (thread/finished? joined))
      (let ((shutdown-socket (zmq/open addr 'request)))
	(zmq/send! shutdown-socket #X"00"))
      (sleep 1)
      (applytest #t thread/finished? server))))


(define (main)
  (test-send/recv)
  (test-simple-server))

