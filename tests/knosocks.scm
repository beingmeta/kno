(use-module '{knosocks samplefns logger})

(define (test-with-socket socket)
  (let* ((server (knosockd/listener socket
				    [xrefs #(square fib fact)
				     initclients 3
				     maxclients 8
				     nthreads 2
				     id "testing"]
				    [square square fib fibi fact facti]))
	 (thread (thread/call knosockd/run server))
	 (client (open-service socket))
	 (waitcount 200))
    (applytest 120 service/call client 'fact 5)
    (applytest 6765 service/call client 'fib 20)
    (thread/join {(spawn (dotimes (i 4) (applytest 120 service/call client 'fact 5)))
    		  (spawn (dotimes (i 4) (applytest 6765  service/call client 'fib 20)))
    		  (spawn (dotimes (i 4) (applytest 6765 service/call client 'fib 20)))
    		  (spawn (dotimes (i 4) (applytest 120 service/call client 'fact 5)))})
    (dotimes (i 20) (applytest 6765 service/call client 'fib 20))
    (knosockd/shutdown! server)
    (while (and (not (thread/exited? thread)) (>= waitcount 0))
      (sleep 0.01)
      (set! waitcount (-1+ waitcount)))
    (if (thread/exited? thread)
	(lognotice |Finished| 
	  "The thread " thread " exited (as expected) when its server was shut down")
	(logwarn |ZombieAlert| 
	  "The thread " thread " didn't exit when its server was shut down"))
    thread))

(define (main (addr "localhost:8888"))
  (test-with-socket addr))
