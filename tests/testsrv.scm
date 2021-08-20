(define testop '+)
(define test-args '(11 99))

(define (evaln server n)
  (let ((s (open-service server)))
    (dotimes (i n) (apply sevice/call s testop test-args))))

(define (calln np n)
  (dotimes (i n) (apply np test-args)))

(define (evalmn server m n)
  (let* ((s (open-service server))
	 (nplus (netproc s testop)))
    (let ((start (elapsed-time))
	  (threads {}))
      (dotimes (i m) (set+! threads (spawn (evaln server n))))
      (thread/join threads)
      (/ (* m n) (- (elapsed-time) start)))))


