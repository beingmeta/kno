(define testexpr (config 'TESTEXPR '(PLUS 11 99)))

(define (evaln server n)
  (let ((s (open-evalserver server)))
    (dotimes (i n) (neteval s (or testexpr i)))))

(define (evalmn server m n)
  (let ((start (elapsed-time))
	(threads {}))
    (dotimes (i m) (set+! threads (spawn (evaln server n))))
    (thread/join threads)
    (/ (* m n) (- (elapsed-time) start))))

