(load-component "common.scm")

(define numbers '())
(define addnumber
  (slambda (n) (set! numbers (cons n numbers))))
(define sleep-base 0.01)
(define (addrange start (len 10))
  (dotimes (i len)
    (addnumber (+ start i))
    (sleep (* (1+ (random 5)) sleep-base))))

(define (check-ordered list)
  (cond ((or (null? list) (null? (cdr list))) #t)
	((< (car list) (cadr list))
	 (check-ordered (cdr list)))
	(else #f)))

(define (test-parallel)
  (set! numbers '())
  (parallel (addrange 0) (addrange 10))
  (applytest 20 length numbers)
  (applytest #f check-ordered numbers)
  (message "TEST-PARALLEL: " numbers))

(define (test-spawn)
  (let ((threads {}))
    (set! numbers '())
    (set+! threads (spawn (addrange 10)))
    (set+! threads (spawn (addrange 0)))
    (threadjoin threads)
    (applytest 20 length numbers)
    (applytest #f check-ordered numbers)
    (message "TEST-SPAWN: " numbers)))

(define (test-threadcall)
  (let ((threads {}))
    (set! numbers '())
    (set+! threads (threadcall addrange 10))
    (set+! threads (threadcall addrange 0))
    (threadjoin threads)
    (applytest 20 length numbers)
    (applytest #f check-ordered numbers)
    (message "TEST-SPAWN: " numbers)))


;;;; CONDVAR testing

(define cvar (make-condvar))
(define touches 0)
(define sightings 0)
(define noisy (config 'NOISY #f))

(define var 33)
(define (watchv i)
  (while var
    (condvar-lock cvar)
    (if (if (zero? (remainder i 2))
	    (condvar-wait cvar (+ 2 (random 4)))
	    (condvar-wait cvar))
	(begin (when noisy
		 (if var
		     (message " [" i "] Seen " var)
		     (message " [" i "] Exiting test")))
	       (when var (set! sightings (1+ sightings)))
	       (condvar-unlock cvar))
	(begin (when noisy
		 (if var (message " [" i "] Just dozing")
		     (message " [" i "] Exiting test")))
	       (condvar-unlock cvar)))))
(define (touchv j)
  (condvar-lock cvar)
  (set! touches (1+ touches))
  (unless (identical? j var)
    (set! var j) (condvar-signal cvar))
  (condvar-unlock cvar))
(define (stop-condvar-test)
  (condvar-lock cvar)
  (set! var #f)
  (condvar-signal cvar #t)
  (condvar-unlock cvar))

(define (test-condvars)
  (let ((threads {}))
    (dotimes (i 5) (set+! threads (spawn (watchv i))))
    (dotimes (i 200)
      (touchv (random 9999)) (sleep (* (random 10) (* 0.1 sleep-base))))
    (stop-condvar-test)
    (threadjoin threads)
    (applytest #t = sightings touches)
    (message "CONDVARS: sightings=touches=" touches)
    (set! var (random 100))))

(message "THREADTEST successfully completed")


