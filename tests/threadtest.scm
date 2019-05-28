(load-component "common.scm")

(use-module 'mttools)

(define numbers '())
(define addnumber
  (slambda (n) (set! numbers (cons n numbers))))
(define sleep-base 0.01)
(define (addrange start (len 10))
  (dotimes (i len)
    (when (zero? (random 5)) (thread/yield))
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
    (set+! threads (spawn (begin (sleep 2) (addrange 10))))
    (applytest #t exists? (config 'ALLTHREADS))
    (set+! threads (spawn (addrange 0)))
    (thread/join threads)
    (applytest 20 length numbers)
    (applytest #f check-ordered numbers)
    (message "TEST-SPAWN: " numbers)))

(define (look-busy n (start (elapsed-time)))
  (dotimes (i (* n 1000))
    (when (zero? (random 5)) (thread/yield)))
  (elapsed-time start))

(define (test-threadcall (waitfn thread/join) (wait-opts #f))
  (let ((threads {}))
    (set! numbers '())
    ;; We're testing a bunch of things here and have addrange sleep so
    ;; it doesn't exit before we finish the thread-find tests
    (set+! threads (thread/call addrange 10))
    (set+! threads (thread/call addrange 0))
    (waitfn threads wait-opts)
    (waitfn threads)
    (applytest #t thread? threads)
    (applytest #t thread/exited? threads)
    (applytest #t thread/finished? threads)
    (applytest #f thread/error? threads)
    (applytest 20 length numbers)
    (applytest #f check-ordered numbers)
    (message "TEST-THREADCALL: " numbers)))

(define (test-threadids)
  (let ((sleep1 (thread/call look-busy 5))
	(sleep2 (thread/call look-busy 5)))
    (look-busy 2)
    (applytest sleep2 find-thread (thread-id sleep2))
    (applytest #t inexact? (thread/finish sleep1))
    (thread/wait sleep2)
    (applytest #t inexact? (thread/result sleep2))))

;;;; CONDVAR testing

(define touches 0)
(define sightings 0)
(define noisy (config 'NOISY #f))

(define cvar (make-condvar))
(define var 33)
(define (watchv i cvar)
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
(define (touchv j cvar)
  (condvar-lock cvar)
  (set! touches (1+ touches))
  (unless (identical? j var)
    (set! var j) (condvar-signal cvar))
  (condvar-unlock cvar))
(define (stop-condvar-test (cvar cvar))
  (condvar-lock cvar)
  (set! var #f)
  (condvar-signal cvar #t)
  (condvar-unlock cvar))

(define (test-condvars (cvar cvar))
  (message "Testing conditional variable " cvar)
  (let ((threads {}))
    (set! var (random 100))
    (dotimes (i 5) (set+! threads (spawn (watchv i cvar))))
    (dotimes (i 200)
      (touchv (random 9999) cvar)
      (sleep (* (random 10) (* 0.1 sleep-base))))
    (sleep 1)
    (stop-condvar-test cvar)
    (thread/join threads)
    (message "CONDVARS: sightings=" sightings "; touches=" touches)
    (applytest #t = sightings touches)))

(define (test-do-choices-mt)
  (let ((ids {}))
    (do-choices-mt (num (mt/nrange 0 2000))
      (set+! ids (threadid)))
    (message "DO-CHOICES-MT-TEST: " ids)
    (applytest #t (> (choice-size ids) 1))))

(errtest (find-thread 0 #t))
(applytest #f synchronizer? 3)
(applytest #f thread? 3)
(applytest #t number? (cstack-limit))
(applytest #t number? (cstack-depth))
(applytest #t < (cstack-depth) (cstack-limit))
(cstack-limit! (+ 2 (cstack-limit)))
(test-threadids)
(test-parallel)
(test-spawn)
(test-threadcall)
(test-threadcall thread/wait 3)
(test-threadcall thread/wait #f)
(test-threadcall thread/wait! 0.0003)
(test-threadcall thread/wait (timestamp+ 1))
(test-threadcall thread/wait [timeout 3])
(test-condvars)
(set! sightings 0)
(set! touches 0)
(set! var 33)
(set! cvar #f)
(test-condvars (make-condvar))
(when (threadid) (test-do-choices-mt))

(test-finished "THREADTEST")
