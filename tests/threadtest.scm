;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(use-module 'mttools)

(define (nrange (start 0) (n 8))
  (let ((nums {}))
    (dotimes (i n) (set+! nums (+ start i)))
    nums))
(define (nrandom (n 8) (range 1101513929))
  (let ((nums {}))
    (dotimes (i n) (set+! nums (random range)))
    nums))

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

;;; TODO: Test threads which throw errors
;;; TODO: Test synchro/lock, with-lock, etc (different kinds of synchronizers)
;;; TODO: Figure out why condvar tests sometimes fail
;;; TODO: Test slambdas

(define (test-parallel)
  (set! numbers '())
  (parallel (addrange 0) (addrange 10))
  (applytest 20 length numbers)
  (applytest #f check-ordered numbers)
  (message "TEST-PARALLEL: " numbers))

(define (test-thread/apply)
  (let ((threads (thread/apply glom "foo" {"bar" "baz"} #("alpha" "beta"))))
    (applytest 2 choice-size threads)
    (applytest #t thread? threads)
    (applytest {"foobaralphabeta" "foobazalphabeta"}
	       thread/finish threads)
    (applytest #t thread/exited? threads)
    (applytest #f thread/error? threads)
    (applytest #t thread/finished? threads)
    (message "TEST-THREAD/APPLY done")))

(define (test-spawn)
  (let ((threads {}))
    (set! numbers '())
    (set+! threads (spawn (begin (sleep 2) (addrange 10))))
    ;; TODO: Cycle in thread environment which contains the threads?
    (applytest #t thread? threads)
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
    (thread/wait! sleep2)
    (applytest #t inexact? (thread/result sleep2))))

;;;; Lock testing

(define num #f)
(define numlock (make-condvar))

(define (change-num (n (nrandom 1)))
  (synchro/lock! numlock)
  (set! num n)
  (unwind-protect (evaltest n num)
    (synchro/unlock! numlock)))

(define (change-num-with-lock (n (nrandom 1)))
  (with-lock numlock
    (set! num n)
    (sleep 2)
    (evaltest n num)))

(define (test-synchro-locks (nthreads 8))
  (thread/wait! (thread/call change-num (nrandom))))
(define (test-with-lock (nthreads 8))
  (thread/wait! (thread/call change-num-with-lock (nrandom))))

;;;; Test fluid variables

(define (doubleup string)
  (thread/set! 'thstring string)
  (sleep 2)
  (prog1 (glom (thread/get 'thstring) string)
    (thread/reset-vars!)))

(evaltest {"foofoo" "barbar" "carcar"}
	  (thread/finish (thread/call doubleup {"foo" "bar" "car"})))

;;;; We don't run this because we can't easily ignore the failed tests

(define (change-num-recklessly (n (nrandom 1)))
  (set! num n)
  (sleep 2)
  (evaltest n num))

(define (test-reckless-locks (nthreads 8))
  (thread/wait! (thread/call change-num-recklessly (nrandom))))

;;;; CONDVAR testing

(define touches 0)
(define sightings 0)
(define noisy (config 'NOISY #f))

(define cvar (make-condvar))
(define var 33)
(define (watchv i cvar)
  (while var
    (condvar/lock! cvar)
    (if (if (zero? (remainder i 2))
	    (condvar/wait cvar (+ 2 (random 4)))
	    (condvar/wait cvar))
	(begin (when noisy
		 (if var
		     (message " [" i "] Seen " var)
		     (message " [" i "] Exiting test")))
	       (when var (set! sightings (1+ sightings)))
	       (condvar/unlock! cvar))
	(begin (when noisy
		 (if var (message " [" i "] Just dozing")
		     (message " [" i "] Exiting test")))
	       (condvar/unlock! cvar)))))
(define (touchv j cvar)
  (condvar/lock! cvar)
  (set! touches (1+ touches))
  (unless (identical? j var)
    (set! var j) (condvar/signal cvar))
  (condvar/unlock! cvar))
(define (stop-condvar-test (cvar cvar))
  (condvar/lock! cvar)
  (set! var #f)
  (condvar/signal cvar #t)
  (condvar/unlock! cvar))

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

;;; Testing synchronized lambdas

(define slambda-test-value #f)

(define change-slambda-test-value
  (slambda (n) 
    (set! slambda-test-value n)
    (sleep 1)
    (evaltest n slambda-test-value)))

(define (test-slambdas (nthreads 8))
  (thread/wait! (thread/call change-slambda-test-value (nrandom))))

(define (goodfact n)
  (if (<= n 0) 1
      (* n (goodfact (-1+ n)))))

(define (errfact n)
  (if (<= n 0) 'one 
      (* n (errfact (-1+ n)))))

;;; Actual tests

(errtest (find-thread 0 #t))
(applytest #f condvar? 3)
(applytest #t condvar? cvar)
(applytest #f synchronizer? 3)
(applytest #t synchronizer? cvar)
(applytest #t synchronizer? change-slambda-test-value)
(applytest #f thread? 3)

(evaltest 3 (thread/finish (inthread (length "abc"))))
(evaltest {3 4 5} (thread/finish (thread/eval (list 'length {"abc" "abcd" "abcde"}))))

(applytest #t number? (cstack-limit))
(applytest #t number? (cstack-depth))
(applytest #t < (cstack-depth) (cstack-limit))
(cstack-limit! (+ 2 (cstack-limit)))

(define good-thread (thread/wait (thread/call goodfact 5)))
(applytest #t thread? good-thread)
(applytest #t thread/exited? good-thread)
(applytest #t thread/finished? good-thread)
(applytest #f thread/error? good-thread)

(define error-thread (thread/wait (thread/call errfact 5)))
(applytest #t thread? error-thread)
(applytest #t thread/exited? error-thread)
(applytest #f thread/finished? error-thread)
(applytest #t thread/error? error-thread)
(applytest #t exception? (thread/result error-thread))
(applytest #t vector? (ex/stack (thread/result error-thread)))

(test-synchro-locks)
(test-with-lock)

(test-threadids)
(test-parallel)
(test-spawn)
(test-slambdas)

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
(test-do-choices-mt)

(test-finished "THREADTEST")
