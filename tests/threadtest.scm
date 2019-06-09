;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(use-module '{mttools fifo})

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
  (dotimes (i (* n 400))
    (when (zero? (random 5)) (thread/yield)))
  (elapsed-time start))

(define (test-thread/call (waitfn thread/join) (wait-opts #default))
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
    (message "TEST-THREAD/CALL: " numbers)))

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
  (sync/lock! numlock)
  (set! num n)
  (unwind-protect (evaltest n num)
    (sync/release! numlock)))

(define (change-num-with-lock (n (nrandom 1)))
  (with-lock numlock
    (set! num n)
    (sleep 2)
    (evaltest n num)))

(define (test-synchro-locks (nthreads 8))
  (thread/wait! (thread/call change-num (nrandom))))
(define (test-with-lock (lock (make-mutex)) (nthreads 8))
  (thread/wait! (thread/call change-num-with-lock (nrandom))))

;;;; Test fluid variables

(define (doubleup string)
  (thread/set! 'thstring string)
  (sleep 2)
  (prog1 (glom (thread/get 'thstring) string)
    (thread/reset-vars!)))

;;;; We don't run this because we can't easily ignore the failed tests

(define (change-num-recklessly (n (nrandom 1)))
  (set! num n)
  (sleep 2)
  (evaltest n num))

(define (test-reckless-locks (nthreads 8))
  (thread/wait! (thread/call change-num-recklessly (nrandom))))

;;;; CONDVAR testing

(define (fifo-generator fifo generated (count 20))
  (dotimes (i count)
    (let ((r (random 1000000)))
      (hashset-add! generated r)
      (fifo/push! fifo r))
    (sleep 0.01)))

(defimport fifo-load 'fifo)

(define (fifo-watcher fifo seen)
  (while (fifo-live? fifo)
    (hashset-add! seen (fifo/pop fifo))))

(defimport fifo-condvar 'fifo)

(define (test-fifo-condvars)
  (let ((fifo (fifo/make #[fillfn #f]))
	(generated (make-hashset))
	(seen (make-hashset))
	(gen-threads {})
	(watch-threads {}))
    (message "Testing conditional variable via FIFO" fifo)

    (applytest #t condvar? (fifo-condvar fifo))
    (applytest #t synchronizer? (fifo-condvar fifo))

    (dotimes (i 4)
      (set+! gen-threads (thread/call fifo-generator fifo generated)))
    (dotimes (i 16)
      (set+! watch-threads (thread/call fifo-watcher fifo seen)))
    (thread/wait! gen-threads)
    (until (zero? (fifo/load fifo)) (fifo/wait! fifo))
    (fifo/close! fifo)
    (thread/wait! watch-threads)
    (applytest #t identical? (hashset-elts generated) (hashset-elts seen))))

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

(define condvar (make-condvar))
(define mutex (make-mutex))
(define rwlock (make-rwlock))

(define (main)

  (applytest {} (find-thread 0))
  (applytest #f (find-thread 0 #f))
  (errtest (find-thread 0 #t))
  (applytest #f condvar? 3)
  (applytest #t condvar? condvar)
  (applytest #f mutex? 3)
  (applytest #t mutex? mutex)
  (applytest #f rwlock? 8)
  (applytest #t rwlock? rwlock)
  (applytest #f synchronizer? 3)
  (applytest #t synchronizer? mutex)
  (applytest #t synchronizer? rwlock)
  (applytest #t synchronizer? condvar)
  (applytest #t synchronizer? change-slambda-test-value)
  (applytest #f thread? 3)

  ;;; This doesn't seem to be working right
  
  (evaltest {"foofoo" "barbar" "carcar"}
	    (thread/finish (thread/call doubleup {"foo" "bar" "car"})))

  (evaltest 3 (thread/finish (spawn (length "abc"))))
  (evaltest {3 4 5} (thread/finish (thread/eval (list 'length {"abc" "abcd" "abcde"}))))

  (applytest #t number? (cstack-limit))
  (applytest #t number? (cstack-depth))
  (applytest #t < (cstack-depth) (cstack-limit))
  (cstack-limit! (+ 2 (cstack-limit)))

  (let ((good-thread (thread/join (thread/call goodfact 5)))
	(error-thread (thread/join (thread/call errfact 5))))
    (applytest #t thread? good-thread)
    (applytest #t thread/exited? good-thread)
    (applytest #t thread/finished? good-thread)
    (applytest #f thread/error? good-thread)
    
    (applytest #t thread? error-thread)
    (applytest #t thread/exited? error-thread)
    (applytest #f thread/finished? error-thread)
    (applytest #t thread/error? error-thread)
    (applytest #t exception? (thread/result error-thread))
    (applytest #t vector? (ex/stack (thread/result error-thread))))
    
  (test-synchro-locks)
  (test-with-lock)
  (test-with-lock (make-rwlock))
  (test-with-lock (make-condvar))
  (test-with-lock change-slambda-test-value)
  
  (test-threadids)
  (test-parallel)
  (test-spawn)
  (test-slambdas)

  (test-thread/call)
  (test-thread/call thread/wait 3)
  (test-thread/call thread/wait #f)
  (test-thread/call thread/wait! 0.0003)
  (test-thread/call thread/wait (timestamp+ 1))
  (test-thread/call thread/wait [timeout 3])

  (test-fifo-condvars)

  (test-do-choices-mt)

  (test-finished "THREADTEST")
  )

