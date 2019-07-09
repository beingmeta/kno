;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(use-module '{mttools fifo})

(define-tester (nrange (start 0) (n 8))
  (let ((nums {}))
    (dotimes (i n) (set+! nums (+ start i)))
    nums))
(define-tester (nrandom (n 8) (range 1101513929))
  (let ((nums {}))
    (dotimes (i n) (set+! nums (random range)))
    nums))
(define-tester (sleeper n (secs 3) (fn (getuuid)))
  (dotimes (i n) 
    (sleep secs)
    (cond ((not fn))
	  ((applicable? fn) (fn i))
	  (else (message "@" i ": " fn)))))

(define numbers '())
(define addnumber
  (slambda (n) (set! numbers (cons n numbers))))
;;; slambdas never get copied
(applytest #t eq? addnumber (deep-copy addnumber))

(define sleep-base 0.01)
(define-tester (addrange start (len 10))
  (dotimes (i len)
    (when (zero? (random 5)) (thread/yield))
    (addnumber (+ start i))
    (sleep (* (1+ (random 5)) sleep-base))))

(define-tester (check-ordered list)
  (cond ((or (null? list) (null? (cdr list))) #t)
	((< (car list) (cadr list))
	 (check-ordered (cdr list)))
	(else #f)))

(define-tester (test-parallel)
  (set! numbers '())
  (parallel (addrange 0) (addrange 10))
  (applytest 20 length numbers)
  (applytest #f check-ordered numbers)
  (message "TEST-PARALLEL: " numbers))

(define-tester (test-more-parallel)
  (set! numbers '())
  (parallel (addrange 0) (addrange 10) (addrange 8) (addrange 15)
	    (addrange 0) (addrange 10) (addrange 8) (addrange 15))
  (applytest 20 length numbers)
  (applytest #f check-ordered numbers)
  (message "TEST-PARALLEL: " numbers))

(define-tester (test-thread/apply)
  (let ((threads (thread/apply glom "foo" {"bar" "baz"} #("alpha" "beta"))))
    (applytest 2 choice-size threads)
    (applytest #t thread? threads)
    (applytest {"foobaralphabeta" "foobazalphabeta"}
	       thread/finish threads)
    (applytest #t thread/exited? threads)
    (applytest #f thread/error? threads)
    (applytest #t thread/finished? threads)
    (message "TEST-THREAD/APPLY done")))

(define-tester (test-spawn)
  (let ((threads {}))
    (set! numbers '())
    (set+! threads (spawn (begin (sleep 1) (addrange 10))))
    (applytest #t thread? threads)
    (applytest #t exists? (config 'ALLTHREADS))
    (set+! threads (spawn (addrange 0)))
    (applytest string? lisp->string (pick-one threads))
    (thread/join threads)
    (thread/cancel! (pick-one threads))
    (applytest 20 length numbers)
    (applytest #f check-ordered numbers)
    (message "TEST-SPAWN: " numbers)))

(define-tester (look-busy n (start (elapsed-time)))
  (sleep 1)
  (dotimes (i (* n 500))
    (when (zero? (random 5)) (thread/yield)))
  (elapsed-time start))

(define-tester (test-thread-cancel)
  (let ((slowhand (lambda (x) (sleep 5) x))
	(thread1 (thread/call slowhand 1))
	(thread2 (thread/call slowhand 7))
	(thread3 (thread/call slowhand 10)))
    (sleep 2)
    (thread/cancel! thread2)
    ;; This should cause the thread structures to get walked when
    ;;  the environment is freed
    (applytest 4 (lambda (x) (+ x 2)) 2)
    (applytest {1 10} thread/finish {thread1 thread2 thread3})))

(define-tester (test-thread-signal)
  (let ((slowhand (lambda (x) (sleep 5) x))
	(thread1 (thread/call slowhand 1))
	(thread2 (thread/call slowhand 7))
	(thread3 (thread/call slowhand 10)))
    (sleep 2)
    (thread/signal! thread2)
    (applytest {1 10} thread/finish {thread1 thread2 thread3})))

(define-tester (test-thread-terminate)
  (let ((slowhand (lambda (x) (sleep 5) x))
	(thread1 (thread/call slowhand 1))
	(thread2 (thread/call slowhand 7))
	(thread3 (thread/call slowhand 10)))
    (sleep 2)
    (thread/terminate! thread2)
    (applytest {1 10} thread/finish {thread1 thread2 thread3})))

(define-tester (test-thread/call (waitfn thread/join) (wait-opts #default))
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

(define-tester (test-threadids)
  (let ((busy1 (thread/call look-busy 3))
	(busy2 (thread/call look-busy 3)))
    (look-busy 2)
    (applytest busy1 find-thread (thread-id busy1))
    (applytest #t inexact? (thread/finish busy2))
    (thread/wait! busy2)
    (applytest #t inexact? (thread/result busy2))))

;;;; Lock testing

(define num #f)
(define numlock (make-condvar))

(applytest string? lisp->string numlock)

(define-tester (change-num numlock (n (nrandom 1)))
  (sync/lock! numlock)
  (set! num n)
  (sleep 0.01)
  (unwind-protect (evaltest n num)
    (sync/release! numlock)))

(define-tester (change-num-with-lock numlock (n (nrandom 1)))
  (with-lock numlock
    (set! num n)
    (sleep 0.01)
    (evaltest n num)))

(define-tester (test-synchro-locks (numlock (make-mutex)) (nthreads 8))
  (when numlock (applytest string? lisp->string numlock))
  (let ((num #f))
    (let ((change-num (lambda (lock newval)
			(when lock (sync/lock! lock))
			(set! num newval)
			(sleep 0.01)
			(unwind-protect (evaltest newval num)
			  (when lock (sync/release! lock))))))
      (thread/wait! (thread/call change-num numlock (nrandom))))))
(define-tester (test-synchro-locks (numlock (make-mutex)) (nthreads 8))
  (when numlock (applytest string? lisp->string numlock))
  (thread/wait! (thread/call change-num numlock (nrandom))))

(define-tester (test-with-lock (numlock (make-mutex)) (nthreads 8))
  (when numlock (applytest string? lisp->string numlock))
  (thread/wait! (thread/call change-num-with-lock numlock (nrandom))))

;;;; Test fluid variables

(define-tester (doubleup string)
  (thread/set! 'thstring string)
  (errtest (thread/set! "thstring" string))
  (sleep 0.01)
  (prog1 (glom (thread/get 'thstring) string)
    (applytest #t thread/bound? 'thstring)
    (applytest #f thread/bound? 'nothstring)
    (evaltest string (thread/cache 'thstring #f))
    (evaltest string (thread/cache 'thstring (+ 3 'x)))
    (applytest #f thread/bound? 'alpha)
    (applytest {} thread/get 'alpha)
    (evaltest "here" (thread/cache 'alpha "here"))
    (applytest #t thread/bound? 'alpha)
    (applytest "here" thread/get 'alpha)
    (evaltest 'void (thread/add! 'options "green"))
    (evaltest 'void (thread/add! 'options "blue"))
    (applytest {"blue" "green"} thread/get 'options)
    (errtest (thread/cache 33))
    (errtest (thread/cache 33 39))
    (thread/reset-vars!)))

;;;; We don't run this because we can't easily ignore the failed tests

(define (change-num-recklessly (n (nrandom 1)))
  (set! num n)
  (sleep 0.2)
  (evaltest n num))

(define-tester (test-reckless-locks (nthreads 8))
  (thread/wait! (thread/call change-num-recklessly (nrandom))))

;;;; CONDVAR testing

(define-tester (fifo-generator fifo generated (count 20))
  (dotimes (i count)
    (let ((r (random 1000000)))
      (hashset-add! generated r)
      (fifo/push! fifo r))
    (sleep 0.01)))

(defimport fifo-load 'fifo)

(define-tester (fifo-watcher fifo seen)
  (while (fifo-live? fifo)
    (hashset-add! seen (fifo/pop fifo))))

(defimport fifo-condvar 'fifo)

(define-tester (test-fifo-condvars)
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

(define-tester (test-do-choices-mt)
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
    (sleep 0.01)
    (evaltest n slambda-test-value)))

(define (test-slambdas (nthreads 8))
  (thread/wait! (thread/call change-slambda-test-value (nrandom))))

(define (goodfact n)
  (if (<= n 0) 1
      (* n (goodfact (-1+ n)))))

(define (errfact n)
  (if (<= n 0) 'one 
      (* n (errfact (-1+ n)))))

;;; Signalling, terminating, and cancelling
;;; These aren't real functional tests, they're just smoke tests
(define (test-threadops)
  (let ((thread1 (thread/call )))))

;;;; SSET

(define sset-value 0)
(define (increment-sset)
  (sset! sset-value (1+ sset-value)))
(define (safe-increment)
  (sset! sset-value (1+ sset-value)))
(define (increment-sset (n 100) (threadid #f))
  (dotimes (i n) (safe-increment)))
(define (test-sset)
  (set! sset-value 0)
  (thread/wait! (thread/call increment-sset 100 {1 2 3 4 5 6 7}))
  (evaltest 700 sset-value))

(test-sset)

(errtest (sset!))
(errtest (sset! "foo"))
(errtest (sset! foo))
(errtest (sset! sset-value (+ 3 "four")))

(errtest (let ((x 3)) (sset! zyyy 9)))

;;; Actual tests

(define condvar (make-condvar))
(define mutex (make-mutex))
(define rwlock (make-rwlock))

(errtest (condvar/wait mutex))
(errtest (condvar/wait rwlock))
(errtest (condvar/signal mutex))
(errtest (condvar/signal rwlock))
(errtest (condvar/lock! mutex))
(errtest (condvar/lock! rwlock))
(errtest (condvar/unlock! mutex))
(errtest (condvar/unlock! rwlock))

(define (main)

  (applytest {} (find-thread 0))
  (applytest #f (find-thread 0 #f))
  (applytest #f (find-thread 1))
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
  (applytest #f synchronizer? change-num-recklessly)
  (applytest #f thread? 3)

  (applytest string? lisp->string condvar)
  (applytest string? lisp->string mutex)
  (applytest string? lisp->string rwlock)
  
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
  
  (logwarn |SynchroLocks|)
  (test-synchro-locks)
  (test-synchro-locks (make-rwlock))
  (test-synchro-locks (make-condvar))
  (test-synchro-locks change-slambda-test-value)
  (test-synchro-locks "string")
  (test-synchro-locks change-num-recklessly)
  (logwarn |WithLocks|)
  (test-with-lock)
  (test-with-lock (make-rwlock))
  (test-with-lock (make-condvar))
  (test-with-lock change-slambda-test-value)
  (test-with-lock "string")
  (test-with-lock change-num-recklessly)
  
  (errtest (thread/wait "thread"))
  (errtest (thread/wait! "thread"))

  (test-threadids)
  (logwarn |Parallel|)
  (test-parallel)
  (logwarn |Spawn|)
  (test-spawn)
  (logwarn |Slambda|)
  (test-slambdas)

  (test-sset)

  (errtest (thread/call "proc"))
  (errtest (thread/call))
  (errtest (thread/call+ #f "proc"))
  (errtest (thread/call+))
  (errtest (spawn (+ 2 3) (error 'noopts)))

  (errtest (thread/eval '(+ 2 3) #"not an env"))
  (errtest (thread/eval))
  (errtest (thread/eval (cons)))
  (errtest (thread/eval (cons 'x 'y) (cons)))
  (errtest (thread/eval (cons 'x 'y) #f (cons)))

  (errtest (thread/apply list 3 4 5 6))
  (evaltest '(3 4 5 6) (thread/finish (thread/apply list 3 4 5 (list 6))))

  (logwarn |ThreadCalls|)
  (test-thread/call)
  (test-thread/call thread/wait 3)
  (test-thread/call thread/wait #f)
  (test-thread/call thread/wait! 0.0003)
  (test-thread/call thread/wait (timestamp+ 1))
  (test-thread/call thread/wait [timeout 3])

  (test-fifo-condvars)

  (test-do-choices-mt)

  ;;; Need to add a thread/shutdown! primitive which calls finish_threads

  (test-finished "THREADTEST")

)
