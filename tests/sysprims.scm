;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'texttools)

(applytester #t string? (getenv "USER"))
(applytester #t inexact? (getload))
(applytester #t every? inexact? (loadavg))

(let ((usage (rusage)))
  (do-choices (key (getkeys usage))
    (let ((v (get usage key))
	  (cmp (rusage key)))
      (cond ((fail? cmp))
	    ((integer? v)
	     (applytester #t integer? cmp))
	    ((inexact? v)
	     (applytester #t inexact? cmp))
	    ((vector? v)
	     (applytester #t vector? cmp)))))
  (applytest flonum? cpusage usage))

(applytester #t table? (uname))
(applytester #t string? (get (uname) 'osname))
(applytester #t fixnum? (getpid))
(applytester #t fixnum? (getppid))
(applytester #t integer? (stacksize))
(applytester #t integer? (threadid))
(applytester #t string? (procstring))
(applytester #t integer? (memusage))
(applytester #t integer? (vmemusage))
(applytester #t integer? (physmem))
(applytester #t inexact? (memload))
(applytester #t inexact? (usertime))
(applytester #t inexact? (systime))
(applytester #t inexact? (cpusage))

(errtest (sleep -5))
(errtest (sleep -5.0))

(define ipcomp #((isdigit) (opt (isdigit)) (opt (isdigit))))
(define ipv4 (vector ipcomp "." ipcomp "." ipcomp "." ipcomp))

(define (ip-addr? string)
  (and (string? string) (textmatch ipv4 string)))

(applytest string? gethostname)
(applytest ip-addr? hostaddrs "beingmeta.com")
(applytester {} hostaddrs "beingmeta.cox")

(applytester #t check-version 5)
(applytester #t check-version 5 0 0)
(applytester #f check-version 5 1)
(applytester #f check-version 6)

(applytester #t require-version 5)
(applytester #t require-version 5 0)

(applytester #f check-version 5 0 2)
(errtest (require-version 5 0 2))
(errtest (require-version 6))
(errtest (require-version 5 1))

(errtest (check-version "5"))
(errtest (check-version 5 "0"))
(errtest (check-version 5 0 "1"))
(errtest (check-version 5 0 "1"))
(errtest (check-version 5 0 0 "patched"))
(applytester #t check-version 5 0 0 1)

;;;; Pointer locks, etc

(define ptrlock-vec #("foo" "bar" "baz"))
(define ptrlock-string #("foo" "bar" "baz"))
(applytest #t = (ptrlock ptrlock-vec) (ptrlock ptrlock-vec))
(applytest #t > (+ (if (= (ptrlock "foo") (ptrlock "foo")) 0 1)
		   (if (= (ptrlock "telephone") (ptrlock "telephone")) 0 1)
		   (if (= (ptrlock #"packet") (ptrlock #"packet")) 0 1))
	   0)
(applytest #t > (+ (if (= (ptrlock "foo" 811) (ptrlock "foo" 811)) 0 1)
		   (if (= (ptrlock "telephone" 811) (ptrlock "telephone" 811)) 0 1)
		   (if (= (ptrlock #"packet" 811) (ptrlock #"packet" 811)) 0 1))
	   0)
(applytest 0 + 
	   (if (= (ptrlock "foo" 777) (ptrlock "foo" 811)) 1 0)
	   (if (= (ptrlock "telephone" 777) (ptrlock "telephone" 811)) 1 0)
	   (if (= (ptrlock #"packet" 777) (ptrlock #"packet" 811)) 1 0))

;;; Config stuff

(applytest #t equal? (config 'loadpath) (config 'loadpath))


(test-finished "SYSPRIMS")
