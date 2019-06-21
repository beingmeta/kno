;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'texttools)

(applytest #t string? (getenv "USER"))
(applytest #t inexact? (getload))
(applytest #t every? inexact? (loadavg))

(let ((usage (rusage)))
  (do-choices (key (getkeys usage))
    (let ((v (get usage key))
	  (cmp (rusage key)))
      (cond ((fail? cmp))
	    ((integer? v)
	     (applytest #t integer? cmp))
	    ((inexact? v)
	     (applytest #t inexact? cmp))
	    ((vector? v)
	     (applytest #t vector? cmp)))))
  (applytest-pred flonum? cpusage usage))

(applytest #t table? (uname))
(applytest #t string? (get (uname) 'osname))
(applytest #t fixnum? (getpid))
(applytest #t fixnum? (getppid))
(applytest #t integer? (stacksize))
(applytest #t integer? (threadid))
(applytest #t string? (procstring))
(applytest #t integer? (memusage))
(applytest #t integer? (vmemusage))
(applytest #t integer? (physmem))
(applytest #t inexact? (memload))
(applytest #t inexact? (usertime))
(applytest #t inexact? (systime))
(applytest #t inexact? (cpusage))

(errtest (sleep -5))
(errtest (sleep -5.0))

(define ipcomp #((isdigit) (opt (isdigit)) (opt (isdigit))))
(define ipv4 (vector ipcomp "." ipcomp "." ipcomp "." ipcomp))

(define (ip-addr? string)
  (and (string? string) (textmatch ipv4 string)))

(applytest-pred string? gethostname)
(applytest-pred ip-addr? hostaddrs "beingmeta.com")
(applytest {} hostaddrs "beingmeta.cox")

(test-finished "SYSPRIMS")
