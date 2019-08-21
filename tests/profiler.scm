;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{reflection bench/miscfns stringfmts optimize})

(define (show-profile arg (fcn))
  (default! fcn (if (compound? arg '%callprofile) (profile/fcn arg) arg))
  (unless (applicable? fcn) (irritant arg |NotAFunction| show-profile))
  (if (profile/getcalls fcn)
      (lineout "Profile for " (if (procedure-name fcn) (procedure-name fcn)) " ;; " fcn "\n"
	"    " (if (procedure-name fcn) (procedure-name fcn) fcn) " was called " (profile/ncalls arg) " times\n"
	"    taking " (secs->string (profile/time arg) #f) " execution time "
	"(" ($num (profile/nsecs arg)) " nanoseconds)\n"
	"    user code took " (secs->string (profile/utime arg) #f)  " "
	"while system calls took " (secs->string (profile/stime arg) #f) "\n"
	"    the function 'waited' (suspended voluntarily) " ($count (profile/waits arg) "time") "\n"
	"    the function was `contested` (suspended involuntarily) " ($count (profile/pauses arg) "time") "\n"
	"    the function also triggered " ($count (profile/faults arg) "fault"))
      (lineout "There is no profile information for "
	(if (procedure-name fcn) (procedure-name fcn)) " ;; " fcn "\n")))

(define fib-iter (importvar 'bench/miscfns 'fib-iter))
(define profiled {fibi + fibr fib-iter})

(do-choices (fn profiled) (config! 'profiled fn))

(dotimes (i 10) (fibi 1000))

;; This was testing for the old tail recursion implementation where
;; the call to fibi became a call to fib-iter. Now, fib-iter recurs,
;; but it doesn't replace fibi because it has more arguments and can't
;; use the stack frame. Keeping this in case, we ever restore the
;; previous functionality.

;;(applytest #t > (profile/time (profile/getcalls fib-iter)) (profile/time (profile/getcalls fibi)) )
;; "More" time is spent in fib-iter than fibi, because fibi is tail recursive
;;(applytest #t < (profile/nsecs (profile/getcalls fibi)) (profile/nsecs (profile/getcalls fib-iter)) )
;;(applytest #t > (profile/ncalls (profile/getcalls fib-iter)) (profile/ncalls (profile/getcalls +)) )

(profile/reset! profiled)

(applytest 0.0 (profile/time (profile/getcalls +)))
(applytest 0 (profile/nsecs (profile/getcalls +)))
(applytest 0 (profile/ncalls (profile/getcalls +)))

(set-procedure-tailable! fibi #f)

(dotimes (i 10) (fibi 1000))

;; Since we've made fibi not tailable, the time for fibi is not greater than fib-iter
;;  TODO: This depends on what kind of tail calls we're doing. Disabled for now.
;;(applytest #t > (profile/time (profile/getcalls fibi)) (profile/time (profile/getcalls fib-iter)) )

;;;; Test for extended profiling

(define-init shared-table #[count1 0 count2 0])

(define (update-field field) (store! shared-table field (1+ (get shared-table field))))

(config! 'profiled update-field)
(config! 'xprofiling #t)

(define wait-count 
  (begin (dotimes (i 10000) (update-field 'count1)) 
    (profile/waits (profile/getcalls update-field))))

(applytest > 0 profile/utime (profile/getcalls update-field))

(profile/reset! update-field)

;;; This threaded wait count test will occasionally fail, but bumping
;;; up threaded-wait-repeat reduces the chance of that.
(define threaded-wait-repeat 100000)
(define threaded-wait-count 
  (begin (parallel 
	  (dotimes (i threaded-wait-repeat)
	    (update-field 'count1))
	  (dotimes (i threaded-wait-repeat)
	    (update-field 'count2))) 
    (profile/waits (profile/getcalls update-field))))

(applytest > 0 profile/utime (profile/getcalls update-field))
(when (> (profile/waits (profile/getcalls update-field)) 0)
  ;; Some platforms (MacOS) don't seem to generate waits or report
  ;; them via rusage
  (applytest > wait-count profile/waits (profile/getcalls update-field)))

;;; Generate some descriptions
(show-profile +)
(show-profile (profile/getcalls +))
(show-profile update-field)
(show-profile (profile/getcalls update-field))

(applytest 'err profile/getcalls if)
(applytest #f profile/getcalls applytest)

(test-finished "PROFILER")
