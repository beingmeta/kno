;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'bugjar/html)

(applytest exception? (catcherr (+ 2 'foo)))
(applytest 5 (catcherr (+ 2 3)))

(errtest (%err '|SimpleError|))
(errtest (%err '|SimpleError| 'errtest))
(errtest (%err '|SimpleError| 'errtest "details"))
(errtest (%err '|SimpleError| 'errtest 'details))
(errtest (%err '|SimpleError| 'errtest '(details)))

(applytest #t exception? (catcherr (begin (%err '|SimpleError|))))

(errtest (irritant "short string" |TooShort|))
(errtest (irritant "short string" |TooShort| |ERRFNSTEST|))
(errtest (irritant "short string" |TooShort| |ERRFNSTEST| 
		   "These arguments should be processed as " printout " output"))

(errtest (irritant (elt "Sample" 11) |RecursiveError| |ERRFNSTEST| 
		   "These arguments should be processed as " printout " output"))

(define myerr error)

(errtest (myerr "This is a custom error with some display elements, like the PID " (getpid)))

(let ((n 0))
   (until (> n 5)
     (onerror
	 (if (> n 3) (break) (set! n (1+ n)))
	 #f))
   (applytest = n 4))

(let ((n 0))
  (onerror (until (> n 5)
	     (if (> n 3) (break) (set! n (1+ n))))
      #f)
  (applytest = n 4))

(define (errfact n)
  (if (<= n 1)
      (irritant n |BadBaseCase| errfact)
      (* n (errfact (-1+ n)))))
(define (goodfact n)
  (if (<= n 1)
      1
      (* n (goodfact (-1+ n)))))

(applytest #f (report-errors (errfact 5)))
(applytest 120 (report-errors (goodfact 5)))

(evaltest 5 (ignore-errors (errfact 5) 5))
(define start 3)
(evaltest 120 (ignore-errors (goodfact 5) (begin (set! start (1+ start)) start)))
(evaltest 4 (ignore-errors (errfact 5) (begin (set! start (1+ start)) start)))

(evaltest "errobj" (onerror (errfact 5) "errobj"))
(evaltest "errobj" (onerror (errfact 5)
		       (lambda (ex) "errobj")))
(evaltest "errobj" (onerror (errfact 5)
		       (lambda (ex) "errobj")))
(evaltest '|BadBaseCase|
	  (onerror (errfact 5)
	      (lambda (ex)
		(lineout (exception-summary ex))
		(lineout (exception-summary ex #t))
		(fileout "backtrace.html" (exception.html ex))
		(get (exception->slotmap ex) 'condition))))

(define (raise-something)
  (test-u8raise '(cons)))

#|
(let ((n 6))
  (errtest (unwind-protect (error |Forced| evaltest)
	     (set! n (1+ n))
	     (clear-errors!)))
  (applytest #t = n 7))
(let ((n 6))
  (errtest (unwind-protect (raise-something)
	     (set! n (1+ n))
	     (clear-errors!)))
  (applytest #t = n 7))
|#

(applytest exception? 
	   make-exception "Testing" 'test-exception
	   "details" 
	   #f ;; threadid
	   #f ;; (config 'sessionid)
	   (elapsed-time)
	   #f ;; timebase
	   #("no stack") #[name "no context"]
	   #[type irritant role "fake"])

(if (exists? errors)
    (begin (message (choice-size errors)
		    " Errors during ERRFNS tests")
	   (error 'tests-failed))
    (message "ERRFNS tests successfuly completed"))

(test-finished "ERRFNS")