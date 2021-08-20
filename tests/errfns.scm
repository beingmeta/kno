;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

;;(use-module 'bugjar/html)

(applytest exception? (catcherr (+ 2 'foo)))
(applytest 5 (catcherr (+ 2 3)))

(applytest 5 call/cc (lambda (throwup) (catcherr (+ 2 3 (throwup (+ 2 3)) "three"))))
(applytest 5 call/cc (lambda (throwup) (report-errors (+ 2 3 (throwup (+ 2 3)) "five"))))
(applytest 5 call/cc (lambda (throwup) 
		       (onerror (+ 2 3 (throwup (+ 2 3)) "five")
			   (lambda (ex) #f))))
(evaltest 9
	  (let ((n 6))
	    (onerror (set! n 7) (lambda (ex) #f) (lambda ((x #f)) (or x 9)))))

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

(applytest #f clear-errors!)

(errtest (error justcond
	     "This was a simple error, " "barely" " worth signalling at all."))
(errtest (error justcond ""))
(errtest (irritant (* 1/2 1/2) "This is a fake error"))
(errtest (irritant (* 1/2 "1/2") "This is a fake error"))

(onerror (begin (error justcond))
    (lambda (ex) 
      (applytest (threadid) exception-threadno ex)
      (applytest 'justcond exception-condition ex)
      ;; This causes this to leak a lot
      ;; (%watch (exception-status ex))
      (applytest #f exception-details ex)
      (applytest #f exception-caller ex)
      (applytest #f exception-irritant ex)
      (applytest #f exception-irritant? ex)
      (applytest string? exception-summary ex)
      #f))
(onerror (begin (error justcond intest))
    (lambda (ex) 
      (applytest 'justcond ex/cond ex)
      (applytest 'intest exception-caller ex)
      (applytest #f exception-irritant ex)
      (applytest string? exception-summary ex #t)
      #f))
(onerror (begin (irritant 5 justcond intest "This is not really a detail"))
    (lambda (ex) 
      (applytest 'justcond ex/cond ex)
      (applytest 'intest ex/caller ex)
      (applytest 5 exception-irritant ex)
      (applytest #t exception-irritant? ex)
      (applytest string? exception-summary ex #t)
      (applytest slotmap? exception->slotmap ex)
      (applytest string? exception-summary ex)
      #f))
(onerror (begin (irritant 5 justcond))
    (lambda (ex) 
      (applytest 'justcond ex/cond ex)
      (applytest #f exception-caller ex)
      (applytest 5 exception-irritant ex)
      (applytest #t exception-irritant? ex)
      (applytest string? exception-summary ex #t)
      (applytest slotmap? exception->slotmap ex)
      (applytest string? exception-summary ex)
      #f))

(errtest (onerror (begin (error justcond intest))
	     (lambda (ex) 
	       (+ 3 "foo")
	       #f)))

(evaltest 5 (onerror (+ 8 9)
		(lambda (ex) (+ 3 "foo") #f)
	      5))
(errtest (onerror (+ 8 "9")
	     (error nohandler)
	   5))
(errtest (onerror (+ 8 9)
	     (lambda (ex) #f)
	   (error no_default_handler)))

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
(evaltest #f (ignore-errors (errfact 5)))
(define start 3)
(evaltest 120 (ignore-errors (goodfact 5) (begin (set! start (1+ start)) start)))
(evaltest 4 (ignore-errors (errfact 5) (begin (set! start (1+ start)) start)))

(onerror (errfact 20)
    (lambda (ex) 
      (applytest #t compound? (exception-stack ex 5))
      (applytest #t compound? (exception-stack ex 5 5))
      (applytest 2 length (exception-stack ex 5 7))
      (applytest 5 length (exception-stack ex -5))
      (applytest fixnum? length (exception-stack ex 0 1000))
      (applytest fixnum? length (exception-stack ex 1000 0))
      #f))

(evaltest "errobj" (onerror (errfact 5) "errobj"))
(evaltest "errobj" (onerror (errfact 5)
		       (lambda (ex) "errobj")))
(evaltest "errobj" (onerror (errfact 5)
		       (lambda (ex) "errobj")))
;; (evaltest '|BadBaseCase|
;; 	  (onerror (errfact 5)
;; 	      (lambda (ex)
;; 		(lineout (exception-summary ex))
;; 		(lineout (exception-summary ex #t))
;; 		(fileout "backtrace.html" (exception.html ex))
;; 		(get (exception->slotmap ex) 'condition))))

(errtest (onerror (errfact 5)
	     (lambda (ex)
	       (exception/context! ex 'broke)
	       (exception/context! ex 'comment "seen")
	       (applytest ambiguous? exception-context ex)
	       (applytest slotmap? exception->slotmap ex)
	       (reraise ex))))

(define (raise-something)
  (test-u8raise '(cons)))

(let ((n 6))
  (onerror (unwind-protect (error |Forced| evaltest)
	     (set! n (1+ n))
	     (clear-errors!)))
  (applytest #t = n 7))
(let ((n 6))
  (onerror (unwind-protect (raise-something)
	     (set! n (1+ n))
	     (clear-errors!)))
  (applytest #t = n 7))

(let ((n 6))
  (unwind-protect (glom "alpha" "beta")
    (set! n (1+ n))
    (clear-errors!))
  (applytest #t = n 7))
(let ((n 6))
  (onerror (unwind-protect (glom "alpha" "beta")
	     (error intest)
	     (set! n (1+ n))
	     (clear-errors!)))
  (applytest #t = n 6))
(let ((n 6))
  (onerror (unwind-protect (error |Forced| evaltest)
	     (error intest)
	     (set! n (1+ n))
	     (clear-errors!)))
  (applytest #t = n 6))

(errtest (dynamic-wind))
(errtest (dynamic-wind (error 'nowind)))
(errtest (dynamic-wind (lambda () (set! n 3)) (error 'nodoit)))
(errtest (dynamic-wind (lambda ()  (set! n 3)) (lambda () 8) (error 'nounwind)))

(let ((n 6))
  (errtest (dynamic-wind 
	       (lambda () (set! n 9))
	       (lambda () (error |Forced| evaltest))
	       (lambda () (set! n (1+ n)))))
  (applytest #t = n 10))

(let ((n 6))
  (errtest (dynamic-wind 
	       (lambda () (set! n 9))
	       (lambda () (error |Forced| evaltest))
	       (lambda () (set! n (1+ "n")))))
  (applytest #t = n 9))

(let ((n 6))
  (errtest (dynamic-wind 
	       (lambda () (set! n 9))
	       (lambda () (lognotice |InTest|))
	       (lambda () (set! n (1+ "n")))))
  (applytest #t = n 9))

(let ((n 6))
  (errtest (dynamic-wind 
	       (lambda (nothunk) (set! n 9))
	       (lambda () (+ 2 3))
	       (lambda () (set! n (1+ n))))))
(let ((n 6))
  (errtest (dynamic-wind 
	       (lambda () (set! n 9))
	       (lambda (nothunk) (+ 2 3))
	       (lambda () (set! n (1+ n))))))
(let ((n 6))
  (errtest (dynamic-wind 
	       (lambda () (set! n 9))
	       (lambda () (+ 2 3))
	       (lambda (nothunk) (set! n (1+ n))))))

(let ((n 6))
  (errtest (dynamic-wind 
	       (lambda () (set! n 9))
	       (lambda () (+ 2 "three"))
	       (lambda () (set! n (1+ n)))))
  (applytest #t = n 10))

(let ((n 6))
  (errtest (dynamic-wind 
	       (lambda () (set! n (+ 2 "three")))
	       (lambda () (+ 2 "three"))
	       (lambda () (set! n (1+ n)))))
  (applytest #t = n 6))

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
