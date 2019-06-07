(use-module '{reflection varconfig optimize logger})

(define started (elapsed-time))
(define logtime-file #f)
(varconfig! LOGTIME logtime-file)
(define (save-elapsed-atexit)
  (when logtime-file 
    (fileout logtime-file 
      (config 'ppid)
      "\t" (elapsed-time started)
      "\t" (doseq (arg (config 'argv)) (printout " " arg)))))
(config! 'atexit save-elapsed-atexit)

(config! 'log:threadid #t)

(define pooltype 'bigpool)
(varconfig! pooltype pooltype #t)
(define indextype 'hashindex)
(varconfig! indextype indextype #t)

(define (parser/roundtrip x)
  (string->lisp (lisp->string x)))

(define (dtype/roundtrip x)
  (packet->dtype (dtype->packet x)))

(define (optimization-leaks)
  (when (and (config 'testoptimized) 
	     (or (getenv "MEMCHECKING") (getenv "HEAPCHECK")))
    (exit)))

(define (qc-wrap x) `(qc ,x))

(defambda (applytest/err proc . args)
  (evaltest '|err| (onerror (apply proc args) '|err|)))

(define errors {})

(define test-finished
  (macro expr
    (let ((name (get-arg expr 1)))
      `(begin (deoptimize!)
	 (if (and (bound? errors) (exists? errors))
	     (begin (message (choice-size errors) " errors during " ,name)
	       (error 'tests-failed))
	     (message ,name " successfuly completed"))))))
  
(define (temp-arglist n (arglist '()))
  (if (= n 0)
      (reverse arglist)
      (temp-arglist (-1+ n)
		    (cons (string->symbol (glom "_arg_" n))
			  arglist))))

(when (config 'testoptimized #f)
  (use-module 'optimize)
  (define applytester
    (macro expr
      (let* ((result-form (second expr))
	     (fn-form (third expr))
	     (args-forms (slice expr 3))
	     (n-args (length args-forms))
	     (arglist (temp-arglist n-args)))
	`(let ((fcn (ambda ,arglist (,fn-form ,@arglist))))
	   (optimize-procedure! fcn)
	   (applytest ,result-form fcn ,@args-forms)))))
  (define evaltester
    (macro expr
      `(let ((fcn (lambda () ,(third expr))))
	 (optimize-procedure! fcn)
	 (pprint (lambda-body fcn))
	 (,evaltest ,(second expr) (fcn)))))
  (define define-tester
    (macro expr
      (if (pair? (cadr expr))
	  `(begin (define ,@(cdr expr))
	     (optimize! ,(car (cadr expr))))
	  `(define ,@(cdr expr)))))
  (define define-amb-tester
    (macro expr
      (if (pair? (cadr expr))
	  `(begin (defambda ,@(cdr expr))
	     (optimize! ,(car (cadr expr))))
	  `(defambda ,@(cdr expr)))))
  (define test-optimize! optimize!))

(when (config 'testoptimized #f)
  (config! 'optimize:rails #t)
  (config! 'optimize:level 4))

(unless (config 'testoptimized #f)
  (define applytester applytest)
  (define evaltester evaltest)
  (define define-tester define)
  (define define-amb-tester defambda)
  (define test-optimize! comment))



