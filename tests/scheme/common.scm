(use-module '{reflection optimize logger})

(define (optimization-leaks)
  (when (and (config 'testoptimized) 
	     (or (getenv "MEMCHECKING") (getenv "HEAPCHECK")))
    (exit)))

(define (qc-wrap x) `(qc ,x))

(define errors {})

(define test-finished
  (macro expr
    (let ((name (get-arg expr 1)))
      `(begin (deoptimize!)
	 (if (and (bound? errors) (exists? errors))
	     (begin (message (choice-size errors) " errors during " ,name)
	       (error 'tests-failed))
	     (message ,name " successfuly completed"))))))
  
(when (config 'testoptimized #f)
  (use-module 'optimize)
  (define applytester
    (macro expr
      `(let ((fcn (lambda () (,applytest ,@(map qc-wrap (cdr expr))))))
	 (optimize-procedure! fcn)
	 (fcn)
	 (deoptimize-procedure! fcn))))
  (define evaltester
    (macro expr
      `(let ((fcn (lambda () (,evaltest ,@(cdr expr)))))
	 (optimize-procedure! fcn)
	 (fcn)
	 (deoptimize-procedure! fcn))))
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

(unless (config 'testoptimized #f)
  (config! 'optimize:rails #t)
  (config! 'optimize:level 4)
  (define applytester applytest)
  (define evaltester evaltest)
  (define define-tester define)
  (define define-amb-tester defambda)
  (define test-optimize! comment))



