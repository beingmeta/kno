(use-module '{reflection optimize})

(define (qc-wrap x) `(qc ,x))

(when (config 'testoptimize #f)
  (use-module 'optimize)
  (define applytester
    (macro expr
      `(let ((fcn (lambda () (,applytest ,@(map qc-wrap (cdr expr))))))
	 (optimize-procedure! fcn)
	 (fcn))))
  (define evaltester
    (macro expr
      `(let ((fcn (lambda () (,evaltest ,@(cdr expr)))))
	 (optimize-procedure! fcn)
	 (fcn))))
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

(unless (config 'testoptimize #f)
  (define applytester applytest)
  (define evaltester evaltest)
  (define define-tester define)
  (define define-amb-tester defambda)
  (define test-optimize! comment))



