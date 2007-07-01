(use-module '{reflection optimize})

(when (config 'testoptimize #f)
  (use-module 'optimize)
  (define applytester
    (macro expr
      `(let ((fcn (lambda () (,applytest ,@(cdr expr)))))
	 (optimize-procedure! fcn)
	 (fcn))))
  (define evaltester
    (macro expr
      `(let ((fcn (lambda () (,evaltest ,@(cdr expr)))))
	 (optimize-procedure! fcn)
	 (fcn))))
  (define test-optimize! optimize!))

(unless (config 'testoptimize #f)
  (define applytester applytest)
  (define evaltester evaltest)
  (define test-optimize! comment))



