(in-module 'varconfig)

(define version "$Id: simpledb.scm 3135 2008-10-22 23:17:47Z haase $")

(module-export! '{varconfigfn varconfig! optconfigfn optconfig!})

(define varconfigfn
  (macro expr
    (let ((varname (cadr expr))
	  (convertfn (and (> (length expr) 2) (third expr))))
      (if convertfn
	  `(let ((_convert ,convertfn))
	     (lambda (var (val))
	       (if (bound? val)
		   (set! ,varname (_convert val))
		   ,varname)))
	  `(lambda (var (val))
	     (if (bound? val)
		 (set! ,varname val)
		 ,varname))))))

(define varconfig!
  (macro expr
    (let ((confname (cadr expr))
	  (confbody (cddr expr)))
      `(config-def! ',confname (varconfigfn ,@confbody)))))


(define optconfigfn
  (macro expr
    (let ((varname (second expr))
	  (optname (third expr))
	  (convertfn (and (> (length expr) 3) (fourth expr))))
      (if convertfn
	  `(let ((_convert ,convertfn))
	     (lambda (var (val))
	       (if (bound? val)
		   (store! ,varname ',optname (_convert val))
		   (get ,varname ',optname))))
	  `(lambda (var (val))
	     (if (bound? val)
		 (store! ,varname ',optname val)
		 (get ,varname ',optname)))))))

(define optconfig!
  (macro expr
    (let ((confname (cadr expr))
	  (confbody (cddr expr)))
      `(config-def! ',confname (optconfigfn ,@confbody)))))







