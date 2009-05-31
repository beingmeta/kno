(in-module 'varconfig)

(define version "$Id$")

(module-export! '{varconfigfn varconfig! optconfigfn optconfig!})

(define varconfigfn
  (macro expr
    (let ((varname (cadr expr))
	  (convertfn (and (> (length expr) 2) (third expr)))
	  (combinefn (and (> (length expr) 3) (fourth expr))))
      `(let ((_convert ,convertfn)
	     (_combine ,combinefn))
	 (lambda (var (val))
	   (if (bound? val)
	       (set! ,varname
		     ,(cond ((and convertfn combinefn)
			     `(_combine (_convert val) ,varname))
			    (convertfn
			     `(_convert val))
			    (combinefn
			     `(_combine val ,varname))
			    (else 'val)))
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







