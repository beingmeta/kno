(in-module 'xhtml/signature)

(use-module '{fdweb texttools})
(use-module '{varconfig logger rulesets ezrecords})

(module-export! '{sig/make sig/check})

(define %loglevel %debug!)
;;(define %loglevel %notify!)

(define (makesigtext op . args)
  (let ((table (if (odd? (length args)) (deep-copy (car args)) #[]))
	(params (if (odd? (length args)) (cdr args) args)))
    (do ((scan params (cddr scan)))
	((null? scan))
      (add! table (car scan) (cadr scan)))
    (stringout
      op "?"
      (doseq (key (lexsorted (getkeys table)) i)
	(if (string? key)
	    (printout (if (> i 0) "&")
		      (uriencode key) "=")
	    (printout (if (> i 0) "&")
		      (uriencode (lisp->string key)) "="))
	(doseq (v (lexsorted (get table key)) j)
	  (printout (if (> j 0) " ") v))))))

(define (sig/make key . args)
  (let ((text (apply makesigtext args)))
    (hmac-sha1 text key)))
(define (sig/check sig key . args)
  (let ((text (apply makesigtext args)))
    (debug%watch "CHECKSIG/FAILED"
		 sig key text (hmac-sha1 text key))
    (or (equal? sig (hmac-sha1 text key))
	(begin (warn%watch "CHECKSIG/FAILED"
			   sig key text (hmac-sha1 text key))))))

