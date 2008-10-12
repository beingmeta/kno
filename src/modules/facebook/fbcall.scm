(in-module 'facebook/fbcall)

(use-module '{fdweb xhtml texttools facebook})

(module-export! '{fbcall fbcalluri})

(define (fbcalluri method args)
  (set! args (cons* "api_key" apikey "call_id" (get (timestamp) 'tick)
		    "v" "1.0" "method" method
		    args))
  (if (cgitest 'fb_sig_session_key)
      (set! args (cons* "session_key" (cgiget 'fb_sig_session_key) args))
      (if (cgitest 'fbsession)
	  (set! args (cons* "session_key" (cgiget 'fbsession) args))))
  (stringout "http://api.facebook.com/restserver.php?"
    (do ((args args (cddr args)))
	((null? args) (printout))
      (printout (car args) "=" (cadr args) "&"))
    (printout "sig=" (downcase (packet->base16 (get-signature args))))))

(define (fbcall method . args)
  (jsonparse
   (get (urlget (fbcalluri method (cons* "format" "json" args)))
	'%content)))

(define (get-signature args)
  (let ((table (frame-create #f)))
    (do ((scan args (cddr scan)))
	((null? scan))
      (add! table (car scan) (cadr scan)))
    (md5 (stringout (doseq (key (lexsorted (getkeys table)))
		      (printout key "=" (get table key)))
	   (stringout apisecretkey)))))





