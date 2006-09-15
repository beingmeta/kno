(in-module 'trackrefs)

(define (trackrefs thunk (trackfn #f))
   (let ((preoids (cached-oids))
	 (prekeys (cached-keys)))
     (let ((value (thunk)))
       (let ((loaded-oids (difference (cached-oids) preoids))
	     (loaded-keys (difference (cached-keys) prekeys)))
	 (cond ((trackfn (trackfn (qc loaded-oids) (qc loaded-keys))))
	       (else
		(when (exists? loaded-oids)
		  (message "Loaded " (choice-size loaded-oids) " oids: "
			   loaded-oids))
		(when (exists? loaded-keys)
		  (message "Loaded " (choice-size loaded-keys) " keys: "
			   loaded-keys)))))
       value)))

(module-export! 'trackrefs)



