(config! 'loadpath "brico=../src/brico")

(define (have-brico)
  (let ((source (config 'bricosource)))
    (and (string? source)
	 (or (position #\@ source)
	     (position #\: source )
	     (if (file-directory? source)
		 (file-exists? (mkpath source "brico.pool"))
		 (file-exists? source)))
	 (onerror (begin (get-module 'brico) #t) #f))))

(when (have-brico)
  (check-modules '{brico brico/dterms brico/indexing brico/lookup
		   brico/analytics brico/maprules brico/xdterms
		   brico/build/wordnet brico/utils/rdf brico/utils/audit
		   knodules/usebrico knodules/defterm
		   xtags}))
