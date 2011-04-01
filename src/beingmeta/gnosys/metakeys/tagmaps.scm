(in-module 'gnosys/metakeys/tagmaps)

(use-module '{brico morph})
(use-module '{gnosys gnosys/metakeys})

(module-export! '{tagmap tagmap/link! tagmap/find tagmap/getconcepts})
(module-export! 'tagmap/lookup)
(module-export! 'tagmapindex)
(module-export! 'keyentry/usetagmaps)

(define tagmapdb #f)
(define tagmappool #f)
(define tagmapindex #f)

(define provider-map (make-hashtable))

(define tagmap
  (ambda (tags user provider)
    (let ((pool (try (car (get provider-map provider)) tagmappool))
	  (index (try (cdr (get provider-map provider)) tagmapindex))
	  (source (try user 'universal))
	  (tags (difference tags "")))
      (try (let ((candidates (find-frames index
			       'tags tags 'source source 'provider provider)))
	     (do-choices (tag tags)
	       (set! candidates (intersection candidates
					      (find-frames index 'tags tag))))
	     candidates)
	   (new-tagmap pool index (qc tags) source provider)))))

(define new-tagmap
  (slambda (pool index tags source provider)
    (try (let ((candidates (find-frames index
			     'tags tags 'source source 'provider provider)))
	   (do-choices (tag tags)
	     (set! candidates (intersection candidates
					    (find-frames index 'tags tag))))
	   candidates)
	 (let ((f (frame-create pool
		    'type 'tagmap
		    'tags tags 'source source 'provider provider
		    '%id (stringout
			     (do-choices (tag tags i)
			       (printout (if (> i 0) ";") tag))
			   "~" (get source 'email) "@"
			   provider))))
	   (index-frame index f '{tags source provider})
	   f))))

(define (tagmap/link! tagmap concept)
  (let ((index (try (cdr (get provider-map (get tagmap 'provider)))
		    tagmapindex)))
    (assert! tagmap @?genls concept)
    (index-frame index tagmap @?genls concept)
    (index-frame index tagmap @?genls* (get concept @?genls*))))

(define (tagmap/find concept (source #f) (provider #f))
  (let ((index (try (cdr (get provider-map provider)) tagmapindex)))
    (if provider
	(if source
	    (find-frames index
	      @?genls* concept 'source source 'provider provider)
	    (find-frames index
	      @?genls* concept 'provider provider))
	(if source
	    (find-frames index @?genls* concept 'source source)
	    (find-frames index @?genls* concept)))))

(define tagmap/getconcepts
  (ambda (tags source provider)
    (let* ((index (try (cdr (get provider-map provider)) tagmapindex))
	   (tagmaps (if (exists? source)
			(find-frames index 'tags tags
				     'source source
				     'provider provider)
			(find-frames index 'tags tags
				     'provider provider))))
      (for-choices (tagmap tagmaps)
	(tryif (fail? (difference (get tagmap 'tags) tags))
	       (get tagmap @?implies))))))

(define (tagmap/lookup tagmap (domain #f))
  (let* ((module (get-module (get tagmap 'provider)))
	 (method (get module 'lookupmethod)))
    (cond ((not domain)
	   (method (get tagmap 'tags) (get tagmap 'source)))
	  ((identical? domain 'any)
	   (method (get tagmap 'tags) #f))
	  (else (method (get tagmap 'tags) domain)))))

(define (keyentry/usetagmaps keyentry (user #f))
  (let ((base (keyentry-base keyentry))
	(language (keyentry-language keyentry)))
    (keyentry/assign
     keyentry
     (if user
	 (qc (find-frames tagmapindex
	       'tags (choice base (get-verb-root base language)
			     (get-noun-root base language))))
	 (qc (find-frames tagmapindex
	       'tags (choice base (get-verb-root base language)
			     (get-noun-root base language))
	       'source user))))))

(define tagmapdb-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) urldb)
	  ((equal? tagmapdb val))
	  (else (message "Setting federated tag db to " val)
		;; This will need to be put in appropriately if we want to use
		;;   a remote tagmap server.
		;; (when (position #\@ val) (set! newurlfn (dtproc 'new-tagmap val)))
		(set! tagmappool (use-pool val))
		(set! tagmapindex (open-index val))
		(set! tagmapdb val)))))
(config-def! 'tagmapdb tagmapdb-config)

