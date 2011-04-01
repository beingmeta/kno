(in-module 'gnosys/getrelated)

(use-module '{gnosys gnosys/disambiguate brico cachequeue})

(define getrelated-cache #f)
(define getrelated-precache #f)
(define getrelated-queue #f)

(module-export! '{getrelated tryrelated getrelated/prefetch!})

;;; We exclude time concepts

(define time-concepts
  (choice
   (file->dtype (get-component "timeconcepts.dtype"))
   (?? @?genls
       @/brico/23c1a(NOUN.TIME "Gregorian calendar month" GENLS "month"))
   (difference
    (?? @?implies
	(list {@/brico/d92c(NOUN.TIME "year" GENLS "period")
               @/brico/13e17(NOUN.TIME "civil day" GENLS "period")}))
    (?? @?genls
	(list {@/brico/d92c(NOUN.TIME "year" GENLS "period")
	       @/brico/13e17(NOUN.TIME "civil day" GENLS "period")})))))

;;; Getting related concepts

(define (getrelated/prefetch! concept (testing #f) (corresort #f))
  (when testing (prefetch-oids! (choice refterms sumterms)))
  (prefetch-oids! concept)
  (prefetch-keys! (cons (choice /sumterms /refterms sumterms refterms
				partof memberof /relterms
				@/brico/2c277{STUFF-OF}
				/always /commonly /somenot)
			concept))
  (when corresort
    (prefetch-keys!
     (for-choices concept
       (let* ((defs (get concept sumterms))
	      (refs (get concept refterms))
	      (refdef (intersection refs (?? sumterms concept))))
	 (cons refterms (choice defs refdef))))))) 

(define (getrelated-core concept (corresort #f))
  (let* ((defs (get concept @?sumterms))
	 (refs (get concept @?refterms))
	 (defdef (intersection defs (?? @?sumterms concept)))
	 (defref (intersection defs (?? @?refterms concept)))
	 (refdef (intersection refs (?? @?sumterms concept)))
	 (refref (intersection refs (?? @?refterms concept)))
	 (predicates (get concept {@?always @?commonly @?rarely @?sometimes @?never @?somenot}))
	 (relations (get concept @?relterms))	 
	 (sortfn (if corresort
		     (lambda (c) (disambig/correlate c concept @?refterms))
		     absfreqs)))
    (append (rsorted (difference defdef time-concepts) sortfn)
	    (rsorted (difference defref defdef time-concepts) sortfn)
	    (rsorted (difference refdef defref defdef time-concepts)
		     sortfn)
	    (rsorted (difference defs defref refdef defdef time-concepts)
		     sortfn)
	    (rsorted predicates sortfn)
	    (rsorted relations sortfn)
	    (rsorted (difference refref defs defref refdef
				 defdef time-concepts)
		     sortfn))))

(define (compute-related c)
  (if (and getrelated-precache (test getrelated-precache c))
      (get getrelated-precache c)
      (begin
	(getrelated/prefetch! c)
	(getrelated-core c))))

(defslambda (init-queue)
  (cond ((cachequeue? getrelated-queue) getrelated-queue)
	(else
	 (unless getrelated-cache (set! getrelated-cache (make-hashtable)))
	 (let ((queue (cachequeue getrelated-cache
				  (config 'getrelatedthreads 4)
				  (config 'getrelatedmeltpoint #f))))
	   (set! getrelated-queue queue)
	   queue))))

(define (getrelated c (usequeue getrelated-queue))
  (if usequeue
      (if (cachequeue? getrelated-queue)
	  (cq/require getrelated-queue compute-related c)
	  (cq/require (init-queue) compute-related c))
      (compute-related c)))

(define (tryrelated c)
  (try (tryif getrelated-precache (get getrelated-precache c))
       (if (cachequeue? getrelated-queue)
	   (cq/get getrelated-queue compute-related c)
	   (cq/get (init-queue) compute-related c))))

;;; Configuration

(config-def! 'getrelatedprecache
	     (lambda (var (val))
	       (cond ((not (bound? val)) getrelated-precache)
		     ((table? val) (set! getrelated-precache val))
		     ((string? val)  (set! getrelated-precache (open-index val)))
		     (else (error "Not a valid precache table" val)))))

(config-def! 'getrelatedqueue
	     (lambda (var (val))
	       (cond ((not (bound? val)) getrelated-queue)
		     ((number? val)
		      (unless getrelated-queue (set! getrelated-queue #t))
		      (config! 'getrelatedthreads val))
		     ((table? val)
		      (unless getrelated-queue (set! getrelated-queue #t))
		      (set! getrelated-cache val))
		     ((string? val)
		      (unless getrelated-queue (set! getrelated-queue #t))
		      (set! getrelated-cache (open-index val)))
		     (else (error "Not a valid precache table" val)))))


