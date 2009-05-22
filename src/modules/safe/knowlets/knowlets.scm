(in-module 'knowlets)

(use-module '{texttools ezrecords varconfig})
(use-module 'knowlets/drules)

(module-export!
 '{knowlet
   kno/dterm kno/dref kno/ref kno/probe knowlet?
   kno/add! kno/drop! kno/replace! kno/find
   knowlet-name knowlet-opts knowlet-language
   knowlet-oid knowlet-pool knowlet-index
   knowlet-alldterms knowlet-dterms knowlet-drules
   default-knowlet knowlets
   knowlet:pool knowlet:index knowlet:indices
   knowlet! ->knowlet iadd!
   langids})

(define-init knowlets (make-hashtable))

(define knowlet:pool (make-mempool "knowlets" @2009/0 (* 4 1024 1024)))
(define (knowlet-pool-config var (val))
  (if (bound? val)
      (set! knowlet:pool
	    (cond ((string? val) (use-pool val))
		  ((pool? val) val)
		  ((oid? val)
		   (try (get-pool val)
			(make-mempool "knowlets" val (* 1024 1024))))
		  (else (error "Not a valid knowlet pool"))))
      knowlet:pool))
(config-def! 'KNO:POOL knowlet-pool-config)

(define knowlet:index (make-hashtable))
(define (knowlet-index-config var (val))
  (if (bound? val)
      (begin
	(set! knowlet:index
	      (cond ((string? val) (open-index val))
		    ((index? val) val)
		    (else (make-hashtable))))
	(set+! knowlet:indices knowlet:index))
      knowlet:index))
(config-def! 'KNO:INDEX knowlet-index-config)

(define knowlet:indices knowlet:index)
(varconfig! KNO:INDICES knowlet:indices open-index choice)

(define knowlet:language 'en)
(varconfig! KNO:LANG knowlet:language)

(define langids (file->dtype (get-component "langids.dtype")))

(defrecord knowlet name
  (oid #f)
  (language 'en)
  (opts (make-hashset))
  ;; Where dterms within this knowlet are allocated
  (pool knowlet:pool)
  ;; Maps string dterms to OID dterms
  (dterms (make-hashtable))
  ;; Inverted index for dterms in the knowlet
  (index (make-hashtable))
  ;; GENLS index (index of the transitive closure of GENLS)
  ;;  (useful in inference and searching)
  (genls* (make-hashtable))
  ;; All dterms in this knowlet (a hashset)
  (alldterms (make-hashset))
  ;; Rules for disambiguating words into dterms
  (drules (make-hashtable)))

;;; Creating and referencing knowlets

(defslambda (new-knowlet name pool opts)
  (let* ((pool (use-pool pool))
	 (oid (frame-create pool 'knoname name '%id name))
	 (language (try (intersection opts langids) knowlet:language))
	 (new (cons-knowlet name oid language (choice->hashset opts))))
    (index-frame knowlet:index oid 'knoname name)
    (store! knowlets (choice oid name) new)
    new))

(define dont-index '{%id})

(defslambda (restore-knowlet oid)
  (let* ((name (get oid 'knoname))
	 (language (get oid 'language))
	 (opts (get oid 'opts))
	 (new (cons-knowlet name oid language (choice->hashset opts)
			    (try (getpool oid) knowlet:pool)))
	 (index (knowlet-index new))
	 (genls*index (knowlet-genls* new))
	 (drules (knowlet-drules new))
	 (kdterms (knowlet-dterms new)))
    (store! knowlets (choice oid name) new)
    (let ((dterms (find-frames knowlet:indices 'knowlet oid)))
      (prefetch-oids! dterms)
      (hashset-add! (knowlet-alldterms new) dterms)
      (do-choices (dterm dterms)
	(index-frame (knowlet-index new) dterm
	  (difference (getkeys dterm) dont-index))
	(add! kdterms (get dterm '{dterms dterm}) dterm)
	(add! genls*index (get* dterm 'genls) dterm)
	(do-choices (drule (get dterm 'drules))
	  (add! drules (drule-cues drule) drule))))
    new))

(define (knowlet name (pool knowlet:pool) (opts #{}))
  (try (tryif (knowlet? name) name)
       (get knowlets name)
       (let ((existing (find-frames knowlet:indices 'knoname name)))
	 (if (exists? existing)
	     (restore-knowlet existing)
	     (new-knowlet name pool (qc opts))))))

(define (knowlet! . args)
  (let ((kno (apply knowlet args)))
    (set! default-knowlet kno)
    kno))

(define (->knowlet object)
  (try (tryif (knowlet? object) object)
       (tryif (and (oid? object) (test object 'knowlet))
	 (get knowlets (get object 'knowlet)))
       (get knowlets object)
       (restore-knowlet
	(find-frames knowlet:indices 'knoname name))))

;;; Creating and referencing dterms

(define default-knowlet #f)
(varconfig! knowlet default-knowlet knowlet)

(define (kno/dterm term (knowlet default-knowlet))
  (try (get (knowlet-dterms knowlet) term)
       (new-dterm term knowlet)))
(define (new-dterm term knowlet)
  (let ((f (frame-create knowlet:pool
	     'knowlet (knowlet-oid knowlet)
	     'dterm term 'dterms term
	     (knowlet-language knowlet) term
	     '%id term)))
    (store! (knowlet-dterms knowlet) term f)
    (hashset-add! (knowlet-alldterms knowlet) f)
    (index-frame knowlet:index f '{dterm dterms knowlet})
    (index-frame (knowlet-index knowlet) f '{dterm dterms})
    (index-frame (knowlet-index knowlet)
	f (knowlet-language knowlet))
    f))

(define (kno/dref term (knowlet default-knowlet) (create #t))
  (try (get (knowlet-dterms knowlet) term)
       (tryif create (kno/dterm term knowlet))))

(define (kno/probe term (knowlet default-knowlet))
  (get (knowlet-dterms knowlet) term))

(define (kno/ref term (knowlet default-knowlet) (lang) (tryhard #f))
  (default! lang (knowlet-language knowlet))
  (try (find-frames (knowlet-index knowlet) lang term)
       (tryif tryhard
	      (find-frames (knowlet-index knowlet)
		lang (choice (metaphone term #t)
			     (string->packet (disemvowel term)))))))

;;; String indexing

(define (dedash string)
  (tryif (position #\- string)
	 (choice (string-subst string "-" " " )
		 (string-subst string "-" ""))))

(define (kno/index-string f slotid (value) (index))
  (default! value (get f slotid))
  (default! index (get-index f))
  (let* ((values (stdspace value))
	 (expvalues (choice values (basestring values)))
	 (normvalues (capitalize (pick expvalues somecap?)))
	 (indexvals (choice expvalues normvalues (dedash normvalues)))
	 (shortvals (pick (choice values normvalues) length > 1))
	 (metavals (metaphone shortvals #t)))
    (add! index (cons slotid indexvals) f)
    (add! index (cons slotid metavals) f)))

;;; Find and edit operations on dterms

(defambda (kno/find . args)
  (let ((knowlet (if (even? (length args))
		     default-knowlet
		     (car args)))
	(args (if (even? (length args)) args (cdr args))))
    (apply find-frames (knowlet-index knowlet) args)))

(define infer-onadd (make-hashtable))
(define infer-ondrop (make-hashtable))

(defambda (kno/add! dterm slotid value)
  (let* ((cur (get dterm slotid))
	 (new (difference value cur))
	 (knowlet (get knowlets (get dterm 'knowlet))))
    (when (exists? new)
      (add! dterm slotid new)
      (if (overlaps? slotid langids)
	  (kno/index-string dterm slotid new (knowlet-index knowlet))
	  (index-frame (knowlet-index knowlet) dterm slotid new))
      (unless (exists? cur)
	(index-frame (knowlet-index knowlet) dterm 'has slotid))
      ((get infer-onadd slotid) dterm slotid new))))

(defambda (kno/drop! dterm slotid value)
  (let ((drop (intersection value (get dterm slotid)))
	(knowlet (get knowlets (get dterm 'knowlet))))
    (when (exists? drop)
      (drop! dterm slotid drop)
      (drop! (knowlet-index knowlet) (cons slotid new) dterm)
      ((get infer-ondrop slotid) dterm slotid drop))))

(defambda (kno/replace! dterm slotid value (toreplace {}))
  (for-choices dterm
    (for-choices slotid
      (let ((replace (difference (try toreplace (get dterm slotid))
				 value)))
	(let ((new (difference value (get dterm slotid)))
	      (todrop replace)
	      (knowlet (get knowlets (get dterm 'knowlet))))
	  (when (exists? todrop)
	    (drop! dterm slotid todrop)
	    ((get infer-ondrop slotid) dterm slotid todrop)
	    (drop! (knowlet-index knowlet) (cons slotid todrop)
		   todrop))
	  (when (exists? new)
	    (add! dterm slotid new)
	    (index-frame (knowlet-index knowlet) dterm slotid new)
	    ((get infer-onadd slotid) dterm slotid new)))))))

;;; Special inference methods

(define (add-genl! f s g)
  (let ((g* (get* g 'genls))
	(g*cur (get f 'genls*))
	(knowlet (get knowlets (get f 'knowlet))))
    (add! f 'genls* g)
    (add! f 'genls* (difference g* g*cur))
    (add! (knowlet-genls* knowlet) (difference g* g*cur) f)))

(define (drop-genl! f s g)
  ;; This is called after the drop happens, so g*cur actually reflects
  ;;  the update.
  (let ((g* (get* g 'genls))
	(g*cur (get f 'genls*))
	(knowlet (get knowlets (get f 'knowlet)))
	(g*drop (difference g* g*cur))
	(g*index (knowlet-genls* knowlet)))
    (drop! f 'genls* g)
    (drop! f 'genls* g*drop)
    (drop! g*index g*drop f)
    (do-choices (specl (get g*index f))
      (let* ((g*cur (get* specl 'genls))
	     (g*drop (difference g*drop g*cur)))
	(when (exists? g*drop)
	  (drop! specl 'genls* g*drop)
	  (drop! g*index g*drop specl))))))

(add! infer-onadd 'genls add-genl!)
(add! infer-ondrop 'genls drop-genl!)

;;; Specls (just the inverse)

(define (add-specl! f s v)
  (add! v 'genls f)
  (add-genl! v 'genls f))

(define (drop-specl! f s g)
  (drop! v 'genls f)
  (drop-genl! v 'genls f))

(add! infer-onadd 'specls add-specl!)
(add! infer-ondrop 'specls drop-specl!)

;;; Symmetric

(define (add-symmetric! frame slotid value (mirror))
  (default! mirror slotid)
  (unless (test value mirror frame)
    (kno/add! value mirror frame)))

(define (drop-symmetric! frame slotid value (mirror))
  (default! mirror slotid)
  (when (test value mirror frame)
    (kno/drop! value mirror frame)))

(add! infer-onadd '{mirror equivalent identical} add-symmetric!)
(add! infer-ondrop '{mirror equivalent identical} drop-symmetric!)

;;; DRULES

(define (add-drule! frame slotid value)
  (add! (knowlet-drules (get knowlets (get frame 'knowlet)))
	(drule-cues value)
	value))
(define (drop-drule! frame slotid value)
  (drop! (knowlet-drules (get knowlets (get frame 'knowlet)))
	 (drule-cues value)
	 value))
(store! infer-onadd 'drules add-drule!)
(store! infer-ondrop 'drules drop-drule!)

;;; IADD!

(define (get-index f)
  (try (knowlet-index (get knowlets (get f 'knowlet)))
       knowlet:index))

(defambda (iadd! f slotid value (index))
  (if (bound? index)
      (if (singleton? slotid)
	  (begin (add! index (cons 'has slotid) (reject f slotid))
		 (add! f slotid value)
		 (add! index (cons slotid value) f))
	  (do-choices slotid
	    (add! index (cons 'has slotid) (reject f slotid))
	    (add! f slotid value)
	    (add! index (cons slotid value) f)))
      (let ((kindex (get-index f)))
	(if (singleton? kindex)
	    (do-choices slotid
	      (add! kindex (cons 'has slotid) (reject f slotid))
	      (add! f slotid value)
	      (add! kindex (cons slotid value) f))
	    (do-choices f
	      (let ((kindex (get-index f)))
		(do-choices slotid
		  (add! kindex (cons 'has slotid) (reject f slotid))
		  (add! f slotid value)
		  (add! index (cons slotid value) f))))))))


