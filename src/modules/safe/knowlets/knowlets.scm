(in-module 'knowlets)

(use-module '{texttools ezrecords varconfig})

(module-export!
 '{knowlet
   kno/dterm kno/dref kno/ref
   kno/add! kno/drop! kno/find
   knowlet-name knowlet-opts knowlet-language
   knowlet-oid knowlet-index knowlet-dterms
   default-knowlet
   langids})

(define-init knowlets (make-hashtable))

(define knowlet:pool #f)
(varconfig! KNO:POOL knowlet:pool use-pool)
(define knowlet:index #f)
(varconfig! KNO:INDEX knowlet:index open-index)
(define knowlet:language 'en)
(varconfig! KNO:LANG knowlet:language)

(define langids (file->dtype (get-component "langids.dtype")))

(defrecord knowlet name
  (oid #f)
  (language 'en)
  (opts (make-hashset))
  ;; Whether to allow non-dterms in references
  (strict #f)
  ;; Maps string dterms to OID dterms
  (dterms (make-hashtable))
  ;; Inverted index for dterms in the knowlet
  (index (make-hashtable))
  ;; Terms which are assumed unique by kno/dref
  (unique (make-hashset))
  ;; Rules for disambiguating words into dterms
  (drules (make-hashtable)))

;;; Creating and referencing knowlets

(defslambda (new-knowlet name pool opts)
  (let* ((pool (use-pool pool))
	 (oid (frame-create pool 'knoname name '%id name))
	 (language (try (intersection opts langids) knowlet:language))
	 (strict (overlaps? opts 'strict))
	 (new (cons-knowlet name oid language (choice->hashset opts) strict)))
    (index-frame knowlet:index oid 'knoname name)
    (store! knowlets (choice oid name) new)
    new))

(define dont-index '{%id})

(defslambda (restore-knowlet oid)
  (let* ((name (get oid 'knoname))
	 (language (get oid 'language))
	 (opts (get oid 'opts))
	 (strict (overlaps? opts 'strict))
	 (new (cons-knowlet name oid language (choice->hashset opts) strict)))
    (store! knowlets (choice oid name) new)
    (let ((dterms (find-frames knowlet:index 'knowlet oid)))
      (do-choices (dterm terms)
	(index-frame (knowlet-indices new) dterm
	  (difference (getkeys dterm) dont-index))
	(index-frame (knowlet-indices new) dterm
	  'genls (get* dterm 'genls))))
    new))

(define (knowlet name (pool knowlet:pool) (opts #{}))
  (try (get knowlets name)
       (let ((existing (find-frames knowlet:index 'knoname name)))
	 (if (exists? existing)
	     (restore-knowlet existing)
	     (new-knowlet name pool (qc opts))))))

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
    (index-frame (knowlet-index knowlet) f '{dterm dterms})
    (index-frame (knowlet-index knowlet)
	f (knowlet-language knowlet))
    f))

(define (kno/dref term (knowlet default-knowlet) (create #t))
  (try (get (knowlet-dterms knowlet) term)
       (if (knowlet-strict knowlet) (fail)
	   (let* ((lang (knowlet-language knowlet))
		  (poss (find-frames (knowlet-index knowlet) lang term)))
	     (tryif (singleton? poss)
		    (begin (hashset-add! (knowlet-unique knowlet) term)
			   poss))))
       (tryif create (kno/dterm term knowlet))))

(define (kno/ref term (knowlet default-knowlet) (lang) (tryhard #f))
  (default! lang (knowlet-language knowlet))
  (try (find-frames (knowlet-index knowlet) lang term)
       (tryif tryhard
	      (find-frames (knowlet-index knowlet)
		lang (choice (metaphone term #t)
			     (string->packet (disemvowel term)))))))

;;; Find and edit operations on dterms

(defambda (kno/find . args)
  (let ((knowlet (if (even? (length args))
		     default-knowlet
		     (car args)))
	(args (if (even? (length args)) args (cdr args))))
    (apply find-frames (knowlet-index knowlet) args)))

(defambda (kno/add! dterm slotid value)
  (let ((new (difference value (get dterm slotid)))
	(knowlet (get knowlets (get dterm 'knowlet))))
    (add! dterm slotid new)
    (index-frame (knowlet-index knowlet) dterm slotid new)))

(defambda (kno/drop! dterm slotid value)
  (let ((drop (intersection value (get dterm slotid)))
	(knowlet (get knowlets (get dterm 'knowlet))))
    (drop! dterm slotid drop)
    (drop! (knowlet-index knowlet) (cons slotid new) dterm)))

