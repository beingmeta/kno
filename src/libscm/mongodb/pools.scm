;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'mongodb/pools)

(use-module '{mongodb logger})

(module-export! '{mgo/pool mgo/poolfetch
		  mgo/store! mgo/drop! mgo/add!
		  mgo/decache!
		  mgo/adjslot})

(define-init pool-table (make-hashtable))

;;; Basic methods for mongodb

(define (fetchfn oid/s collection)
  (if (vector? oid/s)
      (let ((values (mongodb/find collection
			`#[_id #[$oneof ,(elts oid/s)]
			   #[__index 0]]))
	    (result (make-vector (length oid/s) #f))
	    (map (make-hashtable)))
	(do-choices (value values) (store! map (get value '_id) value))
	(doseq (oid oid/s i)
	  (vector-set! result i (get map oid)))
	result)
      (mongodb/get collection oid/s #[return #[__index 0]])))

(define (mgo/pool/fetch oid/s collection) (fetchfn oid/s collection))

(define (allocfn n collection)
  (if (and (integer? n) (> n 0) (<= n 1024))
      (let* ((mod (mongodb/modify collection
		      #[_id "_pool"] 
		    `#[$inc #[load ,n]]
		    #[original #t]))
	     (before (get mod 'value))
	     (start (get before 'load))
	     (base (get before 'base))
	     (result {}))
	(dotimes (i n)
	  (set+! result (oid-plus base (+ i start))))
	(do-choices (oid result)
	  (mongodb/insert! collection `#[_id ,oid])
	  (set-oid-value! oid `#[_id ,oid]))
	result)
      (irritant n |BadAllocCount| "For pool in " collection)))

(define (mgo/pool/alloc n collection) (allocfn n collection))

;;; Declaring pools from MongoDB collections

(define (mgo/pool collection (base-arg #f) (cap-arg #f))
  (try (get pool-table (vector (mongodb/spec collection)
			       (collection/name collection)))
       (let* ((info (try (mongodb/get collection "_pool") #f))
	      (base (get-pool-base collection base-arg cap-arg))
	      (cap (get-pool-capacity collection base-arg cap-arg))
	      (pool (make-extpool (collection/name collection) base cap
				  mgo/pool/fetch 
				  #f ;; save
				  #f ;; lock
				  mgo/pool/alloc ;; alloc (mgo/pool/alloc)
				  collection ;; state
				  #t)))
	 (store! pool-table pool collection)
	 (store! pool-table (vector (mongodb/spec collection)
				    (collection/name collection)) 
		 pool)
	 pool)))

;;; Basic operations for OIDs in mongodb pools

(defambda (mgo/store! oid slotid values (pool) (collection))
  (set! pool (getpool oid))
  (set! collection (get pool-table pool))
  (if (or (fail? pool) (not pool))
      (irritant oid |No pool| mgo/store!)
      (if (fail? collection)
	  (irritant pool |Not A MongoDB pool| mgo/store!)
	  (update!
	   (mongodb/modify! collection 
	       `#[_id ,(if (ambiguous? oid)
			   `#[$oneof ,oid]
			   oid)]
	     `#[$set ,(if (ambiguous? slotid)
			  (get-store-modifier slotid values)
			  `#[,slotid ,values])]
	     #[new #t return #[__index 0]])))))

(defambda (get-store-modifier slotids values (result))
  (set! result #[])
  (do-choices (slotid slotids)
    (store! result slotid values))
  result)

(defambda (mgo/add! oid slotid values (pool) (collection))
  (set! pool (getpool oid))
  (set! collection (get pool-table pool))
  (if (or (fail? pool) (not pool))
      (irritant oid |No pool| mgo/store!)
      (if (fail? collection)
	  (irritant pool |Not A MongoDB pool| mgo/store!)
	  (update! (mongodb/modify! collection 
		       (if (ambiguous? oid)
			   `#[_id #[$in ,oid]]
			   `#[_id ,oid])
		     `#[$addToSet ,(get-add-modifier slotid values)]
		     #[new #t return #[__index 0]])))))

(defambda (get-add-modifier slotids values)
  (if (unique? slotids)
      `#[,slotids ,(if (unique? values) values `#[$each ,values])]
      (let ((q (frame-create #f)))
	(do-choices (slotid slotids)
	  (add! q slotid 
		(if (unique? values) values `#[$each ,values]))
	  q))))

(defambda (mgo/drop! oid slotid (values) (pool) (collection))
  (set! pool (getpool oid))
  (set! collection (get pool-table pool))
  (if (or (fail? pool) (not pool))
      (irritant oid |No pool| mgo/store!)
      (if (fail? collection)
	  (irritant pool |Not A MongoDB pool| mgo/store!)
	  (update!
	   (if (bound? values)
	       (mongodb/modify! collection `#[_id ,oid]
		 (if (unique? values)
		     `#[$pull ,(if (unique? slotid)
				   `#[,slotid ,values]
				   (get-store-modifier slotid values))]
		     `#[$pullAll ,(if (unique? slotid)
				      `#[,slotid ,values]
				      (get-store-modifier slotid values)
				      )])
		 #[new #t return #[__index 0]])
	       (mongodb/modify! collection 
		   `#[_id ,oid] (if (ambiguous? slotid)
				    (get-drop-all-modifier slotid)
				    `#[$unset #[,slotid 1]])
		   #[new #t return #[__index 0]]))))))

(define (get-drop-all-modifier slotids (result #[]))
  (do-choices (slotid slotids)
    (store! result slotid 1))
  result)

(define (mgo/decache! oid (slotid #f))
  (swapout oid))

(define (update! result (value #f))
  (when (test result 'value)
    (set! value (get result 'value))
    (set-oid-value! (get value '_id) value))
  (or value result))

;;; Defining adjunct slots of various kinds

(define-init adjunct-indexes (make-hashtable))

(define (mgo/adjslot pool slot qcoll query (extract '_id))
  (let ((spec (get adjunct-indexes (vector pool slot))))
    (debug%watch "MGO/ADJSLOT" spec pool slot (test adjunct-indexes spec))
    (if (and (exists? spec)
	     (equal? spec (vector pool slot qcoll query extract))
	     (test adjunct-indexes spec))
	(get adjunct-indexes spec)
	(let ((adjunct (make-adjslot pool slot qcoll query extract)))
	  (when (exists? spec)
	    (logwarn |AdjunctRedefine| "Redefining the adjunct slot " 
		     slot " of " pool " with difference parameters:"
		     "\n old: " spec
		     "\n new: " (get adjunct-indexes (vector pool slot)))
	    (drop! adjunct-indexes spec))
	  adjunct))))

(define (make-adjslot pool slot qcoll query extract)
  (info%watch "MAKE-ADJSLOT" pool slot qcoll query extract)
  (let* ((fetchfn (lambda (oid collection)
		    (if extract
			(get (mongodb/find collection (adjunct-query query oid)
			       `#[returns ,extract])
			     extract)
			(mongodb/find collection (adjunct-query query oid)))))
	 (coll (get pool-table pool))
	 (name 
	  (if (exists? coll) 
	      (glom (mongodb/name coll) "/" (collection/name coll) "/" slot)
	      (glom (pool-id pool) "/" slot)))
	 (adjunct (cons-extindex name fetchfn #f qcoll #t)))
    (info%watch "MAKE-ADJSLOT/setup" adjunct name coll fetchfn)
    (store! adjunct-indexes (vector pool slot qcoll query extract) adjunct)
    (store! adjunct-indexes (vector pool slot) 
	    (vector pool slot qcoll query extract))
    (use-adjunct adjunct slot pool)
    adjunct))

(defambda (adjunct-query query subjects)
  (cond ((fail? subjects) query)
	((ambiguous? query)
	 (for-choices (q query) (adjunct-query q subjects)))
	((or (oid? query) (symbol? query) (number? query) (string? query))
	 query)
	((or (slotmap? query) (schemap? query))
	 (let ((keys (getkeys query))
	       (copy (frame-create #f)))
	   (do-choices (key keys)
	     (let ((v (get query key)))
	       (if (identical? v '$subject)
		   (store! copy key
			   (if (ambiguous? subjects)
			       `#[$oneof ,subjects]
			       subjects))
		   (if (and (unique? v)
			    (or (oid? v) (symbol? v) (number? v) (string? v)))
		       (store! copy key v)
		       (store! copy key (adjunct-query v subjects))))))
	   copy))
	((vector? query) (map (lambda (e) (adjunct-query e subjects)) query))
	((pair? query) 
	 (cons (adjunct-query (car query) subjects)
	       (adjunct-query (cdr query) subjects)))
	((mongovec? query)
	 (let ((unpacked (unpack-compound query)))
	   (vector->compound
	    (map (lambda (e) (adjunct-query e subjects)) (cdr unpacked))
	    '%mongovec
	    #f #f)))
	(else query)))

;;; Handling mongo/pool arguments

(define (get-pool-base collection base cap (info))
  (set! info (try (mongodb/get collection "_pool") #f))
  (cond ((and (not info) (not base))
	 (irritant collection 
		   |BadMongoPool| "No pool info or specified base"))
	((and (not info) (not (oid? base)))
	 (irritant base |InvalidPoolBase| "For collection " collection))
	((not info)
	 (init-pool collection base cap)
	 base)
	((not (test info 'base))
	 (mongodb/update! collection #[_id "_pool"] `#[$set #[base ,base]])
	 base)
	((test info 'base base) base)
	(else (irritant base |InconsistentBase| 
			"Doesn't match " (get info 'base) 
			" in " info
			"for " collection))))
(define (get-pool-capacity collection base cap (info))
  (set! info (try (mongodb/get collection "_pool") #f))
  (cond ((and (not info) (not cap))
	 (irritant collection 
		   |BadMongoPool| "No pool info or specified capacity"))
	((and (not info) (not (and (integer? cap)
				   (> cap 0)
				   (<= cap (* 4 1000 1000 1000)))))
	 (irritant cap |InvalidPoolCapacity| "For collection " collection))
	((not info)
	 (init-pool collection base cap)
	 cap)
	((not (test info 'capacity))
	 (mongodb/update! collection #[_id "_pool"]
	   `#[$set #[capacity ,cap]])
	 cap)
	((test info 'capacity cap) cap)
	(else (irritant base |InconsistentCapacity| 
			"Doesn't match " (get info 'capacity) 
			" in " info
			"for " collection))))

(define (init-pool collection base cap)
  (mongodb/insert! collection
    `#[_id "_pool" base ,base capacity ,cap
       name ,(collection/name collection)
       load ,(get-init-load collection base)]))

(define (get-max-id collection)
  (largest (get (mongodb/find collection #[_id #[$type "objectId"]]
		  #[return #[_id: 1]])
		'_id)))
(define (get-init-load collection base)
  (1+ (try (oid-offset (get-max-id collection) base) 0)))


