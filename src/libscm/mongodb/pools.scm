;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'mongodb/pools)

(use-module '{mongodb ezrecords varconfig logger})

(module-export! '{mongopool/open mongopool/make
		  mongopool? mongo/convert})

(module-export! '{mgo/pool mgo/poolfetch
		  mgo/store! mgo/drop! mgo/add!
		  mgo/decache!
		  mgo/adjslot})

(define (mongopool->string f)
  (stringout "#<MONGOPOOL "
    (mongopool-server f) "/" 
    (mongopool-dbname f) " "
    (mongopool-cname f) " "
    (oid->string (mongopool-base f)) "+" 
    (mongopool-capacity f) ">"))

(define-init mongopools (make-hashtable))

(defrecord (mongopool mutable opaque 
		      #[predicate ismongopool?] 
		      `(stringfn . mongopool->string))
  collection server dbname cname base capacity (opts #f)
  (originals (make-hashtable))
  (lock (make-condvar)))

(define (mongopool? x) (and (pool? x) (test mongopools x)))

;;; Converting mongodb objects to FramerD (mostly choices)
(define (mongo/convert object)
  (do-choices (assoc (getassocs object))
    (when (vector? (cdr assoc))
      (store! object (car assoc) (elts (cdr assoc)))))
  object)

;;; Basic methods for mongodb

(define (mongopool-fetch pool mp oid (collection))
  (default! collection (mongopool-collection mp))
  (mongo/convert (mongodb/get collection oid #[return #[__index 0]])))

(define (mongopool-fetchn pool mp oidvec (collection))
  (default! collection (mongopool-collection mp))
  (let ((values (mongodb/find collection
		    `#[_id #[$oneof ,(elts oidvec)]
		       #[__index 0]]))
	(result (make-vector (length oidvec) #f))
	(map (make-hashtable)))
    (do-choices (value values) 
      (store! map (get value '_id) value))
    (doseq (oid oidvec i)
      (vector-set! result i (mongo/convert (get map oid))))
    result))

(define (mongopool-lockoids pool mp oids (collection))
  (default! collection (mongopool-collection mp))
  (let ((current (mongodb/find collection
		     `#[_id #[$oneof ,oids]
			#[__index 0]]))
	(originals (mongopool-originals mp)))
    (do-choices (entry current)
      (store! originals (get entry '_id) (deep-copy entry)))
    #t))

(define (mongopool-releaseoids pool mp oids (originals))
  (default! originals (mongopool-originals mp))
  (drop! originals oids)
  #t)

(define (mongopool-alloc pool mp n)
  (if (and (integer? n) (> n 0) (<= n 1024))
      (with-lock (mongopool-lock mp)
	(let* ((collection (mongopool-collection mp))
	       (mod (mongodb/modify collection
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
	  result))
      (irritant n |BadAllocCount| "For pool in " collection)))

(define (mongopool-load pool mongopool)
  (let* ((collection (mongopool-collection mongopool))
	 (info (mongodb/get collection "_pool")))
    (try (get info 'load)
	 (irritant pool |MongoPoolNotIntialized|))))

(define (mongopool-ctl pool mp op . args)
  (cond ((and (eq? op 'collection) (null? args))
	 (mongopool-collection mp))
	((eq? op 'cachelevel) 1)
	(else (apply poolctl/default pool op args))))

(define (mongopool-commit pool mp phase oidvec valvec metadata) 
  (info%watch "MONGOPOOL-COMMIT" 
    pool mp phase "N" (length oidvec)
    metadata)
  (when (eq? phase 'write)
    (let ((collection (mongopool-collection mp))
	  (originals (mongopool-originals mp)))
      (dotimes (i (length oidvec))
	(let* ((oid (elt oidvec i))
	       (cur (get originals oid))
	       (new (elt valvec i)))
	  (debug%watch oid cur new (modified? new))
	  (let ((sets `#[]) (unsets `#[]) (adds `#[]) (drops `#[]))
	    (do-choices (slotid (getkeys {cur new}))
	      (let ((curv (get cur slotid)) (newv (get new slotid)))
		(cond ((identical? curv newv))
		      ((fail? curv)
		       (store! sets slotid (choice->vector newv)))
		      ((fail? newv) (store! unsets slotid ""))
		      ((and (singleton? curv) (singleton? newv))
		       (store! sets slotid newv))
		      (else (let ((toadd (difference newv curv))
				  (todrop (difference curv newv)))
			      (when (exists? toadd)
				(store! adds slotid `#[$each ,(choice->vector toadd)]))
			      (when (exists? todrop)
				(store! drops slotid (choice->vector todrop))))))))
	    (mongodb/modify! collection `#[_id ,oid]
	      (frame-create #f
		'$set (tryif (> (table-size sets) 0) sets)
		'$addToSet (tryif (> (table-size adds) 0) adds)
		'$pullAll (tryif (> (table-size drops) 0) drops)
		'$unset (tryif (> (table-size unsets) 0) unsets))))
	  (store! originals oid new)))))
  #t)

;;; Opening and initializing mongodb-backed pools

;; This is called by the slambda init-mongopool (below) to create the
;; mongopool entry for a given OID pool stored in MongoDB.
(define (init-mongopool-inner collection (opts #f) (cname))
  (default! cname (collection/name collection))
  (try (get mongopools `#(,(mongodb/getdb collection) ,cname))
       (get mongopools `#(,(mongodb/spec collection) 
			  ,(mongodb/name collection)
			  ,cname))
       (let* ((info (mongodb/get collection "_pool"))
	      (metadata (try (mongodb/get collection "_metadata") #[]))
	      (collname (collection/name collection))
	      (base (get info 'base))
	      (cap (get info 'capacity))
	      (load (try (get info 'load) 0))
	      (name (try (get info 'name) cname)))
	 (cond ((and (exists? base) (exists? cap)))
	       ((not (getopt opts 'create)) #f)
	       ((not (getopt opts 'base))
		(irritant collection |NoBaseOID|))
	       (else (set! base (getopt opts 'base))
		     (set! cap (getopt opts 'capacity #1mib))
		     (set! load (getopt opts 'load 0))
		     (set! name (collection/name collection))
		     (mongodb/insert! collection
		       `#[_id "_pool" base ,base capacity ,cap
			  name ,name load ,load])))
	 (let* ((opts (opts+ `#[type mongopool metadata ,metadata] 
			     opts))
		(record (cons-mongopool collection (mongodb/spec collection)
					(collection/name collection)
					base cap opts))
		(pool (make-procpool name base cap opts record load)))
	   (store! mongopools collection pool)
	   (store! mongopools
	     {(vector (mongodb/getdb collection) cname)
	      (vector (mongodb/spec collection) (mongodb/name collection)
		      cname)}
	     pool)
	   (store! mongopools pool record)
	   pool))))
(define-init init-mongopool
  (slambda (collection (opts #f))
    (init-mongopool-inner collection opts)))

(define (mongopool/open spec (opts #f) (collection))
  (default! collection 
    (if (mongodb/collection? spec) spec 
	(mongodb/collection spec (getopt opts 'name) opts)))
  (init-mongopool collection opts))
(define (mongopool/make spec (opts #f) (collection))
  (default! collection 
    (if (mongodb/collection? spec) spec 
	(mongodb/collection spec (getopt opts 'name) opts)))
  (init-mongopool collection (opts+ #[create #t] opts)))

(defpooltype 'mongopool
  `#[open ,mongopool/open
     create ,mongopool/make
     alloc ,mongopool-alloc
     getload ,mongopool-load
     fetch ,mongopool-fetch
     fetchn ,mongopool-fetchn
     lockoids ,mongopool-lockoids
     releaseoids ,mongopool-releaseoids
     poolctl ,mongopool-ctl
     commit ,mongopool-commit])

;;; Basic operations for OIDs in mongodb pools

(defambda (mgo/store! oid slotid values (pool) (mp) (collection))
  (set! pool (getpool oid))
  (set! mp (get mongopools pool))
  (set! collection (mongopool-collection mp))
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

(defambda (mgo/add! oid slotid values (pool) (mp) (collection))
  (set! pool (getpool oid))
  (set! mp (get mongopools pool))
  (set! collection (mongopool-collection mp))
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

(defambda (mgo/drop! oid slotid (values) (pool) (mp) (collection))
  (set! pool (getpool oid))
  (set! mp (get mongopools pool))
  (set! collection (mongopool-collection mp))
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
    (%set-oid-value! (get value '_id) value))
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
	 (coll (get mongopools pool))
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
