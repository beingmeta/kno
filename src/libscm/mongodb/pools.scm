;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'mongodb/pools)

(use-module '{mongodb ezrecords varconfig logger})

(define-init %loglevel %notice%)
;;(set! %loglevel %notice%)

(module-export! '{mongopool/open mongopool/make mongodb/pool
		  mongopool? mongopool/collection mongodb/convert
		  mongodb/intern
		  mongodb/invert})

(module-export! '{mgo/pool mgo/poolfetch
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
  collection server dbname cname base capacity 
  (opts #f) (slotinfo `#[])
  (originals (make-hashtable))
  (lock (make-condvar)))

(define (mongopool? x) (and (pool? x) (test mongopools x)))

(define (mongopool/collection pool)
  (mongopool-collection (get mongopools pool)))

#|
;;; Converting mongodb objects to Kno (mostly choices)
(define (mongodb/convert object (slotinfo {}))
  (do-choices (assoc (getassocs object))
    (when (and (vector? (cdr assoc)) 
	       (not (test slotinfo (car assoc) 'singleton)))
      (store! object (car assoc) (elts (cdr assoc)))))
  object)

(define (mongodb/invert object (slotinfo {}))
  (do-choices (assoc (getassocs object))
    (unless (test slotinfo (car assoc) 'singleton)
      (store! object (car assoc) (choice->vector (cdr assoc)))))
  object)
|#

;;; Basic methods for mongodb

(define (mongopool-fetch pool mp oid (collection))
  (default! collection (mongopool-collection mp))
  (collection/get collection oid #[return #[__index 0]]))

(define (fetchn collection oidvec)
  (if (= (length oidvec) 1)
      (collection/find collection `#[_id ,(first oidvec)]
	#[__index 0])
      (if (zero? (length oidvec)) {}
	  (collection/find collection `#[_id #[$in ,(elts oidvec)]]
	    #[__index 0]))))

(define (mongopool-fetchn pool mp oidvec (collection))
  (default! collection (mongopool-collection mp))
  (let ((values (fetchn collection oidvec))
	(result (make-vector (length oidvec) #f))
	(map (make-hashtable)))
    (do-choices (value values)
      (store! map (get value '_id) value))
    (doseq (oid oidvec i)
      (vector-set! result i (get map oid)))
    result))

(define (mongopool-lockoids pool mp oids (collection))
  (default! collection (mongopool-collection mp))
  (let ((current (fetchn collection (choice->vector oids)))
	(originals (mongopool-originals mp)))
    (info%watch "MONGPOOL/lockoids" current)
    (do-choices (cur current)
      (store! originals (get cur '_id) (deep-copy cur)))
    #t))

(define (mongopool-releaseoids pool mp oids (originals))
  (default! originals (mongopool-originals mp))
  (drop! originals oids)
  #t)

(define (mongopool-alloc pool mp n)
  (if (and (integer? n) (> n 0) (<= n 1024))
      (with-lock (mongopool-lock mp)
	(let* ((collection (mongopool-collection mp))
	       (mod (collection/modify collection
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
	    (collection/insert! collection `#[_id ,oid])
	    (set-oid-value! oid `#[_id ,oid]))
	  result))
      (irritant n |BadAllocCount| "For pool in " collection)))

(define (mongopool-load pool mongopool)
  (let* ((collection (mongopool-collection mongopool))
	 (info (collection/get collection "_pool")))
    (try (get info 'load)
	 (irritant pool |MongoPoolNotIntialized|))))

(define (mongopool-ctl pool mp op . args)
  (cond ((and (eq? op 'collection) (null? args))
	 (mongopool-collection mp))
	((eq? op 'cachelevel) 1)
	(else (apply poolctl/default pool op args))))

(define (mongopool-commit pool mp phase oidvec valvec metadata) 
  (info%watch "MONGOPOOL-COMMIT" 
    pool mp phase 
    "LEN(O)" (if (vector? oidvec) (length oidvec) oidvec)
    "LEN(V)" (if (vector? valvec) (length valvec) valvec)
    metadata)
  (when (eq? phase 'write)
    (let ((collection (mongopool-collection mp))
	  (originals (mongopool-originals mp)))
      (info%watch "MONGPOOL-COMMIT/WRITE" collection originals)
      (dotimes (i (length oidvec))
	(unless (void? (elt valvec i))
	  (let* ((oid (elt oidvec i))
		 (cur (get originals oid))
		 (new (elt valvec i)))
	    (debug%watch "MONGPOOL-COMMIT/DIFFER" 
	      oid cur new "CUR#" (hashptr cur) "NEW#" (hashptr new)
	      "MODIFIED" (modified? new))
	    (let ((sets (frame-create #f))
		  (unsets (frame-create #f))
		  (adds (frame-create #f))
		  (drops (frame-create #f)))
	      (do-choices (slotid (difference (getkeys {cur new}) '{_id modified}))
		(let ((curv (get cur slotid))
		      (newv (get new slotid)))
		  (detail%watch "COMMIT/COMPARE" oid slotid curv newv
				"CURV#" (hashptr curv)
				"NEWV#" (hashptr newv))
		  (cond ((identical? curv newv))
			((fail? curv)
			 (store! sets slotid (qc newv)))
			((fail? newv) (store! unsets slotid ""))
			((singleton? newv)
			 (store! sets slotid newv))
			(else (let ((toadd (difference newv curv))
				    (todrop (difference curv newv)))
				(if (exists? toadd)
				    (if (and (ambiguous? curv) (fail? todrop))
					(store! adds slotid `#[$each ,toadd])
					(store! sets slotid (choice toadd curv)))
				    (when (exists? todrop)
				      (store! drops slotid (choice->vector todrop)))))))))
	      (info%watch "MONGPOOL/COMMIT/edits" sets adds drops unsets)
	      (when (> (+ (table-size sets) (table-size adds) 
			  (table-size drops) (table-size unsets))
		       0)
		(collection/modify! collection `#[_id ,oid]
		  (frame-create #f
		    '$currentDate #[modified #[$type "timestamp"]]
		    '$set (tryif (> (table-size sets) 0) sets)
		    '$addToSet (tryif (> (table-size adds) 0) adds)
		    '$pullAll (tryif (> (table-size drops) 0) drops)
		    '$unset (tryif (> (table-size unsets) 0) unsets)))))
	    (store! originals oid new))))))
  #t)

;;; Opening and initializing mongodb-backed pools

;; This is called by the slambda init-mongopool (below) to create the
;; mongopool entry for a given OID pool stored in MongoDB.
(define (init-mongopool-inner collection (opts #f) (cname))
  (default! cname (collection/name collection))
  (try (get mongopools `#(,(mongodb/getdb collection) ,cname))
       (get mongopools `#(,(mongodb/dbspec collection) 
			  ,(mongodb/dbname collection)
			  ,cname))
       (let* ((info (collection/get collection "_pool"))
	      (metadata (try (collection/get collection "_metadata") #[]))
	      (collname (collection/name collection))
	      (base (get info 'base))
	      (cap (get info 'capacity))
	      (load (try (get info 'load) 0))
	      (name (try (get info 'name)
			 (glom (mongodb/spec collection) 
			   ":" (mongodb/dbname collection)
			   ":" cname))))
	 (info%watch "INIT-MONGOPOOL" collection opts info name)
	 (config! 'mongo:domain collection)
	 (cond ((and (exists? base) (exists? cap)))
	       ((not (getopt opts 'create)) #f)
	       ((not (getopt opts 'base))
		(irritant collection |NoBaseOID|))
	       (else (set! base (getopt opts 'base))
		     (set! cap (getopt opts 'capacity #mib))
		     (set! load (getopt opts 'load 0))
		     (collection/insert! collection
		       `#[_id "_pool" base ,base capacity ,cap
			  name ,name load ,load])
		     (set! metadata 
		       `#[_id "_metadata" base ,base capacity ,cap
			  name ,name])
		     (collection/insert! collection metadata)))
	 (let* ((opts (opts+ `#[type mongopool metadata ,metadata] 
			     opts))
		(record (cons-mongopool collection (mongodb/getdb collection)
					(mongodb/dbspec collection)
					(collection/name collection)
					base cap opts 
					(qc (getopt opts 'slotinfo {}))))
		(pool (make-procpool name base cap opts record load)))
	   (info%watch "INIT-MONGOPOOL/consed" pool record collection)
	   (store! mongopools collection pool)
	   (store! mongopools
	     {(vector (mongodb/getdb collection) cname)
	      (vector (mongodb/dbspec collection) (mongodb/dbname collection)
		      cname)}
	     pool)
	   (store! mongopools pool record)
	   pool))))
(define-init init-mongopool
  (slambda (collection (opts #f))
    (init-mongopool-inner collection opts)))

(define (mongopool/open spec (opts #f) (collection))
  (default! collection
    (if (collection? spec) spec 
	(collection/open spec (getopt opts 'name) opts)))
  (init-mongopool collection opts))
(define (mongopool/make spec (opts #f) (collection))
  (default! collection 
    (if (collection? spec) spec 
	(collection/open spec (getopt opts 'name) opts)))
  (init-mongopool collection (opts+ #[create #t] opts)))
(define (mongodb/pool collection (opts #f))
  (init-mongopool collection opts))

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
			(get (collection/find collection (adjunct-query query oid)
			       `#[returns ,extract])
			     extract)
			(collection/find collection (adjunct-query query oid)))))
	 (coll (get mongopools pool))
	 (name 
	  (if (exists? coll) 
	      (glom (mongodb/dbname coll) "/" (collection/name coll) "/" slot)
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

;;; Mongopool intern

(define (mongopool-intern pool mp unique 
			  (uuid (getuuid))
			  (collection) (existing) (oid))
  (default! collection (mongopool-collection mp))
  (set! existing (collection/find collection unique))
  (try (get (collection/find collection unique) '_id)
       (with-lock (mongopool-lock mp)
	 (let* ((mod (collection/modify collection
			 #[_id "_pool"] 
		       `#[$inc #[load 1]]
		       #[original #t]))
		(before (get mod 'value))
		(start (get before 'load))
		(base (get before 'base))
		(oid (oid-plus base start))
		(found #f))
	   (collection/upsert! 
	       collection unique
	       `#[$setOnInsert #[_id ,oid _internid ,uuid]])
	   (set! found (collection/find collection unique))
	   (if (test found '_id oid)
	       oid
	       (collection/insert! collection #[_id ,oid reuse #t]))))))

(define (mongodb/intern pool keyframe (uuid (getuuid)))
  (mongopool-intern pool (get mongopools pool) keyframe uuid))

;;; Decaching OIDs

(define (mgo/decache! oid (commit #f))
  (when (oid? oid)
    (when (locked? oid) (unlock-oids! oid commit))
    (swapout oid)))

