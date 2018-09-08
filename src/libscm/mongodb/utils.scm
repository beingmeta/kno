;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'mongodb/utils)

(use-module '{mongodb logger varconfig engine})

(module-export! '{mgo/index/list mgo/index/drop! mgo/index/add!
		  collection/new 
		  collection/rename! collection/copy! collection/drop!
		  mgo/dropdb! 
		  mgo/params/list mgo/params/get mgo/params/set!
		  mongo/copy-pool})


(defambda (getbatch v)
  (if (vector? v) (elts v) v))

(define (extract-results result (collection #f) (batch 100))
  (if (not collection)
      (getbatch (get (get result 'cursor) '|firstBatch|))
      (let* ((cursor (get result 'cursor))
	     (results (getbatch (get cursor '|firstBatch|)))
	     (cursor-id (get cursor 'id)))
	(while (not (zero? cursor-id))
	  (set! result 
		(mongo/results collection 
				 "getMore" cursor-id
				 "collection" (collection/name collection)
				 "batchSize" batch))
	  (set! cursor (get result 'cursor))
	  (set+! results (getbatch (get cursor '|nextBatch|)))
	  (set! cursor-id (get cursor 'id)))
	results)))

(define (mgo/index/list collection)
  (extract-results (mongo/results 
		    collection "listIndexes" (collection/name collection))
		   collection))

(define (mgo/index/drop! collection name)
  (if (string? name)
      (mongo/cmd collection
		  "dropIndexes" (collection/name collection)
		  "index" name)
      (mongo/cmd collection
		  "dropIndexes" (collection/name collection)
		  "index" (get name 'name))))

(defambda (mgo/index/add! collection specs (opts #f))
  (mongo/cmd collection
	      "createIndexes" (collection/name collection)
	      "indexes" (qc (generate-index-specs specs))))

(defambda (generate-index-specs specs)
  (let ((specs (for-choices (spec specs)
		 (cond ((symbol? spec)
			`#["key" #[,(downcase spec) 1]
			   "name" ,(glom (downcase spec) "_" "1")])
		       ((string? spec)
			#["key" #[,spec 1]
			  "name" ,(glom spec "_" "1")])
		       ((vector? spec)
			(let ((keyspec '()))
			  (doseq (spec spec)
			    (set! keyspec 
				  (cons* 1 (if (symbol? spec) (downcase spec) spec)
					 keyspec)))
			  #["key" ,(apply frame-create #f (reverse keyspec))
			    "name" ,(stringout (doseq (elt (reverse keyspec) i)
						 (printout (if (> i 0) "_") elt)))]))
		       ((table? spec) (generate-index-spec spec))
		       (else (irritant spec "Bad index spec"))))))
    (if (singleton? specs) (mongovec specs)
	specs)))

(define (generate-index-spec spec)
  (let ((key '())
	(name (get spec '$name))
	(fields (get spec '$fields))
	(unique (try (get spec '$unique) #f))
	(sparse (try (get spec '$sparse) #f))
	(background (get spec '$background))
	(ttl (get spec '$ttl)))
    (do-choices (field (difference (getkeys spec)
				   '{$name $unique $sparse $fields $ttl
				     $background}))
      (set! key (cons* (qc (get spec field)) field key)))
    (when (exists? fields)
      (do-choices fields
	(cond ((symbol? fields) 
	       (set! key (cons* 1 fields key)))
	      ((or (vector? fields) (pair? fields))
	       (doseq (field fields)
		 (set! key (cons* 1 (if (symbol? field) (downcase field) field)
				  key))))
	      (else
	       (irritant fields "Bad $fields arg")))))
    (when (fail? name)
      (set! name
	    (stringout (doseq (elt (reverse key) i)
			 (printout (if (> i 0) "_") 
			   (if (symbol? elt) (downcase elt) elt))))))
    `#["key" ,(apply frame-create #f (reverse key))
       "name" ,name
       "unique" ,unique
       "sparse" ,sparse
       "background" ,background
       "expireAfterSeconds" ,ttl]))

(define (collection/new db name (opts #f))
  (mongo/cmd db `#["create" ,name 
		    "capped" ,(getopt opts 'capped {})
		    "indexids" ,(getopt opts 'indexid {})
		    "maxsize" ,(getopt opts 'maxsize {})
		    "maxdocs" ,(getopt opts 'maxdocs {})]))

(define (collection/rename! collection newname (db))
  (default! db (mongo/getdb collection))
  (mongo/cmd db
      `#["renameCollection" 
	 ,(glom (mongo/dbname collection) "/" (collection/name collection))
	 "target" ,newname]))

(define (collection/drop! collection (db))
  (default! db (mongo/getdb collection))
  (mongo/cmd db `#["drop" ,(collection/name collection)]))

(define (collection/copy! source dest (opts #f) (batchsize))
  (default! batchsize (getopt opts 'batchsize 200))
  (let* ((cursor (mongo/cursor source #[]))
	 (batch (cursor/read cursor batchsize)))
    (while (exists? batch)
      (if (getopt opts 'upsert)
	  (do-choices (save batch)
	    (collection/upsert! dest `#[_id ,(get save '_id)]
				`#[$set ,save]))
	  (collection/insert! dest batch))
      (if (%wc cursor/done? cursor) 
	  (set! batch {})
	  (set! batch (cursor/read cursor batchsize))))))

(define (mgo/dropdb! db dbname)
  (mongo/cmd db #["dropDatabase" 1]))

(define (mgo/params/list arg (db))
  (set! db (if (mongodb? arg) arg (mongo/getdb arg)))
  (extract-results (mongo/results db "getParameter" "*")))
(define (mgo/params/get arg param (db))
  (set! db (mongo/getdb arg))
  (mongo/results db "getParameter" 1 param 1))
(define (mgo/params/set! arg param value (db))
  (set! db (mongo/getdb arg))
  (mongo/results db "setParameter" 1 param value))

;;;; Copying pools into MongoDB

(define (mongo/copy-pool input collection (slotinfo {}))
  (let ((base (pool-base input))
	(capacity (pool-capacity input))
	(load (pool-load input))
	(metadata (poolctl input 'metadata))
	(curinfo (collection/get collection "_pool"))
	(curmd (collection/get collection "_metadata")))
    (unless (exists? curinfo)
      (collection/insert! collection
	`#[_id "_pool" base ,base capacity ,capacity load ,load]))
    (store! metadata '_id "_metadata")
    (unless (exists? curmd)
      (collection/modify! collection #[_id "_metadata"] `#[$set ,metadata]))
    (engine/run
	(lambda (oid)
	  (let ((v (frame-create #f '_id oid)))
	    (do-choices (slotid (getkeys oid))
	      (store! v slotid (get oid slotid)))
	    (collection/modify! collection
		`#[_id ,oid] `#[$set ,v]
		`#[upsert #t new #t])))
	(pool-elts input))))
