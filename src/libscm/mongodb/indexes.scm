;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'mongodb/indexes)

(use-module '{mongodb logger})

(module-export! '{make-collection-index
		  mongodb/decache-index! mongo/decache-index!
		  mongodb/index/collection
		  mongodb/index
		  mongodb/index?})

(define-init *mongodb-indexes* {})
(define-init *mongodb-indexmap* (make-hashtable))

(define (collection-index-fetchfn key.value collection)
  (if (vector? key.value)
      (forseq (key.value key.value)
	(if (pair? key.value)
	    (get (collection/find collection `#[,(car key.value) ,(cdr key.value)]
		   `#[return #[_id #t]])
		 '_id)
	    (irritant key.value |MongoDB/NonPairKey|)))
      (if (pair? key.value)
	  (get (collection/find collection `#[,(car key.value) ,(cdr key.value)]
		 `#[return #[_id #t]])
	       '_id)
	  (irritant key.value |MongoDB/NonPairKey|))))

(define (make-collection-index collection (opts #f))
  (cons-extindex 
   (glom "index-" (collection/name collection) "@" (mongodb/dbspec collection))
   collection-index-fetchfn #f collection (getopt opts 'cached #f)
   opts))

(define (mongodb/index collection (opts #f) (reuse))
  (default! reuse (getopt opts 'reuse #t))
  (try (tryif reuse
	 (get *mongodb-indexmap* collection)
	 (get *mongodb-indexmap* 
	      (vector (mongodb/getdb collection) (collection/name collection) opts))
	 (get *mongodb-indexmap* 
	      (vector (mongodb/dbspec collection) (collection/name collection) opts)))
       (if reuse
	   (register-mongo-index collection opts)
	   (make-collection-index collection opts))))

(define (mongodb/index? arg)
  (and (extindex? arg) (collection? (extindex-state arg))))
(define (mongodb/index/collection arg)
  (and (extindex? arg) (collection? (extindex-state arg))
       (extindex-state arg)))

(define (register-mongo-index-inner collection (opts #f))
  (try (get *mongodb-indexmap* collection)
       (get *mongodb-indexmap* 
	    (vector (mongodb/getdb collection) (collection/name collection) opts))
       (get *mongodb-indexmap* 
	    (vector (mongodb/dbspec collection) (collection/name collection) opts))
       (let ((index (make-collection-index collection (opts+ opts 'register #t))))
	 (store! *mongodb-indexmap* collection index)
	 (store! *mongodb-indexmap* 
	    (vector (mongodb/getdb collection) (collection/name collection) opts) index)
	 (store! *mongodb-indexmap* 
	    (vector (mongodb/dbspec collection) (collection/name collection) opts) index)
	 (set+! *mongodb-indexes* index)
	 index)))

(define-init register-mongo-index
  (slambda (collection (opts #f))
    (register-mongo-index-inner collection opts)))

(defambda (mongodb/decache-index! arg1 . args)
  (let* ((index-arg (index? (car args)))
	 (indexes (if index-arg (car args) *mongodb-indexes*))
	 (scan (if index-arg args (cons arg1 args)))
	 (keys {}))
    (while (and (pair? scan) (pair? (cdr scan)))
      (set+! keys (cons (car scan) (cadr scan)))
      (set! scan (cddr scan)))
    (do-choices (index *mongodb-indexes*)
      (extindex-decache! index keys))))
(define mongo/decache-index! mongodb/decache-index!)
