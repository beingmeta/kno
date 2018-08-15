;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'mongodb/slots)

(use-module '{varconfig logger})
(use-module '{mongodb})

(define %loglevel %notice%)
(define %volatile 'domains)
(define %nosbust 'domains)

(module-export! '{->collection})
(module-export! '{mgo/get mgo/store! mgo/drop! mgo/add!})
(module-export! '{mgo/decache!})

(defimport mongopools 'mongodb/pools)
(defimport mp->collection 'mongodb/pools mongopool-collection)

;;; Mapping TYPES to collections

(define-init collection-typemap #[])

(define (config-typemap var (val))
  (cond ((not (bound? val)) collection-typemap)
	((and (pair? val) (collection? (car val))
	      (or (symbol? (cdr val)) 
		  (string? (cdr val))
		  (oid? (cdr val))))
	 (add! collection-typemap (cdr val) (car val)))
	(else (irritant val |Bad MongoDB Typemap|))))
(config-def! 'mongo:typemap config-typemap)

(define-init domains {})
(define-init domains-hashset (make-hashset))
(define (config-domains var (val))
  (cond ((not (bound? val)) domains)
	((and (collection? val)
	      (hashset-get domains-hashset
			   (cons (mongo/getdb val) (collection/name val))))
	 #f)
	((collection? val)
	 (set+! domains val)
	 (hashset-add! domains-hashset
		       (cons (mongo/getdb val) (collection/name val)))
	 #t)
	(else (irritant val |Not a MongoDB Collection|))))
(config-def! 'mongo:domains config-domains)
(config-def! 'mongo:domain config-domains)

(define (->collection obj (err #f))
  (if (oid? obj)
      (try (mp->collection (get mongopools (getpool obj)))
	   (tryif err
	     (if (or (fail? (getpool obj)) (not (getpool obj)))
		 (irritant oid |No pool| mgo/store!)
		 (irritant pool |Not A MongoDB pool| mgo/store!))))
      (try (tryif (table? obj)
	     (get collection-typemap (get obj 'type))
	     (get collection-typemap (get obj 'types))
	     (tryif (test obj '_id)
	       (filter-choices (collection domains)
		 (exists? (collection/get collection (get obj '_id) #[return #[_id 1]])))))
	   (tryif (uuid? obj)
	     (filter-choices (collection domains)
	       (exists? (collection/get collection obj #[return #[_id 1]])))))))

;;; Basic operations for OIDs in mongodb pools

(defambda (mgo/get obj slotid)
  (let* ((collection (->collection obj))
	 (id {(reject obj table?) (get (pick obj table?) '_id)})
	 (selector `#[_id ,(if (ambiguous? id) `#[$in ,id] id)]))
    (get (collection/find collection selector `#[return #[,slotid 1]])
	 slotid)))

(defambda (mgo/store! obj slotid values)
  (let* ((collection (->collection obj))
	 (id {(reject obj table?) (get (pick obj table?) '_id)})
	 (selector `#[_id ,(if (ambiguous? id) `#[$in ,id] id)])
	 (result 
	  (collection/modify! collection selector
			      `#[$set ,(if (ambiguous? slotid)
					   (get-store-modifier slotid values)
					   `#[,slotid ,values])]
			      #[new #t return #[__index 0]])))
    (if (oid? obj)
	(update/oid! result)
	(when (table? obj)
	  (store! obj slotid values)))))

(defambda (get-store-modifier slotids values (result))
  (set! result #[])
  (do-choices (slotid slotids)
    (store! result slotid values))
  result)

(defambda (mgo/add! obj slotid values)
  (let* ((collection (->collection obj))
	 (id {(reject obj table?) (get (pick obj table?) '_id)})
	 (selector `#[_id ,(if (ambiguous? id) `#[$in ,id] id)])
	 (result 
	  (collection/modify! collection selector
			      `#[$addToSet ,(get-add-modifier slotid values)]
			      #[new #t return #[__index 0]])))
    (if (oid? obj)
	(update/oid! result)
	(when (table? obj)
	  (add! obj slotid values)))))

(defambda (get-add-modifier slotids values)
  (if (unique? slotids)
      `#[,slotids ,(if (unique? values) values `#[$each ,values])]
      (let ((q (frame-create #f)))
	(do-choices (slotid slotids)
	  (add! q slotid 
		(if (unique? values) values `#[$each ,values]))
	  q))))

(defambda (mgo/drop! obj slotid (values))
  (let* ((collection (->collection obj))
	 (id {(reject obj table?) (get (pick obj table?) '_id)})
	 (selector `#[_id ,(if (ambiguous? id) `#[$in ,id] id)])
	 (result 
	  (if (bound? values)
	      (collection/modify! collection selector
				  (if (unique? values)
				      `#[$pull ,(if (unique? slotid)
						    `#[,slotid ,values]
						    (get-store-modifier slotid values))]
				      `#[$pullAll ,(if (unique? slotid)
						       `#[,slotid ,values]
						       (get-store-modifier slotid values)
						       )])
				  #[new #t return #[__index 0]])
	      (collection/modify! collection 
				  `#[_id ,obj] (if (ambiguous? slotid)
						   (get-drop-all-modifier slotid)
						   `#[$unset #[,slotid 1]])
				  #[new #t return #[__index 0]]))))
    (if (oid? obj)
	(update/oid! result)
	(when (table? obj)
	  (if (bound? values)
	      (drop! obj slotid values)
	      (drop! obj slotid))))))

(define (get-drop-all-modifier slotids (result #[]))
  (do-choices (slotid slotids)
    (store! result slotid 1))
  result)

(define (mgo/decache! oid (slotid #f))
  (swapout oid))

(define (update/oid! result (value #f))
  (when (test result 'value)
    (set! value (get result 'value))
    (%set-oid-value! (get value '_id) value))
  (or value result))






