;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

;;; This is a simple ORM for operating on objects stored in mongodb,
;;; including OIDs stored in mongodb pools. The MGO/{ADD|DROP|STORE}
;;; calls connect directly to MongoDB and then update the local object
;;; as needed as well.

(in-module 'mongodb/orm)

(use-module '{varconfig logger})
(use-module '{mongodb mongodb/indexes})

(define %loglevel %notice%)
(define %volatile 'domains)
(define %nosbust 'domains)

(module-export! '{->collection mongodb/domain!})
(module-export! '{mgo/get mgo/store! mgo/drop! mgo/add! mgo/modify!})

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

;; A domain is basically a collection being used for a particular type of object
(define (mongodb/domain? val)
  (and (collection? val)
       (or (hashset-get domains-hashset 
			(cons (mongodb/getdb val) (collection/name val)))
	   (hashset-get domains-hashset 
			(cons (mongodb/dbspec val) (collection/name val))))))

(define (mongodb/domain! val)
  (if (not (collection? val)) (irritant val |NotMongoDBCollection|))
  (if (or (hashset-get domains-hashset 
		       (cons (mongodb/getdb val) (collection/name val)))
	  (hashset-get domains-hashset 
		       (cons (mongodb/dbspec val) (collection/name val))))
      #f
      (begin
	(hashset-add! domains-hashset
		      (cons (mongodb/getdb val) (collection/name val)))
	(hashset-add! domains-hashset
		      (cons (mongodb/dbspec val) (collection/name val)))
	(set+! domains val)
	#t)))

(define (config-domains var (val))
  (cond ((not (bound? val)) domains)
	((and (collection? val)
	      (or
	       (hashset-get domains-hashset
			    (cons (mongodb/getdb val) (collection/name val)))
	       (hashset-get domains-hashset
			    (cons (mongodb/dbspec val) (collection/name val)))))
	 #f)
	((collection? val) (mongodb/domain! val))
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

(define update-modified #[modified #[$type "timestamp"]])

(defambda (mgo/get obj slotid)
  (let* ((collection (->collection obj))
	 (id {(reject obj table?) (get (pick obj table?) '_id)})
	 (selector `#[_id ,(if (ambiguous? id) `#[$in ,id] id)]))
    (get (collection/find collection selector `#[return #[,slotid 1]])
	 slotid)))

(defambda (mgo/store! obj slotid values (opts #f))
  (if (and (or (ambiguous? obj) (ambiguous? slotid))
	   (not (getopt opts 'batch #f)))
      (for-choices obj
	(for-choices slotid
	  (mgo/store! obj slotid values opts)))
      (let* ((collection (->collection obj))
	     (id (cond ((oid? obj) obj)
		       ((not (table? obj)) obj)
		       (else (try (get obj '_id) obj))))
	     (selector `#[_id ,(if (ambiguous? id) `#[$in ,id] id)])
	     (current (if (table? obj) (get obj slotid) (mgo/get obj slotid)))
	     (vecvals (getopt opts 'vecvals #f))
	     (result #f))
	(info%watch "MGO/STORE!" obj id collection slotid values)
	(set! result
	  (collection/modify! collection selector
	    (if (ambiguous? slotid)
		`#[$set ,(get-store-modifier slotid values (and vecvals (singleton? values)))]
		(if (and (singleton? values) vecvals)
		    `#[$set #[,slotid ,(mongovec values)]
		       $currentDate ,update-modified]
		    `#[$set #[,slotid ,values]
		       $currentDate ,update-modified]))
	    #[new #t return #[__index 0]]))
	(mongodb/decache-index! slotid 
			      {(difference current values)
			       (difference values current)})
	(debug%watch "MGO/STORE!" 
	  obj id slotid collection values "\n" result)
	(cond ((and (oid? obj) (modified? obj))
	       ;; Just write the new value
	       (store! obj slotid values))
	      ((oid? obj)
	       ;; This updates the current OID value from the
	       ;;  value we got from the database from
	       ;; mongodb/modify!
	       (oid/sync! obj slotid result))
	      ((table? obj) (store! obj slotid values))))))

(defambda (get-store-modifier slotids values vecvals (result))
  (set! result #[])
  (do-choices (slotid slotids)
    (store! result slotid (if vecvals (mongovec values) values)))
  result)

(defambda (mgo/add! obj slotid values (opts #f))
  (cond ((fail? values) #f)
	((and (or (ambiguous? obj) (ambiguous? slotid))
	      (not (getopt opts 'batch #f)))
	 (for-choices obj
	   (for-choices slotid
	     (mgo/add! obj slotid values opts))))
	(else
	 (let* ((collection (->collection obj))
		(id (cond ((oid? obj) obj)
			  ((not (table? obj)) obj)
			  (else (try (get obj '_id) obj))))
		(selector `#[_id ,(if (ambiguous? id) `#[$in ,id] id)])
		(current (tryif (singleton? obj) (get obj slotid)))
		(result #f))
	   (info%watch "MGO/ADD!" obj id collection slotid values)
	   (set! result
	     (cond ((empty? current)
		    (collection/modify! collection selector
		      (if (and (singleton? values) (getopt opts 'vecvals #f))
			  `#[$set #[,slotid ,(mongovec values)]
			     $currentDate ,update-modified]
			  `#[$set #[,slotid ,values]
			     $currentDate ,update-modified])
		      #[new #t return #[__index 0]]))
		   ((singleton? current)
		    (collection/modify! collection selector
		      `#[$set #[,slotid ,{values current}]]
		      #[new #t return #[__index 0]]))
		   (else
		    (collection/modify! collection selector
		      `#[$addToSet ,(get-multi-modifier slotid values)
			 $currentDate ,update-modified]
		      #[new #t return #[__index 0]]))))
	   (debug%watch "MGO/ADD!" obj id slotid collection values "\n" result)
	   (mongodb/decache-index! slotid values)
	   (cond ((oid? obj)
		  (add! obj slotid values))
		 ((and (oid? obj) (modified? obj))
		  ;; Just write the new value
		  (add! obj slotid values))
		 ((oid? obj)
		  ;; This updates the current OID value from the
		  ;;  value we got from the database from
		  ;; mongodb/modify!
		  (oid/sync! obj slotid result))
		 ((table? obj) (add! obj slotid values)))))))

(defambda (get-multi-modifier slotids values)
  (if (unique? slotids)
      `#[,slotids ,(if (unique? values) values `#[$each ,values])]
      (let ((q (frame-create #f)))
	(do-choices (slotid slotids)
	  (add! q slotid 
		(if (unique? values) values `#[$each ,values]))
	  q))))

(defambda (mgo/drop! obj slotid (values) (opts #f))
  (if (and (or (ambiguous? obj) (ambiguous? slotid))
	   (not (getopt opts 'batch #f)))
      (do-choices obj
	(do-choices slotid
	  (if (bound? values)
	      (mgo/drop! obj slotid values opts)
	      (mgo/drop! obj slotid #default opts))))
      (let* ((collection (->collection obj))
	     (id (cond ((oid? obj) obj)
		       ((not (table? obj)) obj)
		       (else (try (get obj '_id) obj))))
	     (selector `#[_id ,(if (ambiguous? id) `#[$in ,id] id)])
	     (result #f))
	(info%watch "MGO/DROP!" obj id collection slotid values)
	(do-choices slotid
	  (let ((current (if (table? obj) (get obj slotid) (mgo/get obj slotid))))
	    (debug%watch "MGO/DROP!" obj id collection slotid current values)
	    (set! result
	      (if (or (not (bound? values)) (default? values)
		      (singleton? current))
		  (collection/modify! collection 
		      `#[_id ,id] 
		    `#[$unset #[,slotid 1]
		       $currentDate ,update-modified]
		    #[new #t return #[__index 0]])
		  (collection/modify!
		      collection selector
		    (if (singleton? values)
			`#[$pull #[,slotid ,values]
			   $currentDate ,update-modified]
			`#[$pullAll #[,slotid ,values]
			   $currentDate ,update-modified])
		    #[new #t return #[__index 0]])))
	    (mongodb/decache-index! 
	     slotid (if (or (unbound? values) (eq? values #default)) 
			current
			{(difference current values)
			 (difference values current)})))
	  (cond ((and (oid? obj) (modified? obj))
		 ;; Just drop the specified values (or all of them)
		 (if (or (unbound? values) (default? values))
		     (drop! obj slotid)
		     (drop! obj slotid values)))
		((oid? obj)
		 ;; This updates the current OID value from the
		 ;;  value we got from the database in our call
		 ;;  to mongodb/modify!
		 (oid/sync! obj slotid result))
		((table? obj)
		 (if (unbound? values)
		     (drop! obj slotid)
		     (drop! obj slotid values))))))))

(define (get-drop-all-modifier slotids (result #[]))
  (do-choices (slotid slotids)
    (store! result slotid 1))
  result)

(define (oid/sync! oid slotid result (value #f))
  (debug%watch "OID/SYNC!" oid slotid result value)
  (when (and (test result 'value) (exists? (get result 'value)))
    (set! value (get result 'value))
    (detail%watch "OID/SYNC!" oid slotid "\nVALUE" value "\nRESULT" result)
    (%set-oid-value! oid value)))

;;;; Modify

(define (mgo/modify! obj modifier)
  (let* ((collection (->collection obj))
	 (id (cond ((oid? obj) obj)
		   ((not (table? obj)) obj)
		   (else (try (get obj '_id) obj))))
	 (selector `#[_id ,(if (ambiguous? id) `#[$in ,id] id)])
	 (modifier (if (test modifier '$currentDate)
		       modifier
		       (frame-create modifier 
			 '$currentDate #[modified #[$type "timestamp"]])))
	 (result (collection/modify! collection selector modifier)))
    (cond ((and (oid? obj) (test result 'value))
	   (%set-oid-value! obj (get result 'value)))
	  ((and (table? obj) (test obj '_id))
	   (let ((new (get result 'value)))
	     (drop! obj (difference (getkeys obj) '_id))
	     (do-choices (slotid (getkeys new))
	       (store! obj slotid (get new slotid))))))))

