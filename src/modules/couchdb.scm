;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2010 beingmeta, inc. All rights reserved

(in-module 'couchdb)

(define version "$Id: glosses.scm 5530 2010-09-13 16:53:19Z haase $")
(define revision "$Revision: 5530 $")

(use-module '{fdweb ezrecords extoids})
(use-module '{texttools logger})

(module-export! '{checkgloss newgloss getgloss glossdb/moveid})

(define-init %loglevel %notice!)
;;(define %loglevel %debug!)

(defrecord couchdb url (map #[]) (views (make-hashtable)))
(defrecord couchview db design view path)

(define-init couchdbs (make-hashtable))

(define (couchdb url)
  (try (get couchdbs url) (new-couchdb url)))
(module-export! '{couchdb couchdb? couchdb-url couchdb-map})

(defslambda (new-couchdb url)
  (try (get couchdbs url)
       (let ((new (cons-couchdb url #[])))
	 (store! couchdbs url new)
	 new)))

(define (couchdb/add-map! db . args)
  (let* ((db (if (string? db) (couchdb db) db))
	 (map (couchdb-map db)))
    (when (odd? (length args))
      (do-choices (key (getkeys (car args)))
	(store! map key (get (car args) key)))
      (set! args (cdr args)))
    (do ((scan args (cddr scan)))
	((null? scan))
      (store! map (car scan) (cadr scan)))))
(module-export! 'couchdb/add-map!)

(define (couchdb/req db path (options #f) . args)
  (let* ((path (mkpath (couchdb-url db) path))
	 (call (if (null? args) path (apply scripturl path args)))
	 (resp (urlget call  options)))
    (tryif (and (test resp 'response) (>= (get resp 'response) 200)
		(< (get resp 'response) 300))
      (jsonparse (get resp '%content) 40 (couchdb-map db)))))

(define (couchdb/get db id (options #f))
  (couchdb/req db (if (oid? id)
		      (stringout "@" (number->string (oid-addr id) 16))
		      (if (uuid? id) (uuid->string id)
			  (if (string? id) id
			      (stringout ":" id))))
	       options))

(define (couchdb/save! db value (id))
  (default! id (get value '_id))
  (let*  ((idstring (if (oid? id)
			(stringout "@" (number->string (oid-addr id) 16))
			(if (uuid? id) (uuid->string id)
			    (if (string? id) id
				(stringout ":" id)))))
	  (r (urlput (mkpath (couchdb-url db) idstring) (->json value)))
	  (httpcode (get r 'response)))
    (or (and (exists? httpcode) (>= httpcode 200) (< httpcode 300))
	(begin (notice%watch "COUCHDB/SAVE! failed" r db value (->json value))
	       #f))))

(define (couchdb/mutate! db id mutate)
  (let ((success #f) (cur (couchdb/get db id)))
    (until success
      (mutate cur)
      (set! success (couchdb/save! db cur))
      (unless success (set! cur (couchdb/get db id))))))
  
(define (couchdb/delete! db id (rev #f))
  (let ((rev (or rev (get (couchdb/get db id) '_rev))))
    (couchdb/req db (scripturl (if (oid? id)
				   (stringout "@" (number->string (oid-addr id) 16))
				   (if (string? id) id
				       (stringout ":" id))))
		 #[METHOD DELETE]
		 "rev" rev)))

(define (couchdb/store! db id slotid value)
  (let ((success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (identical? (get cur slotid) value)
	  (set! success #t)
	  (begin (store! cur slotid value)
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))))))
(define (couchdb/push! db id slotid value)
  (let ((success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (and (test cur slotid) (vector? (get cur slotid))
	       (position value (get cur slotid)))
	  (set! success #t)
	  (begin (store! cur slotid
			 (if (test cur slotid)
			     (if (vector? (get cur slotid))
				 (append (vector value) (get cur slotid))
				 (vector value (get cur slotid)))
			     (vector value)))
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))))))
(define (couchdb/add! db id slotid value)
  (let ((success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (test cur slotid value)
	  (set! success #t)
	  (begin (add! cur slotid value)
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))))))
(define (couchdb/drop! db id slotid value)
  (let ((success #f)
	(id (if (slotmap? id) (get id '_id) id))
	(cur (if (slotmap? id) id (couchdb/get db id))))
    (until success
      (if (test cur slotid value)
	  (begin (drop! cur slotid value)
		 (set! success (couchdb/save! db cur))
		 (unless success (set! cur (couchdb/get db id))))
	  (set! success #t)))))

(module-export!
 '{couchdb/req
   couchdb/get couchdb/save! couchdb/mutate! couchdb/delete!
   couchdb/store! couchdb/add! couchdb/drop! couchdb/push!})

;;; Getting views

(define (couchdb/view db design view)
  (if (string? db)
      (couchdb/view (couchdb db) design view)
      (try (get (couchdb-views db) (cons design view))
	   (new-view db design view))))
    
(defslambda (new-view db design view)
  (try (get (couchdb-views db) (cons design view))
       (let ((new (cons-couchview
		   db design view
		   (stringout "_design/" design "/_view/" view))))
	 (store! (couchdb-views db) (cons design view) new)
	 new)))
(module-export! 'couchdb/view)

(define (couchdb/table view (opts #f) (table #f))
  (let ((response (if opts
		      (couchdb/req
		       (couchview-db view)
		       (scripturl (couchview-path view)
			 "startkey" (get opts 'start)
			 "endkey" (get opts 'end)))
		      (couchdb/req (couchview-db view) (couchview-path view))))
	(table (or table
		   (and opts (getopt opts 'output))
		   (make-hashtable))))
    (doseq (row (get response 'rows))
      (add! table (get row 'key) (get row 'value)))
    table))
(define (couchdb/list view (opts #f))
  (let ((response (if opts
		      (couchdb/req
		       (couchview-db view)
		       (scripturl (couchview-path view)
			 "startkey" (tryif (test opts 'start)
				      (->json (get opts 'start)))
			 "endkey" (tryif (test opts 'end)
				    (->json (get opts 'end)))))
		      (couchdb/req (couchview-db view) (couchview-path view))))
	(results {}))
    (doseq (row (get response 'rows)) (set+! results (get row 'value)))
    results))
(module-export! '{couchdb/table couchdb/list})
