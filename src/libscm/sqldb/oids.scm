;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2020 beingmeta, inc. All rights reserved
Copyright (C) 2020-2021 beingmeta, LLC

(in-module 'sqldb/oids)

(use-module '{sqldb varconfig kno/reflect logger})

(define default-sqlmap #[])
(varconfig! sqldb:sqlmap default-sqlmap)

(define-init %loglevel %notice!)
;;(define  %loglevel %debug!)

(module-export! '{xo/store! xo/add! xo/drop! xo/decache!})
(module-export! '{xo/defstore! xo/defadd! xo/defdrop! xo/defget!})
(module-export! '{xo/defstore xo/defadd xo/defdrop xo/defget xo/defgetstore})

(define-init usecache #t)
(varconfig! sqloids:cache usecache)

(define-init store-procs (make-hashtable))
(define-init add-procs (make-hashtable))
(define-init drop-procs (make-hashtable))
(define-init get-indexes (make-hashtable))
(define-init typefns (make-hashtable))
(store! typefns 'boolean true?)

(define (xo/decache! oid (slotid #f))
  (if slotid
      (extindex-decache! (get get-indexes (cons (getpool oid) slotid))
			 oid)
      (swapout oid)))

(defambda (xo/store! oid slotid value)
  (do-choices oid
    (do-choices slotid
      (let ((method
	     (try (get store-procs (cons (getpool oid) slotid))
		  (get store-procs slotid))))
	(logdebug |XO/STORE!| slotid "(" oid ")=" value ", using " method)
	(prog1
	    (if (exists? method)
		(if (applicable? method)
		    (method (qc value) oid)
		    (irritant method |EXTOID method|
			      "Invalid STORE method for " oid " and " slotid))
		(store-with-edits oid slotid value))
	  (extindex-decache! (get get-indexes (cons (getpool oid) slotid))
			     oid)))))
  (store! (oid-value oid) slotid value))
(defambda (xo/add! oid slotid value)
  (when (exists? value)
    (do-choices oid
      (do-choices slotid
	(let ((method
	       (try (get add-procs (cons (getpool oid) slotid))
		    (get add-procs slotid))))
	  (logdebug |XO/ADD!| slotid "(" oid ")=+" value ", using " method)
	  (if (exists? method)
	      (if (applicable? method)
		  (method value oid)
		  (irritant method |EXTOID method|
			    "Invalid ADD method for " oid " and " slotid))
	      (add-with-store oid slotid value)))
	(extindex-decache! (get get-indexes (cons (getpool oid) slotid))
			   oid))))
  (add! (oid-value oid) slotid value))
(defambda (xo/drop! oid slotid (value))
  (do-choices oid
    (do-choices slotid
      (let ((method
	     (try (get drop-procs (cons (getpool oid) slotid))
		  (get drop-procs slotid))))
	(logdebug |XO/DROP!| slotid "(" oid ")=-" value ", using " method)
	(if (exists? method)
	    (if (applicable? method)
		(method (if (bound? value) (if (fail? value) (qc) value)
			    (get oid slotid))
			oid)
		(irritant method |EXTOID method|
			  "Invalid DROP method for " oid " and " slotid))
	    (if (bound? value)
		(drop-with-store oid slotid value)
		(xo/store! oid slotid {}))))
      (extindex-decache! (get get-indexes (cons (getpool oid) slotid))
			 oid)))
  (if (bound? value)
      (drop! (oid-value oid) slotid value)
      (drop! (oid-value oid) slotid)))

(defambda (store-with-edits oid slotid values)
  (let* ((current (get oid slotid))
	 (adds (difference values current))
	 (drops (difference current values))
	 (add-method
	  (try (get add-procs (cons (getpool oid) slotid))
	       (get add-procs slotid)))
	 (drop-method
	  (try (get drop-procs (cons (getpool oid) slotid))
	       (get drop-procs slotid))))
    (debug%watch "STORE-WITH-EDITS" oid slotid values
		 adds drops add-method drop-method)
    (if (and (or (fail? drops) (exists? drop-method))
	     (or (fail? adds) (exists? add-method)))
	(begin (when (exists? adds) (add-method adds oid))
	  (when (exists? drops) (drop-method drops oid)))
	(error "Can't store value on " slotid " of " oid))))

(defambda (drop-with-store oid slotid values)
  (let ((current (get oid slotid))
	(method
	 (try (get store-procs (cons (getpool oid) slotid))
	      (get store-procs slotid))))
    (debug%watch "DROP-WITH-STORE" oid slotid values method)
    (if (exists? method)
	(when (exists? (intersection current values))
	  (method (qc (difference current values)) oid))
	(error "Can't drop values from " slotid " of " oid))))

(defambda (add-with-store oid slotid values)
  (let ((current (get oid slotid))
	(method
	 (try (get store-procs (cons (getpool oid) slotid))
	      (get store-procs slotid))))
    (debug%watch "ADD-WITH-STORE" oid slotid values method)
    (if (exists? method)
	(when (exists? (difference values current))
	  (method (qc (choice values current)) oid))
	(error "Can't add values to " slotid " of " oid))))

;;; Defining methods

(define (xo/defstore pool slotid db query (valtype))
  (default! valtype (try (get default-sqlmap slotid) #f))
  (store! store-procs
	  (if pool (cons pool slotid) slotid)
	  (sqldb/proc db query
		      (qc default-sqlmap)
		      (try (get typefns valtype) valtype)
		      (pool-base pool))))
(defambda (xo/defstore! slotid method (pool #f))
  (do-choices slotid
    (do-choices pool
      (store! store-procs (if pool (cons pool slotid) slotid)
	      method))))
(define (xo/defadd pool slotid db query (valtype #f))
  (default! valtype (try (get default-sqlmap slotid) #f))
  (store! add-procs
	  (if pool (cons pool slotid) slotid)
	  (sqldb/proc db query
		      (qc default-sqlmap)
		      (try (get typefns valtype) valtype)
		      (pool-base pool))))
(defambda (xo/defadd! slotid method (pool #f))
  (do-choices slotid
    (do-choices pool
      (store! add-procs (if pool (cons pool slotid) slotid) method))))
(define (xo/defdrop pool slotid db query (valtype #f))
  (default! valtype (try (get default-sqlmap slotid) #f))
  (store! drop-procs
	  (if pool (cons pool slotid) slotid)
	  (sqldb/proc db query
		      (qc default-sqlmap)
		      (try (get typefns valtype) valtype)
		      (pool-base pool))))
(defambda (xo/defdrop! slotid method (pool #f))
  (do-choices slotid
    (do-choices pool
      (store! drop-procs (if pool (cons pool slotid) slotid) method))))

;;; Getting slots

(define (simple-get key sqlget)
  (if (vector? key) (sqlget (elts key)) (sqlget key)))
(define (get-getter normalize)
  (cond ((not normalize) simple-get)
	((applicable? normalize)
	 (lambda (key sqlget)
	   (normalize (if (vector? key) (sqlget (elts key)) (sqlget key)))))
	((slotid? normalize)
	 (lambda (key sqlget)
	   (get (if (vector? key) (sqlget (elts key)) (sqlget key)) normalize)))
	((table? normalize)
	 (lambda (key sqlget)
	   (get normalize (if (vector? key) (sqlget (elts key)) (sqlget key)))))
	(else simple-get)))

(defambda (xo/defget pool slotid db query
		     (sqlmap default-sqlmap)
		     (normalize #f)
		     (cache usecache))
  (let ((index (cons-extindex (stringout slotid) (get-getter normalize) #f
			      (sqldb/proc db query
					  (if (getopt sqlmap '%merge) sqlmap
					      (cons #[%merge #t] sqlmap))
					  (pool-base pool))
			      cache)))
    (store! get-indexes (cons pool slotid) index)
    (use-adjunct index slotid pool)))

#|
;;; This is a good debugging case for circular GC references in SPROCs 
(defambda (xo/defget pool slotid db query
		     (sqlmap default-sqlmap)
		     (normalize #f)
		     (cache usecache))
  (let* ((getter/sql (sqldb/proc db query (cons #[%merge #t] sqlmap)
				 (pool-base pool)))
	 (rawgetter (lambda (key)
		      (if (vector? key)
			  (getter/sql (elts key))
			  (getter/sql key))))
	 (getter (cond ((not normalize) rawgetter)
		       ((applicable? normalize)
			(lambda (x) (normalize (rawgetter x))))
		       ((slotid? normalize)
			(lambda (x) (get  (rawgetter x) normalize)))
		       ((table? normalize)
			(lambda (x) (get normalize (rawgetter x))))
		       (else rawgetter)))
	 (index (cons-extindex (stringout slotid) getter #f #f cache)))
    (store! get-indexes (cons pool slotid) index)
    (use-adjunct index slotid pool)))
|#

(defambda (xo/defgetstore pool slotid index)
  (store! get-indexes (cons pool slotid) index))

(defambda (xo/defget! slotid methods (pool #f))
  (let ((index (cons-extindex (stringout slotid) methods #f)))
    (do-choices slotid
      (do-choices pool
	(store! get-indexes (cons pool slotid) index)
	(use-adjunct index slotid pool)))))

