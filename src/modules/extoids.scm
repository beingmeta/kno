(in-module 'extoids)

(define version "$Id$")
(define revision "$Revision$")

(use-module '{extdb varconfig reflection logger})

(define default-sqlmap #[])
(varconfig! extdb:sqlmap default-sqlmap)

(define-init %loglevel %notice!)
;;(define  %loglevel %debug!)

(module-export! '{xo/store! xo/add! xo/drop! xo/decache!})
(module-export! '{xo/defstore! xo/defadd! xo/defdrop! xo/defget!})
(module-export! '{xo/defstore xo/defadd xo/defdrop xo/defget xo/defgetstore})

(define-init usecache #t)
(varconfig! extoids:cache usecache)

(define-init store-procs (make-hashtable))
(define-init add-procs (make-hashtable))
(define-init drop-procs (make-hashtable))
(define-init get-indices (make-hashtable))

(define (xo/decache! oid (slotid #f))
  (if slotid
      (extindex-decache! (get get-indices (cons (getpool oid) slotid))
			 oid)
      (swapout oid)))

(defambda (xo/store! oid slotid value)
  (do-choices oid
    (do-choices slotid
      (let ((method
	     (try (get store-procs (cons (getpool oid) slotid))
		  (get store-procs slotid))))
	(prog1
	    (if (exists? method)
		(method (qc value) oid)
		(store-with-edits oid slotid value))
	  (extindex-decache! (get get-indices (cons (getpool oid) slotid))
			     oid)))))
  (store! (oid-value oid) slotid value))
(defambda (xo/add! oid slotid value)
  (do-choices oid
    (do-choices slotid
      (let ((method
	     (try (get add-procs (cons (getpool oid) slotid))
		  (get add-procs slotid))))
	(debug%watch "XO/ADD!" oid slotid method)
	(if (exists? method)
	    (method (if (fail? value) (qc) value) oid)
	    (add-with-store oid slotid value)))
      (extindex-decache! (get get-indices (cons (getpool oid) slotid))
			 oid)))
  (add! (oid-value oid) slotid value))
(defambda (xo/drop! oid slotid (value))
  (do-choices oid
    (do-choices slotid
      (let ((method
	     (try (get drop-procs (cons (getpool oid) slotid))
		  (get drop-procs slotid))))
	(debug%watch "XO/DROP!" oid slotid method)
	(if (exists? method)
	    (method (if (bound? value) (if (fail? value) (qc) value)
			(get oid slotid))
		    oid)
	    (if (bound? value)
		(drop-with-store oid slotid value)
		(xo/store! oid slotid {}))))
      (extindex-decache! (get get-indices (cons (getpool oid) slotid))
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

(define (xo/defstore pool slotid db query (valtype #f))
  (store! store-procs
	  (if pool (cons pool slotid) slotid)
	  (extdb/proc db query
		      (qc default-sqlmap)
		      valtype (pool-base pool))))
(define (xo/defstore! slotid method (pool #f))
  (store! store-procs (if pool (cons pool slotid) slotid) method))
(define (xo/defadd pool slotid db query (valtype #f))
  (store! add-procs
	  (if pool (cons pool slotid) slotid)
	  (extdb/proc db query
		      (qc default-sqlmap)
		      valtype (pool-base pool))))
(define (xo/defadd! slotid method (pool #f))
  (store! add-procs (if pool (cons pool slotid) slotid) method))
(define (xo/defdrop pool slotid db query (valtype #f))
  (store! drop-procs
	  (if pool (cons pool slotid) slotid)
	  (extdb/proc db query
		      (qc default-sqlmap)
		      valtype (pool-base pool))))
(define (xo/defdrop! slotid method (pool #f))
  (store! drop-procs (if pool (cons pool slotid) slotid) method))

;;; Getting slots

(defambda (xo/defget pool slotid db query
		     (sqlmap default-sqlmap)
		     (normalize #f)
		     (cache usecache))
  (let* ((getter/sql (extdb/proc db query (cons #[%merge #t] sqlmap)
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
	 (index (make-extindex (stringout slotid) getter #f #f cache)))
    (store! get-indices (cons pool slotid) index)
    (use-adjunct index slotid pool)))

(defambda (xo/defgetstore pool slotid index)
  (store! get-indices (cons pool slotid) index))



