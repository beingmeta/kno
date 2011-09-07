(in-module 'extoids)

(define version "$Id$")
(define revision "$Revision$")

(use-module '{extdb varconfig reflection})

(define default-sqlmap #[])
(varconfig! extdb:sqlmap default-sqlmap)

(module-export! '{xo/store! xo/add! xo/drop!})
(module-export! '{xo/defstore! xo/defadd! xo/defdrop! xo/defget!})
(module-export! '{xo/defstore xo/defadd xo/defdrop xo/defget})

(define-init usecache #t)
(varconfig! extoids:cache usecache)

(define-init store-procs (make-hashtable))
(define-init add-procs (make-hashtable))
(define-init drop-procs (make-hashtable))
(define-init get-indices (make-hashtable))

(defambda (xo/store! oid slotid value)
  (do-choices oid
    (do-choices slotid
      (let ((method
	     (try (get store-procs (cons (getpool oid) slotid))
		  (get store-procs slotid))))
	(extindex-decache! (get get-indices (cons (getpool oid) slotid))
			   oid)
	(if (exists? method)
	    (method (if (fail? value) (qc) value) oid)
	    (store-with-edits oid slotid value)))))
  (store! (oid-value oid) slotid value))
(defambda (xo/add! oid slotid value)
  (do-choices oid
    (do-choices slotid
      (extindex-decache! (get get-indices (cons (getpool oid) slotid))
			 oid)
      (let ((method
	     (try (get add-procs (cons (getpool oid) slotid))
		  (get add-procs slotid))))
	(if (exists? method)
	    (method (if (fail? value) (qc) value) oid)
	    (add-with-store oid slotid value)))))
  (add! (oid-value oid) slotid value))
(defambda (xo/drop! oid slotid value)
  (do-choices oid
    (do-choices slotid
      (extindex-decache! (get get-indices (cons (getpool oid) slotid))
			 oid)
      (let ((method
	     (try (get drop-procs (cons (getpool oid) slotid))
		  (get drop-procs slotid))))
	(if (exists? method)
	    (method (if (fail? value) (qc) value) oid)
	    (drop-with-store oid slotid value)))))
  (drop! (oid-value oid) slotid value))

(defambda (store-with-edits oid slotid values)
  (let* ((current (get oid slotid))
	 (add (difference values current))
	 (drop (difference values current))
	 (add-method
	  (try (get add-procs (cons (getpool oid) slotid))
	       (get add-procs slotid)))
	 (drop-method
	  (try (get drop-procs (cons (getpool oid) slotid))
	       (get drop-procs slotid))))
    (if (and (or (fail? drop) (exists? drop-method))
	     (or (fail? add) (exists? add-method)))
	(begin (when (exists? add) (add-method values oid))
	  (when (exists? drop) (drop-method values oid)))
	(error "Can't store value on " slotid " of " oid))))

(defambda (drop-with-store oid slotid values)
  (let ((current (get oid slotid))
	(method
	 (try (get store-procs (cons (getpool oid) slotid))
	      (get store-procs slotid))))
    (if (exists? method)
	(when (exists? (intersection current values))
	  (method (qc (difference current values)) oid))
	(error "Can't drop values from " slotid " of " oid))))

(defambda (add-with-store oid slotid values)
  (let ((current (get oid slotid))
	(method
	 (try (get store-procs (cons (getpool oid) slotid))
	      (get store-procs slotid))))
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


