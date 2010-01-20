(in-module 'extoids)

(define version "$Id$")
(define revision "$Revision$")

(use-module '{extdb varconfig reflection})

(define default-sqlmap #[])
(varconfig! extdb:sqlmap default-sqlmap)

(module-export! '{xo/store! xo/add! xo/drop!})
(module-export! '{xo/defstore xo/defadd xo/defdrop xo/defget})

(define store-procs (make-hashtable))
(define add-procs (make-hashtable))
(define drop-procs (make-hashtable))

(define (xo/store! oid slotid value)
  ((try (get store-procs (cons (getpool oid) slotid))
	(get store-procs slotid))
   value oid)
  (store! (oid-value oid) slotid value))
(define (xo/add! oid slotid value)
  ((try (get add-procs (cons (getpool oid) slotid))
	(get add-procs slotid))
   value oid)
  (add! (oid-value oid) slotid value))
(define (xo/drop! oid slotid value)
  ((try (get drop-procs (cons (getpool oid) slotid))
	(get drop-procs slotid))
   value oid)
  (drop! (oid-value oid) slotid value))

(define (xo/defstore pool slotid db query (valtype #f))
  (store! store-procs
	  (if pool (cons pool slotid) slotid)
	  (extdb/proc db query (qc default-sqlmap) valtype (pool-base pool))))
(define (xo/defadd pool slotid db query (valtype #f))
  (store! add-procs
	  (if pool (cons pool slotid) slotid)
	  (extdb/proc db query (qc default-sqlmap) valtype (pool-base pool))))
(define (xo/defdrop pool slotid db query (valtype #f))
  (store! drop-procs
	  (if pool (cons pool slotid) slotid)
	  (extdb/proc db query (qc default-sqlmap) valtype (pool-base pool))))

(defambda (xo/defget pool slotid db query
		     (sqlmap default-sqlmap)
		     (normalize #f))
  (let* ((getter/sql (extdb/proc db query (cons #[%merge #t] sqlmap)
				 (pool-base pool)))
	 (getter (cond ((not normalize) getter/sql)
		       ((applicable? normalize)
			(lambda (x) (normalize (getter/sql x))))
		       ((slotid? normalize)
			(lambda (x) (get  (getter/sql x) normalize)))
		       ((table? normalize)
			(lambda (x) (get normalize (getter/sql x))))))
	 (index (make-extindex (stringout slotid) getter)))
    (use-adjunct index slotid pool)))


