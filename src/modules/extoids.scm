(in-module 'extoids)

(define version "$Id$")
(define revision "$Revision$")

(use-module '{extdb varconfig})

(define default-sqlmap #[])
(varconfig! extdb:sqlmap default-sqlmap)

(module-export! '{exto/store! exto/add! exto/drop!})
(module-export! '{exto/defstore exto/defadd exto/defdrop exto/defget})

(define store-procs (make-hashtable))
(define add-procs (make-hashtable))
(define drop-procs (make-hashtable))

(define (exto/store! oid slotid value)
  ((try (get store-procs (cons (getpool oid) slotid))
	(get store-procs slotid))
   value oid)
  (store! (oid-value oid) slotid value))
(define (exto/add! oid slotid value)
  ((try (get add-procs (cons (getpool oid) slotid))
	(get add-procs slotid))
   value oid)
  (add! (oid-value oid) slotid value))
(define (exto/drop! oid slotid value)
  ((try (get drop-procs (cons (getpool oid) slotid))
	(get drop-procs slotid))
   value oid)
  (drop! (oid-value oid) slotid value))

(define (exto/defstore pool slotid db query (valtype #f))
  (store! store-procs
	  (if pool (cons pool slotid) slotid)
	  (extdb/proc db query (qc default-sqlmap) valtype (pool-base pool))))
(define (exto/defadd pool slotid db query (valtype #f))
  (store! add-procs
	  (if pool (cons pool slotid) slotid)
	  (extdb/proc db query (qc default-sqlmap) valtype (pool-base pool))))
(define (exto/defdrop pool slotid db query (valtype #f))
  (store! drop-procs
	  (if pool (cons pool slotid) slotid)
	  (extdb/proc db query (qc default-sqlmap) valtype (pool-base pool))))

(define (exto/defget pool slotid db query (sqlmap default-sqlmap))
  (let* ((getter (extdb/proc db query (qc sqlmap) (pool-base pool)))
	 (index (make-extindex (stringout slotid) getter)))
    (use-adjunct index slotid pool)))


