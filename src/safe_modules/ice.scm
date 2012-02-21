(in-module 'ice)

(module-export! '{ice/freeze ice/thaw})

#|
The idea is to take a set of roots, a pool, and a table, descending from the
roots and storing an adapted value for every OID you encounter.  The adapted
|#

(define (dump x pool mapping output (counter (list 0)))
  (if (oid? x)
      (try (get mapping x)
	   (let ((dop (make-oid 0 (car counter))))
	     (store! mapping x dop)
	     (store! output dop
		     (dump (oid-value x) pool mapping output counter))
	     (set-car! counter (1+ (car counter)))
	     (set-cdr! counter (cons dop (cdr counter)))
	     dop))
      (if (pair? x)
	  (cons (dump (car x) pool mapping output counter)
		(dump (cdr x) pool mapping output counter))
	  (if (vector? x)
	      (map (lambda (e) (dump e pool mapping output counter))
		   x)
	      (if (table? x)
		  (let ((copy (if (hashtable? x) (make-hashtable)
				  (frame-create #f))))
		    (do-choices (key (getkeys x))
		      (store! copy key
			      (dump (get x key) pool mapping output
				    counter)))
		    copy)
		  x)))))

;; (define (restore x pool mapping input)
;;   (%watch (restore-inner x pool mapping input) x))

(define (restore x pool mapping input)
  (if (oid? x)
      (if (zero? (oid-hi x))
	  (try (get mapping x) x)
	  x)
      (if (pair? x)
	  (cons (restore (car x) pool mapping input)
		(restore (cdr x) pool mapping input))
	  (if (vector? x)
	      (map (lambda (e) (restore e pool mapping input))
		   x)
	      (if (table? x)
		  (let ((copy (if (hashtable? x) (make-hashtable)
				  (frame-create #f))))
		    (do-choices (key (getkeys x))
		      (store! copy key
			      (restore (get x key) pool mapping input)))
		    copy)
		  x)))))

(defambda (ice/freeze roots pool)
  (let ((mapping (make-hashtable))
	(output (make-hashtable))
	(counter (list 0)))
    (store! output 'roots
	    (dump roots pool mapping output counter))
    output))

(defambda (ice/thaw input pool)
  (let ((mapping (make-hashtable))
	(oids (pickoids (getkeys input))))
    (doseq (oid (sorted oids))
      (store! mapping oid (frame-create pool)))
    (do-choices (oid oids)
      (set-oid-value! (get mapping oid)
		      (restore (get input oid) pool mapping input)))
    (restore (get input 'roots) pool mapping input)))


