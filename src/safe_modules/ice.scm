;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2012 beingmeta, inc.  All rights reserved.

;;; This is a serialization module for FramerD structures, especially
;;;  ones partially constituted of "relative OIDs" whose identity is
;;;  scoped within a particular object.  For example, the OID-based
;;;  representation of a document where text nodes are implemented as
;;;  OIDs but whose absolute OID addresses don't matter as long as
;;;  their relations are properly reconstituted.

;;;  Serialization is based on three properties: a "dynamic pool," a
;;;  set of *data rules*, and a set of *slot rules*.  The idea is that
;;;  any OID in the dynamic pool will be dumped as @0/num where num is
;;;  increased for each member of the dynamic pool encountered.

;;;  For export, the data rules are maps of either pools or compound
;;;    tags to functions which produce representations that can be
;;;    written as DTYPEs.  In many cases, these will generate compound
;;;    objects (make-compound/compound-type?) whose tags represent
;;;    serialized representations distinct from their origins.

;;; For import, the data rules generally map compound tags into
;;;    functions for restoring serialized objects locally.

;;; For export, the slotrules either map slots to #f (indicating that
;;;    the slot is to be ignored), other slotids (indicating that the
;;;    slot is to be renamed), or functions, which are called to
;;;    generate a value to actually serialize.  The function signature
;;;    is (value,slot,whole) and the value returned is actually stored
;;;    in the slot (unless it's {}).

;;; For import, the slotrules work in exactly the same way, but in the
;;;    other direction.

(in-module 'ice)

(use-module '{reflection logger})

(module-export! '{ice/freeze ice/thaw})

(define %loglevel %debug!)


;;;; Top level functions

(defambda (ice/freeze roots pool (datarules #f) (slotrules #f))
  (let ((mapping (make-hashtable))
	(output (make-hashtable))
	(count 0) (counter #f))
    (do-choices (root roots)
      (cond ((%test mapping root))
	    ((oid? root)
	     (store! mapping root (make-oid 0 count))
	     (set! count (1+ count)))
	    ((hashtable? root)
	     (store! mapping root (make-compound '|hashtable_root| count))
	     (set! count (1+ count)))
	    ((slotmap? root)
	     (store! mapping root (make-compound '|slotmap_root| count))
	     (set! count (1+ count)))
	    (else)))
    (set! counter (list count))
    (store! output 'roots
	    (for-choices (root roots)
	      (if (%test mapping root)
		  (let ((dop (get mapping root)))
		    (store! output dop
			    (dump (if (oid? root) (oid-value root) root)
				  pool mapping output datarules slotrules
				  counter))
		    dop)
		  (dump root pool mapping output datarules slotrules
			counter))))
    output))

(defambda (ice/thaw input pool (slotrules #f) (recrules #f))
  (let ((mapping (make-hashtable))
	(oids (pickoids (getkeys input))))
    (doseq (oid (sorted oids))
      (store! mapping oid (frame-create pool)))
    (do-choices (oid oids)
      (set-oid-value! (get mapping oid)
		      (restore (get input oid) pool mapping input slotrules recrules)))
    (restore (get input 'roots) pool mapping input slotrules recrules)))


;;;; The main DUMP function

(define (dump x pool mapping output (drules #f) (srules #f)
	      (counter (list 0)) (depth 1))
  (logdebug "[" depth "] Dumping "
	    (typeof x) " #" (number->string (hashptr x) 16))
  (if (oid? x)
      (let* ((inpool (getpool x)))
	(if (eq? inpool pool)
	    (try (get mapping x)
		 (let ((dop (make-oid 0 (car counter))))
		   (set-car! counter (1+ (car counter)))
		   (set-cdr! counter (cons dop (cdr counter)))
		   (store! mapping x dop)
		   (logdebug "Dumping value of OID " x)
		   (store! output dop
			   (dump (oid-value x) pool mapping output
				 drules srules counter (1+ depth)))
		   dop))
	    (if (and drules (exists? (get drules inpool)))
		((get drules inpool) x mapping output
		 (lambda (v)
		   (dump v pool mapping output drules srules
			 counter (1+ depth))))
		x)))
      (if (pair? x)
	  (let ((scan x) (result '()))
	    (while (pair? scan)
	      (set! result
		    (cons (dump (car scan) pool mapping output drules srules
				counter (1+ depth))
			  result))
	      (set! scan (cdr scan)))
	    (if (null? scan) (reverse result)
		(let* ((backwards (reverse result))
		       (tail backwards))
		  (until (null? (cdr tail)) (set! tail (cdr tail)))
		  (set-cdr! tail
			    (dump scan pool mapping output drules srules
				  counter (1+ depth)))
		  backwards)))
	  (if (exists? (get mapping x))
	      (get mapping x)
	      (if (vector? x)
		  (map (lambda (e)
			 (dump e pool mapping output drules srules
			       counter (1+ depth)))
		       x)
		  (if (timestamp? x) x
		      (if (table? x)
			  (let ((copy (if (hashtable? x) (make-hashtable)
					  (frame-create #f))))
			    (do-choices (key (getkeys x))
			      (unless (and srules
					   (overlaps? (get srules key)
						      '{#f ignore discard}))
				(let* ((rules (tryif srules (get srules key)))
				       (method (pick rules applicable?))
				       (newkey (try (pick rules slotid?)
						    (dump key pool mapping output
							  drules srules counter
							  (1+ depth))))
				       (values (get x key)))
				  (if (fail? method)
				      (store! copy newkey
					      (dump values pool mapping output
						    drules srules counter
						    (1+ depth)))
				      (when method
					(store! copy newkey
						(dump (method
						       ;; value, oldslot, newslot, container
						       values key newkey x
						       ;; dumpfn
						       (lambda (v) (dump v pool mapping output drules srules counter)))
						      pool mapping output
						      drules srules counter
						      (1+ depth))))))))
			    copy)
			  (if (uuid? x)
			      (make-compound '|uuid| (uuid->packet x))
			      (if (and drules (compound-type? x)
				       (test drules (compound-tag x)))
				  ((get drules (compound-tag x)) x mapping output
				   (lambda (v)
				     (dump v pool mapping output drules srules
					   counter (1+ depth))))
				  x)))))))))


;;;; The main RESTORE function

(define (restore x pool mapping input (drules #f) (srules #f))
  (if (oid? x)
      (if (zero? (oid-hi x))
	  (try (get mapping x) x)
	  x)
      (if (pair? x)
	  (cons (restore (car x) pool mapping input drules srules)
		(restore (cdr x) pool mapping input drules srules))
	  (if (vector? x)
	      (map (lambda (e) (restore e pool mapping input drules srules))
		   x)
	      (if (table? x)
		  (let ((copy (if (hashtable? x) (make-hashtable)
				  (frame-create #f))))
		    (do-choices (key (getkeys x))
		      (store! copy
			      (restore key pool mapping input drules srules)
			      (if (or (not srules) (fail? (get srules key)))
				  (restore (get x key) pool mapping input drules srules)
				  ((get srules key) (get x key) key x
				   (lambda (y) (restore y pool mapping input drules srules))))))
		    copy)
		  (if (compound-type? x)
		      (try (get mapping x)
			   (if (and drules (exists? (get drules (compound-tag x))))
			       ((get drules (compound-tag x)) x
				(lambda (y) (restore y pool mapping input drules srules)))
			       (if (compound-type? x '|uuid|)
				   (getuuid (compound-ref x 0))
				   x)))
		      x))))))


