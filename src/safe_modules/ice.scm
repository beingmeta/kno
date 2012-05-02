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

(use-module 'reflection)

(module-export! '{ice/freeze ice/thaw})

;;; The process of dumping and restoring is controlled by three 


;;;; Top level functions

(defambda (ice/freeze roots pool (datarules #f) (slotrules #f))
  (let ((mapping (make-hashtable))
	(output (make-hashtable))
	(counter (list 0)))
    (store! output 'roots
	    (dump roots pool mapping output datarules slotrules counter))
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

(define (dump x pool mapping output (drules #f) (srules #f) (counter (list 0)))
  (if (oid? x)
      (let* ((inpool (getpool x)))
	(if (eq? inpool pool)
	    (try (get mapping x)
		 (let ((dop (make-oid 0 (car counter))))
		   (set-car! counter (1+ (car counter)))
		   (set-cdr! counter (cons dop (cdr counter)))
		   (store! mapping x dop)
		   (store! output dop
			   (dump (oid-value x) pool mapping output
				 drules srules counter))
		   dop))
	    (if (and drules (exists? (get drules inpool)))
		((get drules inpool) x mapping output
		 (lambda (v) (dump v pool mapping output drules srules counter)))
		x)))
      (if (pair? x)
	  (cons (dump (car x) pool mapping output drules srules counter)
		(dump (cdr x) pool mapping output drules srules counter))
	  (if (vector? x)
	      (map (lambda (e) (dump e pool mapping output drules srules counter))
		   x)
	      (if (timestamp? x) x
		  (if (table? x)
		      (let ((copy (if (hashtable? x) (make-hashtable) (frame-create #f)))
			    (method #f))
			(do-choices (key (getkeys x))
			  (set! method (tryif srules (get srules key)))
			  (if (fail? method)
			      (store! copy key
				      (dump (get x key) pool mapping output
					    drules srules counter))
			      (when method
				(store! copy
					(try (pick method slotid?) key)
					(dump (if (exists applicable? method)
						  ((pick method applicable?)
						   ;; value, newslot, container
						   (get x key) (try (pick method slotid?) key) x
						   ;; dumpfn
						   (lambda (v) (dump v pool mapping output drules srules counter)))
						  v)
					      pool mapping output
					      drules srules counter)))))
			copy)
		      (if (uuid? x)
			  (make-compound '|uuid| (uuid->packet x))
			  (if (and drules (compound-type? x)
				   (test drules (compound-tag x)))
			      ((get drules (compound-tag x)) x mapping output
			       (lambda (v) (dump v pool mapping output drules srules counter)))
			      x))))))))


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
		      (store! copy key
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


