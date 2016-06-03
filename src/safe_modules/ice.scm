;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2016 beingmeta, inc.  All rights reserved.

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

(define-init %loglevel %notify%)


;;;; Top level functions

(defambda (ice/freeze obj pool (roots) (datarules #f) (slotrules #f))
  (default! roots obj)
  (let ((mapping (make-hashtable))
	(output (make-hashtable))
	(rootset (make-hashset))
	(dumped (make-hashset))
	(count 0) (state #f))
    ;; Make placeholders
    (loginfo "Allocating dopplegangers for " (choice-size roots) " roots")
    (do-choices (root roots)
      (unless (%test mapping root)
	(let ((dop (if (oid? root) (make-oid 0 count)
		       (if (hashtable? root)
			   (make-compound '|hashtable| count)
			   (if (slotmap? root)
			       (make-compound '|slotmap| count)
			       (if (hashset? root)
				   (make-compound '|hashset| count)
				   #f))))))
	  (when dop
	    (hashset-add! rootset root)
	    (store! mapping root dop)
	    (set! count (1+ count))))))
    (set! state (cons count rootset))
    ;; Dump the initial roots
    (loginfo "Saving data for dopplegangers")
    (do-choices (root (pick roots mapping))
      (let ((dop (get mapping root)))
	(store! output dop
		(dump (if (oid? root) (oid-value root) root)
		      pool mapping output datarules slotrules
		      state))
	(hashset-add! dumped root)
	dop))
    ;; Now store the object
    (loginfo "Saving the core object")
    (store! output '%obj
	    (dump obj pool mapping output datarules slotrules
		  state))
    ;; And save any newly discovered roots
    (let ((todo (reject (hashset-elts rootset) dumped)))
      (when (fail? todo) (loginfo "No additional roots to save"))
      (until (fail? todo)
	(loginfo "Saving information for " (choice-size todo) " new roots")
	(do-choices (root todo)
	  (store! output (get mapping root)
		  (dump (if (oid? root) (oid-value root) root)
			pool mapping output datarules slotrules
			state))
	  (hashset-add! dumped root))
	(set! todo (reject (hashset-elts rootset) dumped))))
    output))

(defambda (ice/thaw input pool (slotrules #f) (recrules #f))
  (let* ((mapping (make-hashtable))
	 (roots (difference (getkeys input) '%obj)))
    (do-choices (root roots)
      (store! mapping root
	      (if (oid? root) (frame-create pool)
		  (if (compound-type? root '|hashtable|)
		      (make-hashtable)
		      (if (compound-type? root '|slotmap|)
			  (frame-create #f)
			  (if (compound-type? root '|hashset|)
			      (make-hashset)
			      {}))))))
    (do-choices (root roots)
      (restore-into (get mapping root) (get input root)
		    pool mapping input slotrules recrules))
    (restore (get input '%obj) pool mapping input slotrules recrules)))


;;;; The main DUMP function

(define (dump x pool mapping output (drules #f) (srules #f)
	      (state (cons 0 (make-hashset))) (depth 1))
  (logdetail "[" depth "] Dumping "
	     (typeof x) " #" (number->string (hashptr x) 16))
  (if (oid? x)
      (let* ((inpool (getpool x)))
	(if (eq? inpool pool)
	    (try (get mapping x)
		 (let ((dop (make-oid 0 (car state))))
		   (set-car! state (1+ (car state)))
		   (hashset-add! (cdr state) x)
		   (store! mapping x dop)
		   dop))
	    (if (and drules (exists? (get drules inpool)))
		((get drules inpool) x mapping output pool state
		 (lambda (v)
		   (dump v pool mapping output drules srules
			 state (1+ depth))))
		x)))
      (if (and (pair? x) (pair? (cdr x)))
	  ;; Only recur in one direction to keep the stack smaller
	  (let ((scan x) (result '()))
	    (while (pair? scan)
	      (set! result
		    (cons (dump (car scan) pool mapping output drules srules
				state (1+ depth))
			  result))
	      (set! scan (cdr scan)))
	    (if (null? scan)
		(reverse result)
		;; improper list, a little tricky
		(let* ((backwards (reverse result))
		       (tail backwards))
		  (until (null? (cdr tail)) (set! tail (cdr tail)))
		  (set-cdr! tail
			    (dump scan pool mapping output drules srules
				  state (1+ depth)))
		  backwards)))
	  (if (pair? x)
	      (cons (dump (car x) pool mapping output drules srules
			  state (1+ depth))
		    (dump (cdr x) pool mapping output drules srules
			  state (1+ depth)))
	      (try (tryif (> depth 1) (get mapping x))
		   (if (vector? x)
		       (map (lambda (e)
			      (dump e pool mapping output drules srules
				    state (1+ depth)))
			    x)
		       ;; Timestamps are tables, so we test them first
		       (if (timestamp? x) x
			   (if (table? x)
			       (dump-table x pool mapping output
					   drules srules state depth)
			       (if (uuid? x)
				   (make-compound '|uuid| (uuid->packet x))
				   (if (and drules (compound-type? x)
					    (test drules (compound-tag x)))
				       ((get drules (compound-tag x))
					x mapping output
					(lambda (v)
					  (dump v pool mapping output drules srules
						state (1+ depth))))
				       x))))))))))

(define (dump-table x pool mapping output (drules #f) (srules #f)
		    (state (cons 0 (make-hashset))) (depth 1))
  (if (hashset? x)
      (let ((copy (make-hashset)))
	(do-choices (elt (hashset-elts x))
	  (hashset-add! copy (dump elt pool mapping output
				   drules srules state (1+ depth))))
	copy)
      (let ((copy (if (hashtable? x) (make-hashtable) (frame-create #f))))
	(do-choices (key (getkeys x))
	  (unless (and srules (overlaps? (get srules key) '{#f ignore discard}))
	    (let* ((rules (tryif srules (get srules key)))
		   (method (pick rules applicable?))
		   (newkey (try (pick rules slotid?)
				(dump key pool mapping output
				      drules srules state (1+ depth))))
		   (dumpfn (lambda (v)
			     (dump v pool mapping output
				   drules srules state (1+ depth))))
		   (values (get x key))
		   (newvalues (if (fail? method) values
				  ;; value, oldslot, newslot, container, dumpfn
				  (method values key newkey x dumpfn))))
	      (store! copy newkey
		      (dump newvalues pool mapping output
			    drules srules state (1+ depth))))))
	copy)))
  
  
;;;; The main RESTORE function

(define (restore x pool mapping input (drules #f) (srules #f) (depth 1))
  (cond ((oid? x) (try (get mapping x) x))
	((or (symbol? x) (number? x) (string? x) (packet? x) (timestamp? x)) x)
	((and (pair? x) (pair? (cdr x)))
	 ;; Only recur in one direction to keep the stack smaller
	 (let ((scan x) (result '()))
	   (while (pair? scan)
	     (set! result
		   (cons (restore (car scan) pool mapping input drules srules (1+ depth))
			 result))
	     (set! scan (cdr scan)))
	   (if (null? scan) (reverse result)
	       ;; improper list, a little tricky
	       (let* ((backwards (reverse result))
		      (tail backwards))
		 (until (null? (cdr tail)) (set! tail (cdr tail)))
		 (set-cdr! tail
			   (restore scan pool mapping input drules srules (1+ depth)))
		 backwards))))
	((pair? x)
	 (cons (restore (car x) pool mapping input drules srules (1+ depth))
	       (restore (cdr x) pool mapping input drules srules (1+ depth))))
	((vector? x)
	 (map (lambda (e) (restore e pool mapping input drules srules (1+ depth)))
	      x))
	((hashset? x)
	 (let ((copy (make-hashset)))
	   (do-choices (elt (hashset-elts x))
	     (hashset-add! copy
			   (restore elt pool mapping input drules srules (1+ depth))))
	   copy))
	((table? x)
	 (let ((copy (if (hashtable? x) (make-hashtable)
			 (frame-create #f))))
	   (do-choices (key (getkeys x))
	     (store! copy
		     (restore key pool mapping input drules srules (1+ depth))
		     (if (or (not srules) (fail? (get srules key)))
			 (restore (get x key) pool mapping input drules srules (1+ depth))
			 ((get srules key) (get x key) key x
			  (lambda (y) (restore y pool mapping input drules srules (1+ depth)))))))
	   copy))
	((compound-type? x)
	 (try (get mapping x)
	      (if (and drules (exists? (get drules (compound-tag x))))
		  ((get drules (compound-tag x)) x
		   (lambda (y) (restore y pool mapping input drules srules (1+ depth))))
		  (if (compound-type? x '|uuid|)
		      (getuuid (compound-ref x 0))
		      x))
	      x))
	(else x)))

(define (restore-into into x pool mapping input (drules #f) (srules #f))
  (unless (or (hashtable? x) (slotmap? x))
    (error "Internal restore error, can't restore non-table "
	   x " into " into))
  (do-choices (key (getkeys x))
    (store! into
	    (restore key pool mapping input drules srules)
	    (if (or (not srules) (fail? (get srules key)))
		(restore (get x key) pool mapping input drules srules)
		((get srules key) (get x key) key x
		 (lambda (y) (restore y pool mapping input drules srules))))))
  into)
  

