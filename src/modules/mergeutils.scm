;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'mergeutils)

;;; Tools for combining tables in different ways

(define (diffkeys table1 table2 (symmetric #t))
  (if symmetric
      (choice (difference (getkeys table1) (getkeys table2))
	      (difference (getkeys table2) (getkeys table1)))
      (difference (getkeys table1) (getkeys table2))))

(define (difftables table1 table2 (symmetric #t))
  (if symmetric
      (let ((keys (getkeys (choice table1 table2)))
	    (difftable (make-hashtable)))
	(do-choices (key keys)
		    (let ((adds (difference (get table1 key)
					    (get table2 key)))
			  (drops (difference (get table2 key)
					     (get table1 key))))
		      (add! difftable key (cons '+ adds))
		      (add! difftable key (cons '- drops))))
	difftable)
      (let ((keys (getkeys table1))
	    (difftable (make-hashtable)))
	(do-choices (key keys)
		    (let ((adds (difference (get table1 key)
					    (get table2 key)))
			  (drops (difference (get table2 key)
					     (get table1 key))))
		      (add! difftable key (cons '+ adds))
		      (add! difftable key (cons '- drops))))
	difftable)))

(define (realdiff t1 t2 base)
  (let ((realdiffs (make-hashtable))
	(keys (getkeys (choice t1 t2))))
    (do-choices (key keys)
      (unless (or (identical? (get t1 key) (get t2 key))
		  (fail? (get t1 key)) (fail? (get t2 key)))
	(store! realdiffs key (cons (qc (get t1 key)) (qc (get t2 key))))))
    realdiffs))

(define (merge-overlay-entries e1 e2 key)
  (cond ((and (vector? e1) (vector e2))
	 (sorted (choice (elts e1) (elts e2))))
	((and (pair? e1) (pair? e2))
	 (let ((adds (choice (car e1) (car e2)))
	       (drops (choice (cdr e1) (cdr e2))))
	   (when (overlaps? adds drops)
	     (message "Conflicing add/drops for " key
		      "\n\t" e1 "\n\t" e2
		      "\n\t" (cons (qc adds) (qc drops))))
	   (cons (qc adds) (qc drops))))
	(else (let* ((base (if (vector? e1) (elts e1) (elts e2)))
		     (edits (if (pair? e1) e1 e2))
		     (adds (car edits))
		     (drops (cdr edits)))
		(sorted (difference (choice base adds) drops))))))

(define (merge-overlays ov1 ov2 base)
  (let ((merge (make-hashtable))
	(keys (getkeys (choice ov1 ov2))))
    (do-choices (key keys)
      (let ((v1 (get ov1 key)) (v2 (get ov2 key)) (bv (get base key)))
	(store! merge key
		(cond ((and (fail? bv) (fail? v2)) v1)
		      ((and (fail? bv) (fail? v1)) v2)
		      ((identical? v1 v2) v1)
		      ((identical? v1 bv) v2)
		      ((identical? v2 bv) v1)
		      (else
		       (let ((new (merge-overlay-entries v1 v2 key)))
			 (message "Merging overlays for " key
				  "\n\t" v1 "\n\t" v2 "\n\t" new)
			 new))))))
    merge))

(module-export! '{diffkeys difftables realdiff merge-overlays})


