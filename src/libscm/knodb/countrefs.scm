;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb/countrefs)

(use-module '{logger varconfigkno/mttools kno/reflect})

(module-export! '{countrefs countrefs/vec countrefs/pool})

(defambda (ref-counter object freqs (alloids #f))
  (cond ((ambiguous? object)
	 (do-choices (elt object)
	   (cond ((symbol? elt) (table-increment! freqs elt))
		 ((oid? elt) 
		  (table-increment! freqs (oid-base elt))
		  (when alloids (table-increment! freqs elt)))
		 ((not (cons? elt)))
		 ((or (table? elt) (vector? elt) (compound? elt))
		  (ref-counter elt freqs alloids)))))
	((symbol? object) (table-increment! freqs object))
	((oid? object)
	 (if alloids (table-increment! freqs object))
	 (table-increment! freqs (oid-base object)))
	((pair? object)
	 (if (proper-list? object)
	     (dolist (elt object) (ref-counter elt freqs alloids))
	     (let ((scan object))
	       (while (pair? scan)
		 (ref-counter (car scan) freqs alloids)
		 (set! scan (cdr scan))))))
	((or (vector? object) (and (sequence? object) (compound? object)))
	 (doseq (elt object) (ref-counter elt freqs alloids)))
	((table? object)
	 (do-choices (key (getkeys object))
	   (when (symbol? key) (table-increment! freqs key))
	   (when (oid? key)
	     (table-increment! freqs key)
	     (table-increment! freqs (oid-base key)))
	   (ref-counter (get object key) freqs alloids)))
	(else)))

;;;; Simple countrefs

(defambda (countrefs objects (freqs (make-hashtable)) (opts #f))
  (ref-counter objects freqs (getopt opts 'alloids #f)))

;;; Vector countrefs

(define (countrange freqs vec start n alloids (elt))
  (dotimes (i n)
    (set! elt (elt vec (+ start i)))
    (ref-counter elt freqs alloids)))

(define (countrefs/vec vec (opts #f) (freqs) (nthreads) (alloids))
  (default! freqs (getopt opts 'freqs (make-hashtable)))
  (default! nthreads (mt/threadcount (getopt opts 'nthreads (config 'nthreads #t))))
  (default! alloids (getopt opts 'alloids #f))
  (let* ((n (length vec))
	 (tables {}) (threads {})
	 (batch-size (quotient n nthreads)))
    (dotimes (i nthreads)
      (let ((table (make-hashtable)))
	(set+! threads (thread/call countrange
			   table vec (* i batch-size)
			   (min (- n (* i batch-size)) batch-size)
			   alloids))
	(set+! tables table)))
    (thread/wait! threads)
    (do-choices (table tables)
      (do-choices (key (getkeys table))
	(table-increment! freqs key (get table key)))))
  freqs)

;;;; Pool countrefs

(define (countoids freqs pool base n alloids (oid))
  (dotimes (i n)
    (set! oid (oid-plus base i))
    (ref-counter (get pool oid) freqs alloids)
    (swapout pool oid)))

(define (countrefs/pool pool (opts #f) (freqs) (nthreads) (alloids))
  (default! freqs (getopt opts 'freqs (make-hashtable)))
  (default! nthreads (mt/threadcount (getopt opts 'nthreads (config 'nthreads #t))))
  (default! alloids (getopt opts 'alloids #f))
  (if nthreads
      (let* ((tables {}) (threads {}) 
	     (load (pool-load pool))
	     (batch-size (quotient load nthreads))
	     (base (pool-base pool)))
	(dotimes (i nthreads)
	  (let ((table (make-hashtable)))
	    (set+! threads (thread/call countoids
			       table pool (oid-plus base (* i batch-size))
			       (min (- load (* i batch-size)) batch-size)
			       alloids))
	    (set+! tables table)))
	(thread/wait! threads)
	(do-choices (table tables)
	  (do-choices (key (getkeys table))
	    (table-increment! freqs key (get table key)))))
      (countoids freqs pool (pool-base pool) (pool-load pool) alloids))
  freqs)

