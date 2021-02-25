;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'dopool)

;;; This provides macros for iteration across pools

(define dopool
  (macro expr
    (let* ((control-expr (cadr expr))
	   (control-var (car control-expr))
	   (pool-param (cadr control-expr))
	   (blocksize-param (if (= (length control-expr) 3)
				(third control-expr)
				16384))
	   (body (cddr expr)))
      `(let* ((%pool (use-pool ,pool-param))
	      (blocksize ,blocksize-param)
	      (poolvec (pool-vector %pool))
	      (len (length poolvec))
	      (n-blocks (1+ (quotient len blocksize))))
	 (dotimes (i n-blocks)
	   (let* ((offset (* i blocksize))
		  (chunksize (min blocksize (- len offset))))
	     (message "Prefetching " chunksize " frames")
	     (prefetch-oids!
	      (elts (subseq poolvec offset (+ offset chunksize))))
	     (dotimes (j chunksize)
	       (let ((,control-var (elt poolvec (+ offset j))))
		 ,@body))))))))



(module-export! 'dopool)

