;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'dopool)

(define version "$Id: dopool.scm 1631 2007-08-16 14:12:11Z haase $")

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

