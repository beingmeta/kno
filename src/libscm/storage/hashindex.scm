;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/hashindex)

(use-module '{reflection logger logctl stringfmts mttools engine})

(module-export! '{repack-index! index-bucketmap copy-buckets repack-hashindex!})

(define %loglevel %info%)

(defambda (index-bucketmap in out (mod #t) (keys))
  (default! keys (getkeys in))
  (let ((table (make-hashtable)))
    (do-choices (key keys)
      (add! table (indexctl out 'hash key mod) key))
    table))

(defambda (copy-keys from to (keys) (bucketmap) (opts #f))
  (default! keys (getkeys from))
  (default! bucketmap (index-bucketmap from to #t keys))
  (let* ((buckets (getkeys bucketmap)))
    (let* ((copy-queue (choice->vector buckets))
	   (batchlim (getopt opts 'batchlim (config 'BATCHLIM 1000000)))
	   (len (length copy-queue))
	   (n-keys (choice-size keys))
	   (copied 0)
	   (i 0))
      (while  (< i len)
	(let ((batch {}) (batchsize 0))
	  (while (< batchsize batchlim)
	    (let ((bucketno (elt copy-queue i))
		  (keys (get bucketmap i)))
	      (set+! batch keys)
	      (set! batchsize (+ batchsize (choice-size keys)))
	      (set! i (1+ i))))
	  (loginfo |RepackIndex| "Copying " batchsize " keys" )
	  (when (getopt opts 'prefetch #t)
	    (prefetch-keys! from batch))
	  (do-choices (key batch) (store! to key (get from key)))
	  (commit to)
	  (set! copied (+ copied batchsize))
	  (lognotice |RepackIndex|
	    "Copied " batchsize " additional keys, currently " copied "/" n-keys 
	    " (" (show% copied n-keys) ")")
	  (clearcaches))))))

(define (get-output-size in n-keys opts)
  (or (getopt opts 'size #f)
      (config 'NEWSIZE #f)
      (and (getopt opts 'keepsize (config 'KEEPSIZE))
	   (indexctl in 'hash))
      (* 3 n-keys)))

(defambda (get-index-slotids keys (table (make-hashtable)))
  (do-choices (key keys)
    (when (pair? key) (hashtable-increment! table (car key))))
  (rsorted (getkeys table) table))

(define (repack-index! infile outfile (opts #f))
  (let* ((in (open-index infile #[register #t cachelevel 2])))
    (logwarn |RepackIndex| "Extracting keys from " in)
    (let ((keys (getkeys in)))
      (let ((newsize (get-output-size in (choice-size keys) opts)))
	(unless (file-exists? outfile)
	  (logwarn |NewIndex| 
	    "Creating new hash index " outfile " with " newsize " slots")
	  (make-index outfile
		      `#[type hashindex register #t cachelevel 2
			 size ,newsize]))
	(let ((out (open-index outfile #[register #t cachelevel 2])))
	  (logwarn |Copying keys|
	    "Copying values for " (choice-size keys) " keys "
	    "from " in " to " out)
	  (let ((bucketmap (index-bucketmap in out #t keys)))
	    (copy-keys in out keys bucketmap)))))))

(define (repack-hashindex! infile outfile (opts #f))
  (let* ((in (open-index infile #[register #t cachelevel 2]))
	 (restart (getopt opts 'restart (CONFIG 'RESTART #f))))
    (logwarn |RepackIndex| "Extracting keys from " in)
    (let* ((keys (getkeys in))
	   (buckets (indexctl in 'buckets))
	   (newsize (get-output-size in (choice-size keys) opts)))
      (when (and restart (file-exists? outfile))
	(remove-file outfile))
      (unless (file-exists? outfile)
	(logwarn |NewIndex| 
	  "Creating new hash index " outfile " with " newsize " slots for "
	  (choice-size keys) " keys, reading " (choice-size buckets) " "
	  "buckets from " infile)
	(make-index outfile
		    `#[type hashindex register #t cachelevel 2
		       size ,newsize]))
      (let ((out (open-index outfile #[register #t cachelevel 2])))
	(engine/run copy-buckets buckets
		    `#[loop #[from ,in to ,out]
		       batchsize ,(getopt opts 'batchsize (config 'BATCHSIZE 10000))
		       batchrange ,(getopt opts 'batchrange (config 'BATCHRANGE 8))
		       nthreads ,(getopt opts 'nthreads (config 'NTHREADS (rusage 'ncpus)))
		       checkfreq 60
		       checktests ,(engine/interval 180)
		       checkstate ,out
		       logfreq 30
		       checkfreq ,(getopt 'checkfreq 120)])))))

(define (repack-hashindex! infile outfile (opts #f))
  (let* ((in (open-index infile #[register #t cachelevel 2]))
	 (restart (getopt opts 'restart (CONFIG 'RESTART #f))))
    (logwarn |RepackIndex| "Extracting keys from " in)
    (let* ((keys (getkeys in))
	   (newsize (get-output-size in (choice-size keys) opts)))
      (when (and restart (file-exists? outfile))
	(remove-file outfile))
      (unless (file-exists? outfile)
	(logwarn |NewIndex| 
	  "Creating new hash index " outfile " with " newsize " slots for "
	  (choice-size keys) " keys")
	(make-index outfile
		    `#[type hashindex register #t cachelevel 2
		       size ,newsize]))
      (let ((out (open-index outfile #[register #t cachelevel 2])))
	(engine/run copy-keyvals keys
		    `#[loop #[from ,in to ,out]
		       batchsize ,(getopt opts 'batchsize (config 'BATCHSIZE 10000))
		       batchrange ,(getopt opts 'batchrange (config 'BATCHRANGE 8))
		       nthreads ,(getopt opts 'nthreads (config 'NTHREADS (rusage 'ncpus)))
		       checkfreq 60
		       checktests ,(engine/interval 180)
		       checkstate ,out
		       logfreq 30
		       checkfreq ,(getopt 'checkfreq 120)])))))

(defambda (copy-buckets buckets batch-state loop-state task-state)
  (let* ((from (get loop-state 'from))
	 (to (get loop-state 'to))
	 (info (indexctl from 'buckets buckets))
	 (table (make-hashtable))
	 (keys (get info 'key)))
    (index-prefetch! from keys)
    (do-choices (key keys)
      (store! table key (get from key)))
    (index-merge! to table)
    (swapout from keys)))

(defambda (copy-keyvals keys batch-state loop-state task-state)
  (let* ((from (get loop-state 'from))
	 (to (get loop-state 'to))
	 (table (make-hashtable)))
    (index-prefetch! from keys)
    (do-choices (key keys)
      (store! table key (get from key)))
    (index-merge! to table)
    (swapout from keys)))

