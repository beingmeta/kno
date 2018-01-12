;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/hashindex)

(use-module '{reflection logger logctl stringfmts mttools fifo engine})

(module-export! '{hashindex/mapkeys
		  hashindex/counts
		  hashindex/repack!
		  hashindex/repack-keys
		  hashindex/repack-buckets
		  hashindex/repack-mapkeys})

(define %loglevel %info%)

;; Support functions


(define (get-new-size n-keys opts (minimum))
  (default! minimum (getopt opts 'minsize (config 'MINSIZE 100)))
  (let ((specified (getopt opts 'newsize (config 'newsize 2.0))))
    (max
     (if (exact? specified)
	 specified
	 (->exact (ceiling (* n-keys specified))))
     minimum)))

(defambda (new-index file old opts)
  (let* ((n-keys (indexctl old 'metadata 'keys))
	 (size (get-new-size n-keys opts))
	 (type (getopt opts 'type (config 'NEWTYPE (CONFIG 'TYPE 'hashindex))))
	 (new (make-index file
			  `#[type ,type size ,size
			     register ,(getopt opts 'register #t)])))
    (logwarn |NewIndex|
      "Created new " type " " file " with size " ($num size))
    new))

;;; Mapping over buckets in a hashindex

(define (get-spans n-buckets span-width)
  (let ((n-spans (1+ (quotient n-buckets span-width)))
	(spans {}))
    (dotimes (i n-spans)
      (let ((bottom (* i span-width))
	    (top (* (1+ i) span-width)))
	(unless (> bottom n-buckets)
	  (set! top (min top n-buckets))
	  (set+! spans (cons bottom top)))))
    spans))

(define (hashindex/mapkeys mapfn index (opts #f))
  (let* ((n-buckets (indexctl index 'hash))
	 (n-keys (indexctl index 'metadata 'keys))
	 (span-width (config 'WIDTH 500000))
	 (spans (get-spans n-buckets span-width))
	 (loop-init (getopt opts 'loop #[])))
    (let ((loopfn (lambda (span batch-state loop-state task-state)
		    (mapfn (indexctl index 'buckets (car span) (cdr span))
			   batch-state loop-state task-state)))
	  (opts (cons `#[loop ,loop-init] opts)))
      (store! loop-init 'index index)
      (engine/run loopfn spans opts))))

;;; Getting index-sizes by iterating over buckets

(defambda (key-counter keys batch-state loop-state task-state)
  (let ((counts (get loop-state 'counts))
	(indexes (get loop-state 'indexes))
	(rare (try (get loop-state 'rare) #f))
	(batch-counts (make-hashtable))
	(record-keys {})
	(rare-count 0))
    (do-choices (keyinfo keys)
      (add! batch-counts (get keyinfo 'count) (get keyinfo 'key))
      (cond ((not rare)
	     (set+! record-keys (get keyinfo 'key)))
	    ((test keyinfo 'value)
	     (add! rare (get keyinfo 'key) (get keyinfo 'value))
	     (set! rare-count (1+ rare-count)))
	    (else (set+! record-keys (get keyinfo 'key)))))
    (do-choices (count (getkeys batch-counts))
      (hashtable-increment! counts (get batch-counts count) count))
    (add! indexes record-keys (get loop-state 'index))
    (store! batch-state 'keys (choice-size keys))
    (store! batch-state 'rare rare-count)))

(define (hashindex/counts index (counts) (indexes) (rare))
  (default! counts (make-hashtable (indexctl index 'metadata 'keys)))
  (default! indexes (make-hashtable (indexctl index 'metadata 'keys)))
  (default! rare (make-hashtable (indexctl index 'metadata 'keys)))
  (hashindex/mapkeys key-counter index
		     `#[batchsize 1
			loop #[index ,index counts ,counts indexes ,indexes rare ,rare]
			count-term "spans"
			counters {keys rare}
			logfns ,engine/log
			logfreq 45])
  `#[counts ,counts indexes ,indexes rare ,rare])

;;; Repacking an index using hashindex/mapkeys

(defambda (key-packer keys batch-state loop-state task-state (n-keys) (separate))
  (set! n-keys (choice-size keys))
  (set! separate (getopt loop-state 'rare))
  (let ((index (get loop-state 'index))
	(thresh (get loop-state 'thresh))
	(output (make-hashtable))
	(rare (make-hashtable))
	(tofetch (make-vector n-keys))
	(fetch-count 0))
    (do-choices (keyinfo keys)
      (cond ((and thresh (< (get keyinfo 'count) thresh)))
	    ((and separate (test keyinfo 'value))
	     (add! rare (get keyinfo 'key) (get keyinfo 'value)))
	    (else 
	     (vector-set! tofetch fetch-count (get keyinfo 'key))
	     (set! fetch-count (1+ fetch-count)))))
    (let* ((keyvec (slice tofetch 0 fetch-count))
	   (keyvals (index/fetchn index keyvec)))
      (dotimes (i fetch-count)
	(add! output (elt keyvec i) (elt keyvals i))))
    (index-merge! (get loop-state 'output) output)
    (index-merge! (get loop-state 'rare) rare)
    (store! batch-state 'keys (choice-size keys))
    (store! batch-state 'fetched fetch-count)
    (store! batch-state 'separated (table-size rare))))

(define (hashindex/repack-mapkeys index output opts (rare))
  (default! rare (getopt opts 'rare #f))
  (let* ((thresh (getopt opts 'thresh #f))
	 (loop-init `#[index ,index output ,output 
		       rare ,(and (not thresh) rare)
		       thresh ,thresh]))
    (hashindex/mapkeys key-packer index
		       `#[batchsize 1
			  loop ,loop-init
			  count-term "spans"
			  counters {keys fetched separated}
			  logrates {keys fetched separated}
			  checkpoint {,output ,rare}
			  checkfreq 15
			  checktests ,(engine/interval (getopt opts 'savefreq (config 'savefreq 60)))
			  ;; checktests ,(engine/usage 'memusage (* 10 #gib))
			  loglevel ,%info%
			  nthreads ,(getopt opts 'nthreads (config 'NTHREADS 4))
			  ;; nthreads 3
			  logchecks #t
			  logfns ,engine/log
			  logfreq 45])))

;;; Repacking by buckets

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

(define (hashindex/repack-buckets in out (opts #f))
  (let ((n-keys (indexctl in 'metadata 'keys))
	(buckets (indexctl in 'buckets)))
    (engine/run copy-buckets buckets
		`#[loop #[from ,in to ,out]
		   batchsize ,(getopt opts 'batchsize (config 'BATCHSIZE 10000))
		   batchrange ,(getopt opts 'batchrange (config 'BATCHRANGE 8))
		   nthreads ,(getopt opts 'nthreads (config 'NTHREADS (rusage 'ncpus)))
		   checkfreq 15
		   checktests ,(engine/interval (getopt opts 'savefreq (config 'savefreq 60)))
		   checkpoint ,out
		   logfreq 30
		   checkfreq ,(getopt opts 'checkfreq 120)])))


;;; Repacking by keys

(defambda (copy-keyvals keys batch-state loop-state task-state)
  (let* ((from (get loop-state 'from))
	 (to (get loop-state 'to))
	 (table (make-hashtable)))
    (index-prefetch! from keys)
    (do-choices (key keys)
      (store! table key (get from key)))
    (index-merge! to table)
    (swapout from keys)))

(define (hashindex/repack-keys in out (opts #f))
  (let ((keys (getkeys in)))
    (engine/run copy-keyvals keys
		`#[loop #[from ,in to ,out]
		   batchsize ,(getopt opts 'batchsize (config 'BATCHSIZE 10000))
		   batchrange ,(getopt opts 'batchrange (config 'BATCHRANGE 8))
		   nthreads ,(getopt opts 'nthreads (config 'NTHREADS (rusage 'ncpus)))
		   checkfreq 15
		   checktests ,(engine/interval (getopt opts 'savefreq (config 'savefreq 60)))
		   checkpoint ,out
		   logfreq 30
		   checkfreq ,(getopt 'checkfreq 120)])))

;;; Repacking the hashindex

(define default-method hashindex/repack-mapkeys)

(define (hashindex/repack! infile (outfile #f) (opts #f) (method) (rarefile) (in #f))
  (default! method (getopt opts 'method default-method))
  (when (symbol? method) (set! method (getopt method-names method default-method)))
  (set! rarefile (getopt opts 'rare (config 'RARE #f)))
  (when (index? infile)
    (set! in infile)
    (set! infile (index-source in)))
  (cond ((not (string? infile))
	 (irritant infile |NotAFilename| hashindex/repack!))
	((not (file-exists? infile))
	 (irritant infile |MissingFilename| hashindex/repack!))
	((and outfile (not (string? outfile)))
	 (irritant infile |NotAFilename| hashindex/repack!))
	((and rarefile (not (string? rarefile)))
	 (irritant rarefile |NotAFilename| hashindex/repack!))
	((and outfile (file-exists? outfile) 
	      (not (getopt opts 'overwrite (config 'overwrite #f))))
	 (irritant outfile |AlreadyExists| hashindex/repack!))
	((and outfile (file-exists? outfile) (not (file-writable? outfile)))
	 (irritant outfile |Unwritable| hashindex/repack!))
	((and outfile (file-exists? outfile) 
	      (not (getopt opts 'overwrite (config 'overwrite #f))))
	 (irritant rarefile |AlreadyExists| hashindex/repack!))
	((and rarefile (file-exists? rarefile) (not (file-writable? rarefile)))
	 (irritant rarefile |Unwritable| hashindex/repack!))
	(else))
  (dorepack method 
	    (or in (open-index infile #[readonly #t cachelevel 2]))
	    infile
	    (or outfile infile)
	    rarefile
	    opts))

(define (dorepack method in infile outfile rarefile opts)
  (let* ((outfile (or outfile infile))
	 (tmpfile (glom outfile ".part"))
	 (raretmp (and rarefile (glom rarefile ".part")))
	 (bakfile (glom infile ".bak")))
    (when (file-exists? tmpfile)
      (lognotice |RemoveTemp| "Removing left-over temporary file " tmpfile)
      (remove-file tmpfile))
    (when (and raretmp (file-exists? raretmp))
      (lognotice |RemoveTemp| "Removing left-over temporary file " raretmp)
      (remove-file raretmp))
    (let ((out (new-index tmpfile in opts))
	  (rare (and raretmp (new-index raretmp in opts))))
      (if rare
	  (method in out (cons `#[rare ,rare] opts))
	  (method in out opts))
      (cond ((equal? infile outfile)
	     (logwarn |Backup/Install| 
	       "Backing up " (write infile) " to " (write bakfile)
	       " and installing " (write tmpfile) " into " (write infile)
	       (when rare
		 (printout 
		   " and installing " (write raretmp) " into " (write rarefile))))
	     (move-file! infile bakfile)
	     (move-file! tmpfile infile)
	     (when raretmp (move-file! raretmp rarefile))
	     (close-index in))
	    (else (logwarn |Installing| outfile " from " tmpfile
			   (when rare
			     (printout 
			       " and " (write raretmp) " into " (write rarefile))))
		  (move-file! tmpfile outfile)
		  (when raretmp (move-file! raretmp rarefile)))))))

(define method-names
  `#[keys ,hashindex/repack-keys
     buckets ,hashindex/repack-buckets
     mapkeys ,hashindex/repack-mapkeys])
