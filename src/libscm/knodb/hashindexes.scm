;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
Copyright (C) 2020-2021 beingmeta, LLC

(in-module 'knodb/hashindexes)

(use-module '{kno/reflect logger logctl text/stringfmts kno/mttools fifo engine})

(module-export! '{hashindex/mapkeys
		  hashindex/counts
		  hashindex/histogram
		  hashindex/repack!
		  hashindex/copy-keys!
		  hashindex/repack-keys
		  hashindex/repack-buckets
		  hashindex/repack-mapkeys})

(define %loglevel %info%)

;;; Mapping over buckets in a hashindex

(define (get-spans n-buckets span-width)
  (let ((n-spans (1+ (quotient n-buckets span-width)))
	(spans {}))
    (dotimes (i n-spans)
      (let ((bottom (* i span-width))
	    (top    (* (1+ i) span-width)))
	(unless (> bottom n-buckets)
	  (set! top (min top n-buckets))
	  (set+! spans (cons bottom top)))))
    spans))

(define (hashindex/mapkeys mapfn index (opts #f))
  (let* ((n-buckets (indexctl index 'hash))
	 (n-keys (indexctl index 'metadata 'keys))
	 (span-width (getopt opts 'spanwidth (config 'SPANWIDTH 100000)))
	 (spans (get-spans n-buckets span-width))
	 (loop-init (getopt opts 'loop #[])))
    (let ((loopfn (lambda (span batch-state loop-state task-state)
		    (mapfn (indexctl index 'buckets (car span) (cdr span))
			   batch-state loop-state task-state)))
	  (opts (cons `#[loop ,loop-init] opts)))
      (store! loop-init 'index index)
      (engine/run loopfn spans opts))))

(define (update-histogram info batch-state loop-state task-state)
  (let ((histogram (get loop-state 'histogram)))
    (do-choices (keyinfo info)
      (hashtable-increment! histogram (get keyinfo 'key) (get keyinfo 'count)))))

(define (hashindex/histogram index (opts #f) (table (make-hashtable)))
  (hashindex/mapkeys update-histogram index `#[loop #[histogram ,table]])
  table)

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
    (store! batch-state 'rarekeys rare-count)))

(define (hashindex/counts index (counts) (indexes) (rare))
  (default! counts (make-hashtable (indexctl index 'metadata 'keys)))
  (default! indexes (make-hashtable (indexctl index 'metadata 'keys)))
  (default! rare (make-hashtable (indexctl index 'metadata 'keys)))
  (hashindex/mapkeys key-counter index
		    `#[batchsize 1
		       loop #[index ,index counts ,counts indexes ,indexes rare ,rare]
		       count-term "spans"
		       counters {keys rarekeys}
		       logfns ,engine/log
		       logfreq 45])
  `#[counts ,counts indexes ,indexes rare ,rare])

;;; Repacking an index using hashindex/mapkeys

(defambda (hashindex-copier buckets batch-state loop-state task-state)
  (let* ((minthresh (get loop-state 'minthresh))
	 (rarethresh (get loop-state 'rarethresh))
	 (maxthresh (get loop-state 'maxthresh))
	 (rare   (try (get loop-state 'rare) #f))
	 (input  (get loop-state 'input))
	 (output (get loop-state 'output))
	 (outhash (make-hashtable (* 3 (choice-size buckets))))
	 (rarehash (and rare (make-hashtable (* 3 (choice-size buckets)))))
	 (stopkeys (indexctl output 'metadata 'stopkeys))
	 (tofetch '())
	 (rarefetch '())
	 (key-count 0)
	 (drop-count 0)
	 (rare-count 0)
	 (value-count 0))
    (let ((buckets (if (pair? buckets)
		       (indexctl input 'buckets (car buckets) (cdr buckets))
		       (indexctl input 'buckets (qc buckets)))))
      (do-choices (keyinfo (if (exists? stopkeys)
			       (reject buckets 'key stopkeys)
			       buckets))
	(let ((key (get keyinfo 'key))
	      (nvals (get keyinfo 'count)))
	  (cond ((and minthresh (< nvals minthresh)))
		((test keyinfo 'value)
		 (add! (or rarehash outhash)
		     (get keyinfo 'key)
		   (get keyinfo 'value))
		 (set! key-count (1+ key-count))
		 (when minthresh (set! rare-count (1+ rare-count)))
		 (set! value-count (1+ value-count)))
		((and rarethresh (< nvals rarethresh))
		 (set! rare-count (1+ rare-count))
		 (when rarehash 
		   (set! rarefetch (cons (get keyinfo 'key) rarefetch))
		   (set! key-count (1+ key-count))
		   (set! value-count (+ value-count (get keyinfo 'count)))))
		((not (and maxthresh (> nvals maxthresh)))
		 (set! tofetch (cons (get keyinfo 'key) tofetch))
		 (set! key-count (1+ key-count))
		 (set! value-count (+ value-count (get keyinfo 'count))))))))
    (unless (null? tofetch)
      (let* ((keyvec (->vector tofetch))
	     (keyvals (index/fetchn input keyvec))
	     (len (length keyvec))
	     (i 0))
	(while (< i len)
	  (add! outhash (elt keyvec i) (elt keyvals i))
	  (set! i (1+ i)))))
    (when (and rarehash (not (null? rarefetch)))
      (let* ((keyvec (->vector rarefetch))
	     (keyvals (index/fetchn input keyvec))
	     (len (length keyvec))
	     (i 0))
	(while (< i len)
	  (add! rarehash (elt keyvec i) (elt keyvals i))
	  (set! i (1+ i)))))
    ;; (%watch output outhash)
    (index-merge! output outhash)
    ;; (%watch output (modified? output) (indexctl output 'metadata))
    (when rare (index-merge! rare rarehash))
    (table-increment! batch-state 'keys key-count)
    (table-increment! batch-state 'dropped drop-count)
    (table-increment! batch-state 'rarekeys rare-count)
    (table-increment! batch-state 'values value-count)))

(defambda (hashindex-simple-copier buckets batch-state loop-state task-state)
  (let* ((input  (get loop-state 'input))
	 (output (get loop-state 'output))
	 (outhash (make-hashtable (* 3 (choice-size buckets))))
	 (stopkeys (indexctl output 'metadata 'stopkeys))
	 (tofetch '())
	 (key-count 0)
	 (value-count 0))
    (let ((buckets (if (pair? buckets)
		       (indexctl input 'buckets (car buckets) (cdr buckets))
		       (indexctl input 'buckets (qc buckets)))))
      (do-choices (keyinfo (if (exists? stopkeys)
			       (reject buckets 'key stopkeys)
			       buckets))
	(if (test keyinfo 'value)
	    (add! outhash (get keyinfo 'key) (get keyinfo 'value))
	    (let ((key (get keyinfo 'key))
		  (nvals (get keyinfo 'count)))
	      (set! tofetch (cons (get keyinfo 'key) tofetch))
	      (set! key-count (1+ key-count))
	      (set! value-count (+ value-count (get keyinfo 'count)))))))
    (unless (null? tofetch)
      (let* ((keyvec (->vector tofetch))
	     (keyvals (index/fetchn input keyvec))
	     (len (length keyvec))
	     (i 0))
	(while (< i len)
	  (add! outhash (elt keyvec i) (elt keyvals i))
	  (set! i (1+ i)))))
    ;; (%watch output outhash)
    (index-merge! output outhash)
    ;; (%watch output (modified? output) (indexctl output 'metadata))
    (table-increment! batch-state 'keys key-count)
    (table-increment! batch-state 'values value-count)))

(define (hashindex/copy-keys! index output opts (rare))
  (let* ((started (elapsed-time))
	 (span (getopt opts 'span 1000))
	 (buckets (if span
		      (get-spans (indexctl index 'hash) span)
		      (indexctl index 'buckets)))
	 (rare (getopt opts 'rare {})))
    (lognotice |Copying|
      ($num (choice-size buckets)) 
      (if span " bucket spans" " buckets")
      " from " (write (index-source index))
      " to " (write (index-source output)))
    (engine/run (if (exists? rare) hashindex-copier hashindex-simple-copier)
	buckets  
      `#[loop #[input ,index output ,output
		rare ,(getopt opts 'rare)
		maxthresh ,(getopt opts 'maxthresh)
		rarethresh ,(and rare (getopt opts 'maxthresh))
		minthresh ,(getopt opts 'minthresh)]
	 logcontext ,(stringout "Copying " (index-source index))
	 count-term ,(if span "buckets" "bucket spans")
	 onerror {stopall signal}
	 counters {keys rarekeys values}
	 logrates {keys rarekeys values}
	 batchsize ,(if span 1 (getopt opts 'batchsize (config 'BATCHSIZE 100)))
	 batchrange ,(if span 1 (getopt opts 'batchrange (config 'BATCHRANGE 8)))
	 nthreads ,(getopt opts 'nthreads (config 'NTHREADS (rusage 'ncpus)))
	 checktests ,(engine/interval (getopt opts 'savefreq (config 'savefreq 60)))
	 checkpoint {,output ,(or rare {})}
	 logfreq ,(getopt opts 'logfreq (config 'LOGFREQ 30))
	 checkfreq ,(getopt opts 'checkfreq (config 'checkfreq 15))
	 logchecks #t
	 started ,started])))

