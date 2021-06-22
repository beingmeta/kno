;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/hashindexes)

(use-module '{reflection logger logctl text/stringfmts kno/mttools fifo engine})

(module-export! '{hashindex/mapkeys
		  hashindex/counts
		  hashindex/histogram
		  hashindex/repack!
		  hashindex/copy-keys!
		  hashindex/repack-keys
		  hashindex/repack-buckets
		  hashindex/repack-mapkeys
		  hashindex/keyset
		  hashindex/slotkeyset})

(define %loglevel %info%)

(define %optmods '{kno/mttools fifo engine})

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

(define (update-keyset info batch-state loop-state task-state)
  (let ((hashset (get loop-state 'keyset)))
    (do-choices (keyinfo info)
      (hashset-add! hashset (get keyinfo 'key)))))

(define (hashindex/keyset index (opts #f) (hashset (make-hashset)))
  (hashindex/mapkeys update-keyset index `#[loop #[keyset ,hashset]])
  hashset)

(define (update-slotkeyset info batch-state loop-state task-state)
  (let ((hashset (get loop-state 'keyset))
	(slotids (get loop-state 'slotids))
	(key #f))
    (do-choices (keyinfo info)
      (set! key (get keyinfo 'key))
      (when (and (pair? key) (overlaps? (car key) slotids))
	(hashset-add! hashset key)))))

(defambda (hashindex/slotkeyset index slotids (hashset (make-hashset)))
  (hashindex/mapkeys update-slotkeyset index `#[loop #[keyset ,hashset slotids ,slotids]])
  hashset)

;;; Getting index-sizes by iterating over buckets

(defambda (key-counter keys batch-state loop-state task-state)
  (let ((counts (get loop-state 'counts))
	(indexes (get loop-state 'indexes))
	(tail (try (get loop-state 'tail) #f))
	(batch-counts (make-hashtable))
	(record-keys {})
	(tail-count 0))
    (do-choices (keyinfo keys)
      (add! batch-counts (get keyinfo 'count) (get keyinfo 'key))
      (cond ((not tail)
	     (set+! record-keys (get keyinfo 'key)))
	    ((test keyinfo 'value)
	     (add! tail (get keyinfo 'key) (get keyinfo 'value))
	     (set! tail-count (1+ tail-count)))
	    (else (set+! record-keys (get keyinfo 'key)))))
    (do-choices (count (getkeys batch-counts))
      (hashtable-increment! counts (get batch-counts count) count))
    (add! indexes record-keys (get loop-state 'index))
    (store! batch-state 'keys (choice-size keys))
    (store! batch-state 'tailkeys tail-count)))

(define (hashindex/counts index (counts) (indexes) (tail))
  (default! counts (make-hashtable (indexctl index 'metadata 'keys)))
  (default! indexes (make-hashtable (indexctl index 'metadata 'keys)))
  (default! tail (make-hashtable (indexctl index 'metadata 'keys)))
  (hashindex/mapkeys key-counter index
		    `#[batchsize 1
		       loop #[index ,index counts ,counts indexes ,indexes tail ,tail]
		       count-term "span"
		       counters {keys tailkeys}
		       logfns ,engine/log
		       logfreq 45])
  `#[counts ,counts indexes ,indexes tail ,tail])

;;; Repacking an index using hashindex/mapkeys

(defambda (hashindex-copier buckets batch-state loop-state task-state)
  "This processes all of the source keys based on their number of values, skipping keys "
  "with either less then *mincount* or more than *maxcount* values and saving keys with fewer "
  "than *tailcount* values into a separate output table."
  (let* ((mincount (get loop-state 'mincount))
	 (tailcount (get loop-state 'tailcount))
	 (maxcount (get loop-state 'maxcount))
	 (tail   (try (get loop-state 'tail) #f))
	 (input  (get loop-state 'input))
	 (output (get loop-state 'output))
	 (headout (make-hashtable (* 3 (choice-size buckets))))
	 (tailout (and tail (make-hashtable (* 3 (choice-size buckets)))))
	 (stopkeys (indexctl output 'metadata 'stopkeys))
	 (tofetch '())
	 (tailfetch '())
	 (key-count 0)
	 (drop-count 0)
	 (tail-count 0)
	 (over-count 0)
	 (value-count 0))
    (let ((buckets (if (pair? buckets)
		       (indexctl input 'buckets (car buckets) (cdr buckets))
		       (indexctl input 'buckets (qc buckets)))))
      (do-choices (keyinfo (if (exists? stopkeys)
			       (reject buckets 'key stopkeys)
			       buckets))
	(let ((key (get keyinfo 'key))
	      (nvals (get keyinfo 'count)))
	  (cond ((and mincount (< nvals mincount))
		 (set! drop-count (1+ drop-count)))
		((test keyinfo 'value)
		 (add! (or tailout headout) (get keyinfo 'key) (get keyinfo 'value))
		 (when tail (set! tail-count (1+ tail-count)))
		 (set! key-count (1+ key-count))
		 (set! value-count (1+ value-count)))
		((and tailcount (< nvals tailcount))
		 (set! tail-count (1+ tail-count))
		 (when tailout
		   (set! tailfetch (cons (get keyinfo 'key) tailfetch))
		   (set! key-count (1+ key-count))
		   (set! value-count (+ value-count (get keyinfo 'count)))))
		((and maxcount (> nvals maxcount))
		 (set! drop-count (1+ drop-count))
		 (set! over-count (1+ over-count)))
		(else
		 (set! tofetch (cons (get keyinfo 'key) tofetch))
		 (set! key-count (1+ key-count))
		 (set! value-count (+ value-count (get keyinfo 'count))))))))
    (unless (null? tofetch)
      (let* ((keyvec (->vector tofetch))
	     (keyvals (index/fetchn input keyvec))
	     (len (length keyvec))
	     (i 0))
	(while (< i len)
	  (add! headout (elt keyvec i) (elt keyvals i))
	  (set! i (1+ i)))))
    (when (and tailout (not (null? tailfetch)))
      (let* ((keyvec (->vector tailfetch))
	     (keyvals (index/fetchn input keyvec))
	     (len (length keyvec))
	     (i 0))
	(while (< i len)
	  (add! tailout (elt keyvec i) (elt keyvals i))
	  (set! i (1+ i)))))
    ;; (%watch output outhash)
    (index-merge! output headout)
    ;; (%watch output (modified? output) (indexctl output 'metadata))
    (when tail (index-merge! tail tailout))
    (table-increment! batch-state 'keys key-count)
    (table-increment! batch-state 'drops drop-count)
    (table-increment! batch-state 'tops over-count)
    (table-increment! batch-state 'tails tail-count)
    (table-increment! batch-state 'values value-count)))

(defambda (hashindex-simple-copier buckets batch-state loop-state task-state)
  "This simply copies keys without any valuecount logic"
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

(define (hashindex/copy-keys! index output opts)
  (let* ((started (elapsed-time))
	 (span (getopt opts 'span 1000))
	 (buckets (if span
		      (get-spans (indexctl index 'hash) span)
		      (indexctl index 'buckets)))
	 (tail (getopt opts 'tail {}))
	 (using-counts (or (exists? tail)
			   (testopt opts '{maxcount mincount tailcount})))
	 (counters
	  (vector 'keys 'values
		  (and (exists? tail) 'tails)
		  (and (testopt opts '{mincount maxcount}) 'drops)
		  (and (testopt opts 'maxcount) 'tops))))
    (lognotice |Copying|
      ($num (choice-size buckets)) 
      (if span " bucket spans" " buckets")
      " from " (write (index-source index))
      " to " (write (index-source output)))
    (engine/run (if using-counts hashindex-copier hashindex-simple-copier)
	buckets  
      `#[loop #[input ,index output ,output
		tail ,(getopt opts 'tail)
		maxcount ,(getopt opts 'maxcount)
		tailcount ,(and tail (getopt opts 'maxcount))
		mincount ,(getopt opts 'mincount)]
	 logcontext ,(stringout "Copying " (index-source index))
	 count-term ,(if span "bucket span" "bucket")
	 onerror {stopall signal}
	 logcounters ,(remove #f counters)
	 counters ,(difference (elts counters) #f)
	 logrates ,(difference (elts counters) #f)
	 batchsize ,(if span 1 (getopt opts 'batchsize (config 'BATCHSIZE 100)))
	 batchrange ,(if span 1 (getopt opts 'batchrange (config 'BATCHRANGE 8)))
	 nthreads ,(getopt opts 'nthreads (config 'NTHREADS (rusage 'ncpus)))
	 checktests ,(engine/interval (getopt opts 'savefreq (config 'savefreq 60)))
	 checkpoint {,output ,(or tail {})}
	 logfreq ,(getopt opts 'logfreq (config 'LOGFREQ 30))
	 checkfreq ,(getopt opts 'checkfreq (config 'checkfreq 15))
	 logchecks #t
	 started ,started])))

