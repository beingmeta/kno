;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(config! 'optlevel 4)
(use-module '{optimize mttools logger stringfmts fifo varconfig})

(define (getflags)
  (choice->vector
   (choice (tryif (config 'dtypev2 #t) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64)
	   )))

(config! 'logprocinfo #t)
(config! 'logthreadinfo #t)

(define %loglevel %notice%)

;;; Computing baseoids 

(define (get-baseoids-from-pool pool)
  (get-baseoids-from-base (pool-base pool) (pool-capacity pool)))

(define (get-baseoids-from-base base capacity)
  (choice base
	  (tryif (> capacity 0)
		 (get-baseoids-from-base (oid-plus base capacity)
					 (- capacity (* 1024 1024))))))

(define baseoids {})

(config-def! 'baseoid (slambda (var val)
			(if (bound? val)
			    (set+! baseoids val)
			    baseoids)))
(config-def! 'pool (slambda (var val)
		     (if (bound? val)
			 (set+! baseoids (get-baseoids-from-pool val))
			 (fail))))

;;; Computing slotids

(define slotids #f)

(config-def! 'slotid
	     (slambda (var val)
	       (if (bound? val)
		   (if slotids (set! slotids (append slotids (list val)))
		       (set! slotids (list val)))
		   slotids)))

(define (compute-slotids keyvec)
  (let ((slotids-config (config 'slotids #f)))
    (if (and (string? slotids-config) (file-exists? slotids-config))
	(append (if slotids (->vector slotids) #())
		(file->dtype slotids-config))
	(let ((table (make-hashtable))
	      (start (elapsed-time)))
	  (loginfo |PackIndex/slotids|
	    "Computing slotids based on " ($num (length keyvec)) " keys")
	  (doseq (key keyvec)
	    (when (pair? key) (hashtable-increment! table (car key))))
	  (lognotice |PackIndex/slotids|
	    "Found " ($num (choice-size (getkeys table)))
	    " slotids across " ($num (length keyvec)) " keys in "
	    (secs->string (elapsed-time start)))
	  (if slotids (drop! table (elts slotids)))
	  (append (if slotids (->vector slotids) #())
		  (rsorted (getkeys table) table))))))
  
;;; Other features

(define (symbolize s)
  (if (symbol? s) s
      (if (string? s) (string->symbol (upcase s))
	  (irritant s |NotStringOrSymbol|))))

(define (get-new-size base)
  (config 'newsize (inexact->exact (* (config 'newload 3) base))))

(define (get-metadata base)
  (and (config 'metadata #f) (file->dtype (config 'metadata #f))))

(define (get-metadata)
  (and (config 'metadata #f) (file->dtype (config 'metadata #f))))

(define (make-new-index filename old keys (type))
  (default! type (symbolize (config 'type 'hashindex)))
  (lognotice |NewIndex|
    "Constructing new index " (write filename) " based on "
    (index-source old))
  (if (eq? type 'stdindex)
      (make-index filename 
		  #[type fileindex
		    slots ,(max (* 2 (length keys))
				(get-new-size (length keys)))])
      (make-index filename 
		  `#[type hashindex
		     slots ,(get-new-size (length keys))
		     slotids ,(compute-slotids keys)
		     baseoids ,(sorted baseoids)
		     metadata ,(get-metadata)]))
  (open-index filename))

(define (copy-loop fifo buckets old new (bucket))
  (while (fifo-live? fifo)
    (set! bucket (fifo/pop fifo))
    (do-choices (key (get buckets bucket))
      (store! new key (get old key)))))

(defambda (copy-keys keys old new)
  (prefetch-keys! old keys)
  (do-choices (key keys)
    (store! new key (get old key))))

(defambda (process-batch batch batch-size buckets old new 
			 (start (elapsed-time)) (nthreads (mt/threadcount)))
  (comment
   (loginfo |PackIndex/batch/fetch|
     "Prefetching " ($num (choice-size batch))
     " buckets covering " ($num batch-size) " keys")
   (index-prefetch! old (get buckets batch))
   (lognotice |PackIndex/batch/fetch| 
     "Prefetched " ($num (choice-size batch)) 
     " buckets covering " ($num batch-size) " keys "
     "in " (secs->string (elapsed-time start)) ", "
     "copying with " nthreads " threads..."))
  (lognotice |PackIndex/Batch/copy|
    "Copying " (choice-size batch) " buckets (" batch-size " keys)")
  (do-choices-mt (bucket batch)
    (copy-keys (get buckets bucket) old new))
  (lognotice |PackIndex/Batch/copy|
      "Copied " (choice-size batch) " buckets (" batch-size " keys) "
      "in " (secs->string (elapsed-time start)) ", committing...")
  (commit new)
  (swapout new)
  (swapout old))

(define (copy-all-keys keyv old new (chunk-size) (start (elapsed-time)))
  (default! chunk-size (quotient (length keyv) (config 'nchunks 20)))
  (lognotice |PackIndex/copy|
    "Copying " ($num (length keyv)) " keys" 
    " from " (or (index-source old) old)
    " into " (or (index-source new) new))
  (let ((buckets (make-hashtable (length keyv)))
	(total-keys 0)
	(batch-size 0)
	(batch {})
	(bucket-start (elapsed-time)))
    (loginfo |PackIndex/buckets|
      "Generating a bucket map for " (or (index-source new) new))
    (doseq (key keyv)
      (add! buckets (indexctl new 'hash key) key))
    (lognotice |PackIndex/buckets|
      "Generated mapping from " ($num (table-size buckets)) " buckets to "
      ($num (length keyv)) " keys in " (secs->string (elapsed-time bucket-start)))
    (do-choices (bucket (getkeys buckets) i)
      (set+! batch bucket)
      (set! batch-size (+ batch-size (choice-size (get buckets bucket))))
      (when (> batch-size chunk-size)
	(process-batch batch batch-size buckets old new)
	(set! total-keys (+ total-keys batch-size))
	(set! batch {})
	(set! batch-size 0)
	(lognotice |PackIndex|
	  "In total, " (show% total-keys (length keyv)) 
	  " of the keys (" ($num total-keys) ") have been processed in "
	  (secs->string (elapsed-time start)))
	(lognotice |PackIndex|
	  "At this rate, the copy should finish in about "
	  (secs->string (- (/  (length keyv) (/ total-keys (elapsed-time start)))
			   (elapsed-time start)))
	  ", but you know how these things are.")))
    (lognotice |PackIndex/final| 
      "Processing final batch of " batch-size " keys")
    (process-batch batch batch-size buckets old new)))

(define (main from (to #f))
  (cond ((not to) (repack-index from))
	((not (file-exists? from))
	 (logcrit |PackIndex| "Can't locate source " from))
	((and (file-exists? to) (not (config 'OVERWRITE #f)))
	 (logcrit |PackIndex| "Not overwriting " to))
	(else
	 (when (file-exists? to) (remove-file to))
	 (lognotice |PackIndex| "Copying index " from " into " to)
	 (let* ((old (open-index from))
		(keyv (index-keysvec old))
		(new (make-new-index to old keyv)))
	   (copy-all-keys keyv old new)))))

(define (repack-index from)
  (let* ((base (basename from))
	 (tmpfile (or (config 'TMPFILE #f)
		      (and (config 'TMPDIR #f)
			   (mkpath (config 'TMPDIR)
				   (string-append base ".tmp")))
		      (string-append base ".tmp")))
	 (bakfile (or (config 'BAKFILE #f)
		      (string-append base ".bak"))))
    (lognotice |PackIndex| 
      "Repacking the index " from " using " tmpfile 
      " and saving original in " bakfile)
    (let* ((old (open-index from))
	   (keyv (index-keysvec old))
	   (new (make-new-index tmpfile old keyv)))
      (copy-all-keys keyv old new))
    (onerror (move-file from bakfile)
	     (lambda (ex) (system "mv " from " " bakfile)))
    (onerror (move-file tmpfile from)
	     (lambda (ex) (system "mv " tmpfile " " from)))))

(optimize!)
