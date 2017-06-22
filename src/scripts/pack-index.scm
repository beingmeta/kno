;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module '{optimize mttools logger stringfmts fifo varconfig})

(define (getflags)
  (choice->vector
   (choice (tryif (config 'dtypev2 #t) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64)
	   )))

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
	(let ((table (make-hashtable)))
	  (message "Computing slotids based on " ($num (length keyvec)) " keys")
	  (doseq (key keyvec)
	    (when (pair? key) (hashtable-increment! table (car key))))
	  (message "Found " (choice-size (getkeys table))
		   " slotids across " ($num (length keyvec)) " keys")
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
  (message "Constructing new index " (write filename) " based on "
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

(defambda (process-batch batch batch-size buckets old new (start (elapsed-time)))
  (message "Processing a batch of " (choice-size batch) 
    " buckets covering " batch-size " keys")
  (do-choices (key (get buckets batch))
    (store! new key (get old key)))
  (message "Copied " (choice-size batch) " buckets (" batch-size " keys) "
    "in " (secs->string (elapsed-time start)) ", committing...")
  (commit new)
  (swapout new)
  (swapout old))

(define (copy-keys keyv old new (chunk-size) (start (elapsed-time)))
  (default! chunk-size (quotient (length keyv) (config 'nchunks 20)))
  (message "Copying " (length keyv) " keys" 
    " from " (or (index-source old) old)
    " into " (or (index-source new) new))
  (let ((buckets (make-hashtable (length keyv)))
	(total-keys 0)
	(batch-size 0)
	(batch {}))
    (message "Generating a bucket map for " (or (index-source new) new))
    (doseq (key keyv)
      (add! buckets (indexctl new 'hash key) key))
    (do-choices (bucket (getkeys buckets) i)
      (set+! batch bucket)
      (set! batch-size (+ batch-size (choice-size (get buckets bucket))))
      (when (> batch-size chunk-size)
	(process-batch batch batch-size buckets old new)
	(set! total-keys (+ total-keys batch-size))
	(set! batch {})
	(set! batch-size 0)
	(message "In total, " (show% total-keys (length keyv)) 
	  " of the keys (" total-keys ") have been processed in "
	  (secs->string (elapsed-time start)))
	(message "At this rate, the copy should finish in about "
	  (secs->string (- (/  (length keyv) (/ total-keys (elapsed-time start)))
			   (elapsed-time start)))
	  ", but you know how these things are.")))
    (process-batch batch batch-size buckets old new)))

(define (main from (to #f))
  (optimize! copy-keys) (optimize! compute-slotids)
  (cond ((not to) (repack-index from))
	((and (file-exists? to) (not (config 'OVERWRITE #f)))
	 (message "Not overwriting " to))
	((not (file-exists? from))
	 (message "Can't locate source " from))
	(else
	 (when (file-exists? to) (remove-file to))
	 (let* ((old (open-index from #[cachelevel 3]))
		(keyv (index-keysvec old))
		(new (make-new-index to old keyv)))
	   (copy-keys keyv old new)))))

(define (repack-index from)
  (message "Repacking the index " from)
  (let* ((base (basename from))
	 (tmpfile (or (config 'TMPFILE #f)
		      (and (config 'TMPDIR #f)
			   (mkpath (config 'TMPDIR)
				   (string-append base ".tmp")))
		      (string-append base ".tmp")))
	 (bakfile (or (config 'BAKFILE #f)
		      (string-append base ".bak"))))
    (let* ((old (open-index from))
	   (keyv (index-keysvec old))
	   (new (make-new-index tmpfile old keyv)))
      (copy-keys keyv old new))
    (onerror (move-file from bakfile)
	     (lambda (ex) (system "mv " from " " bakfile)))
    (onerror (move-file tmpfile from)
	     (lambda (ex) (system "mv " tmpfile " " from)))))

(optimize!)
