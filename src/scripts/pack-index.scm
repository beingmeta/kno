;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module '{optimize mttools})

(define (getflags)
  (choice->vector
   (choice (tryif (config 'dtypev2 #t) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64)
	   ;; (tryif (config 'ZLIB #f) 'ZLIB)
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
	  (message "Computing slotids based on " (length keyvec) " keys")
	  (doseq (key keyvec)
	    (when (pair? key) (hashtable-increment! table (car key))))
	  (message "Found " (choice-size (getkeys table))
		   " slotids across " (length keyvec) " keys")
	  (if slotids (drop! table (elts slotids)))
	  (append (if slotids (->vector slotids) #())
		  (rsorted (getkeys table) table))))))
  
;;; Other features

(define (get-new-size base)
  (config 'newsize (inexact->exact (* (config 'newload 3) base))))

(define (get-metadata base)
  (and (config 'metadata #f) (file->dtype (config 'metadata #f))))

(define (get-metadata)
  (and (config 'metadata #f) (file->dtype (config 'metadata #f))))

(define (make-new-index filename old keys)
  (message "Constructing new index " (write filename) " based on "
	   (index-source old))
  (cond ((config 'STDINDEX #f)
	 (make-file-index filename (max (* 2 (length keys))
					(get-new-size (length keys))))
	 (open-index filename))
	((config 'HASHINDEX #f)
	 (make-hash-index filename (get-new-size (length keys))
			  (compute-slotids keys) (sorted baseoids)
			  (get-metadata))
	 (open-index filename))
	(else
	 (make-hash-index filename (get-new-size (length keys))
			  (compute-slotids keys) (sorted baseoids)
			  (get-metadata))
	 (open-index filename))))

(define (copy-keys keyv old new)
  (message "Copying " (length keyv) " keys" 
	   " from " (or (index-source old) old)
	   " into " (or (index-source new) new))
  (let* ((prefetcher (lambda (keys done)
		       (when done (commit) (clearcaches))
		       (unless done
			 (prefetch-keys! old keys)))))
    (do-vector-mt (key keyv (config 'nthreads 4)
		       prefetcher (config 'blocksize 50000)
		       (mt/custom-progress "Copying keys"))
		  (add! new key (get old key)))))

(define (main from (to #f))
  (optimize! copy-keys) (optimize! compute-slotids)
  (cond ((not to) (repack-index from))
	((and (file-exists? to) (not (config 'OVERWRITE #f)))
	 (message "Not overwriting " to))
	((not (file-exists? from))
	 (message "Can't locate source " from))
	(else
	 (when (file-exists? to) (remove-file to))
	 (let* ((old (open-index from))
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
		      (string-append base ".tmp"))))
    (let* ((old (open-index from))
	   (keyv (index-keysvec old))
	   (new (make-new-index tmpfile old keyv)))
      (copy-keys keyv old new))
    (move-file from bakfile)
    (move-file tmpfile from)))





