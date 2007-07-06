;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module '{optimize mttools})

(define (getflags)
  (choice->list
   (choice (tryif (config 'dtypev2 #f) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64))))

(define default-schematize #f)

(define (get-schemas old)
  (and (or (and default-schematize (not (config 'NOSCHEMAS #f)))
	   (config 'SCHEMAS #f))
       (let ((table (make-hashtable)))
	 (message "Computing schemas from " (pool-load old) " OIDs in " old)
	 (do-choices-mt (f (pool-elts old) (config 'nthreads 4)
			   (lambda (oids done)
			     (unless done (file-pool-prefetch! old oids)))
			   (config 'blocksize (/ (pool-load old) 10)))
	   (hashtable-increment! table (sorted (getkeys f)))
	   (swapout f))
	 (message "Identified " (choice-size (getkeys table)) " schemas")
	 (rsorted (getkeys table) table))))
  
(define (make-new-pool filename old)
  (cond ((config 'OLDPOOL #f)
	 (make-file-pool filename (pool-base old)
			 (or (config 'NEWCAP #f) (pool-capacity old))
			 (pool-load old))
	 (label-pool! filename (or (config 'label #f) (pool-label old)))
	 (use-pool filename))
	((config 'OIDPOOL #f)
	 (make-oidpool filename (pool-base old)
		       (or (config 'NEWCAP #f) (pool-capacity old))
		       (pool-load old) (getflags) (get-schemas old) #f
		       (or (config 'LABEL #f)
			   (try (pool-label old) #f)))
	 (use-pool filename))
	(else 
	 (make-file-pool filename (pool-base old)
			 (or (config 'NEWCAP #f) (pool-capacity old))
			 (pool-load old))
	 (use-pool filename))))

(define (copy-oids old new)
  (message "Copying OIDs from " old " into " new)
  (let* ((prefetcher (lambda (oids done)
		       (when done (commit) (clearcaches))
		       (unless done
			 (file-pool-prefetch! old oids)
			 (lock-oids! oids)
			 (prefetch-oids! oids)))))
    (do-choices-mt (f (pool-elts old) (config 'nthreads 4)
		      prefetcher (config 'blocksize 50000))
      (set-oid-value! f (get old f)))))

(define (main from (to #f))
  (cond ((not to) (repack-pool from))
	((file-exists? to)
	 (message "Not overwriting " to))
	((not (file-exists? from))
	 (message "Can't locate source " from))
	(else
	 (let* ((old (open-file-pool from))
		(new (make-new-pool to old)))
	   (copy-oids old new)))))

(define (repack-pool from)
  (let* ((base (basename from))
	 (tmpfile (or (config 'TMPFILE #f)
		      (and (config 'TMPDIR #f)
			   (mkpath (config 'TMPDIR)
				   (string-append base ".tmp")))
		      (string-append base ".tmp")))
	 (bakfile (or (config 'BAKFILE #f)
		      (string-append base ".tmp"))))
    (let* ((old (open-file-pool from))
	   (new (make-new-pool tmpfile old)))
      (copy-oids old new))
    (move-file from bakfile)
    (move-file tmpfile from)))

(optimize! copy-oids)

