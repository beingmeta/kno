;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module '{optimizekno/mttools logger varconfig})

(define (getflags)
  (choice->list
   (choice (tryif (config 'dtypev2 #f) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64))))

(define (copy-to-oidpool old new)
  (message "Copying OIDs in " old " into " new)
  (let* ((old (open-file-pool old))
	 (new (begin (make-oidpool new (pool-base old) (pool-capacity old)
				   (pool-load old) (getflags) #f #f
				   (try (pool-label old)
					(config 'LABEL #f)))
		     (use-pool new)))
	 (prefetcher (lambda (oids done)
		       (when done (commit) (clearcaches))
		       (unless done
			 (file-pool-prefetch! old oids)
			 (lock-oids! oids)
			 (prefetch-oids! oids)))))
    (do-choices-mt (f (pool-elts old) (config 'nthreads 4)
		      prefetcher (config 'blocksize 50000))
      (set-oid-value! f (get old f)))))

(define (main file (arg1 #f) . args)
  (cond ((not arg1)
	 (let* ((source file)
		(base (basename file))
		(dest (config 'TMPFILE
			      (if (config 'TMPDIR #f)
				  (mkpath (config 'TMPDIR #f)
					  (string-append base ".tmp"))
				  (string-append base ".tmp")))))
	   (copy-to-oidpool source dest)))
	((string? arg1)
	 (if (not (file-exists? arg1))
	     (copy-to-oidpool file arg1)
	     (copy-to-oidpool arg1 file)))
	((oid? arg1)
	 (if (null? (cdr args))
	     (error "Not enough args, need a capacity")
	     (make-oidpool file arg1 (car args)
			   (config! 'load 0)
			   (getflags) #f #f
			   (config 'label #f))))
	(else (lineout "Usage: make-oidpool toconvert")
	      (lineout "Usage: make-oidpool dest source")
	      (lineout "Usage: make-oidpool dest oid capacity"))))

(optimize! main)

