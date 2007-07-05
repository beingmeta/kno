;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module '{optimize mttools})

(define (getflags)
  (choice->list
   (choice (tryif (config 'dtypev2 #f) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64))))

(define (main file from . pools)
  (let* ((old (open-file-pool from))
	 (new (begin (make-oidpool file (pool-base old) (pool-capacity old)
				   (pool-load old) (getflags) #f #f
				   (pool-label old))
		     (use-pool file)))
	 (prefetcher (lambda (oids done)
		       (when done (commit) (clearcaches))
		       (unless done
			 (file-pool-prefetch! old oids)
			 (lock-oids! oids)
			 (prefetch-oids! oids)))))
    (do-choices-mt (f (pool-elts old) (config 'nthreads 4) prefetcher (config 'blocksize 50000))
      (set-oid-value! f (get old f)))))

(optimize! main)

