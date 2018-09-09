;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(use-module '{optimize varconfig logger mttools})
(use-module '{storage/indexes})

(onerror (use-module 'rocksdb))
(onerror (use-module 'leveldb))

(define (overwriting file)
  (when (file-exists? file)
    (cond ((config 'unsafe)
	   (remove-file! file)
	   (logwarn |FileExists| "Removed existing file " (write file)))
	  (else
	   (onerror
	       (move-file! file (glom file ".bak"))
	       (lambda (ex)
		 (logwarn |FileExists|
		   "Couldn't back up " file " to " (glom file ".bak"))
		 (exit)))
	   (logwarn |FileExists|
	     "Moved existing file " file " " "to " (glom file ".bak"))))))

(define (main out . in)
  (let* ((index (open-index (and (pair? in) (car in))))
	 (first-size (indexctl index 'metadata 'keys))
	 (keyslot (symbolize (config 'KEYSLOT (indexctl index 'keyslot))))
	 (newsize (config 'NEWSIZE (* 4 first-size)))
	 (opts (frame-create #f
		 'newsize newsize
		 'keyslot keyslot
		 'mincount (config 'mincount {})
		 'maxcount (config 'maxcount {})
		 'rarefile (config 'rare {})
		 'uniquefile (config 'unique {})
		 'repair (config 'repair #f)
		 'overwrite #f)))
    (when (and (config 'rebuild) (file-exists? out))
      (onerror
	  (move-file! out (glom out ".bak"))
	  (lambda (ex)
		 (logwarn |FileExists|
		   "Couldn't back up " out " to " (glom out ".bak"))
		 (exit)))
	   (logwarn |FileExists|
	     "Moved existing file " out " " "to " (glom out ".bak")))
    (doseq (indexfile in)
      (index/merge! indexfile out opts))))

(when (config 'optimize #t)
  (optimize! '{storage/indexes storage/hashindexes ezrecords fifo engine})
  (optimize!))
