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
  (let* ((opts (frame-create #f
		 'newsize (config 'newsize {})
		 'keyslot (string->symbol (upcase (config 'keyslot {})))
		 'mincount (config 'mincount {})
		 'maxcount (config 'maxcount {})
		 'rarefile (config 'rare {})
		 'uniquefile (config 'unique {})
		 'repair (config 'repair #f)
		 'overwrite overwrite)))
    (when (and (config 'rebuild) (file-exists? out))
      (onerror
	  (move-file! out (glom out ".bak"))
	  (lambda (ex)
		 (logwarn |FileExists|
		   "Couldn't back up " out " to " (glom out ".bak"))
		 (exit)))
	   (logwarn |FileExists|
	     "Moved existing file " out " " "to " (glom out ".bak")))
    (index/merge! (elts in) out opts)))

(when (config 'optimize #t)
  (optimize! '{storage/indexes storage/hashindexes ezrecords fifo engine})
  (optimize!))
