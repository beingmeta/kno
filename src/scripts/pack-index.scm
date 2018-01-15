;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(use-module '{optimize varconfig logger mttools})
(use-module '{storage/indexes})

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

(define (main in (out #f))
  (let* ((overwrite (config 'overwrite #f))
	 (opts (frame-create #f
		 'newsize (config 'newsize {})
		 'keyslot (string->symbol (upcase (config 'keyslot {})))
		 'mincount (config 'mincount {})
		 'maxcount (config 'maxcount {})
		 'rarefile (config 'rare {})
		 'uniquefile (config 'unique {})
		 'repair (config 'repair #f)
		 'overwrite overwrite)))
    (when (and out (file-exists? out) (not overwrite))
      (logwarn |FileExists|
	"The output file " (write out) " already exists.\n  "
	"Specify OVERWRITE=yes to remove.")
      (exit))
    (when (and overwrite (getopt opts 'unique)
	       (file-exists? (getopt opts 'unique)))
      (overwriting (getopt opts 'unique)))
    (when (and overwrite (getopt opts 'rare)
	       (file-exists? (getopt opts 'rare)))
      (overwriting (getopt opts 'rare)))
    (index/pack! in out opts)))

(when (config 'optimize #t)
  (optimize! '{storage/indexes storage/hashindexes ezrecords fifo engine})
  (optimize!))


