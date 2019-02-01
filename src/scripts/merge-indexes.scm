;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
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

(define (->slotid arg)
  (if (not (string? arg))
      arg 
      (if (has-prefix arg {"@" ":@"})
	  (parse-arg arg)
	  (string->symbol (upcase arg)))))

(define (temp-index file)
  (open-index file #[register #f cachelevel 1]))

(define (make-opts in)
  (let ((sum 0) (keyslots {}))
    (dolist (in in)
      (let* ((index (temp-index in))
	     (n-keys (indexctl index 'metadata 'keys))
	     (keyslot (indexctl index 'keyslot)))
	(when n-keys (set! sum (+ sum n-keys)))
	(set+! keyslots keyslot)))
    `#[keyslot ,(try (singleton keyslots) #f) 
       size ,sum]))

(define (main out . in)
  (let* ((combined (make-opts in))
	 (newsize (config 'NEWSIZE (getopt combined 'size)))
	 (keyslot (config 'KEYSLOT (getopt combined 'keyslot)))
	 (opts (frame-create #f
		 'newsize newsize
		 'keyslot (tryif keyslot (->slotid keyslot))
		 'mincount (or (config 'mincount) {})
		 'maxcount (or (config 'maxcount) {})
		 'rarefile (or (config 'rare) {})
		 'uniquefile (or (config 'unique) {})
		 'repair (config 'repair #f)
		 'overwrite #f))
	 (n (length in)))
    (when (and (config 'rebuild) (file-exists? out))
      (onerror
	  (move-file! out (glom out ".bak"))
	  (lambda (ex)
		 (logwarn |FileExists|
		   "Couldn't back up " out " to " (glom out ".bak"))
		 (exit)))
	   (logwarn |FileExists|
	     "Moved existing file " out " " "to " (glom out ".bak")))
    (doseq (indexfile in i)
      (config! 'appid (stringout "merge(" (basename indexfile) ")[" i "/" n))
      (index/merge! indexfile out opts))))

(when (config 'optimize #t)
  (optimize! '{storage/indexes storage/hashindexes
	       ezrecords fifo engine})
  (optimize!))
