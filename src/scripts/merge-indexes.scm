;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(use-module '{optimize varconfig logger kno/mttools})
(use-module '{knodb/indexes})

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
	  (string->symbol (downcase arg)))))

(define (temp-index file)
  (open-index file #[register #f cachelevel 1]))

(define (make-opts outfile in)
  (let* ((out (and (file-exists? outfile) (temp-index outfile)))
	 (sum (if out (indexctl out 'metadata 'keys) 0)) 
	 (keyslots (if out (indexctl out 'keyslot) {})))
    (dolist (in in)
      (let* ((index (temp-index in))
	     (n-keys (indexctl index 'metadata 'keys))
	     (keyslot (indexctl index 'keyslot)))
	(when n-keys (set! sum (+ sum n-keys)))
	(set+! keyslots keyslot)))
    `#[keyslot ,(try (singleton keyslots) #f) 
       size ,sum]))

(define (main out . in)
  (let* ((combined (make-opts out in))
	 (newsize (config 'NEWSIZE (getopt combined 'size)))
	 (keyslot (config 'KEYSLOT (getopt combined 'keyslot)))
	 (archive (config 'archive))
	 (tailfile (or (config 'tailfile) {}))
	 (opts (frame-create #f
		 'newsize newsize
		 'keyslot (tryif keyslot (->slotid keyslot))
		 'mincount (or (config 'mincount (config 'tailcount))
			       (if tailfile 7 {}))
		 'maxcount (or (config 'maxcount) {})
		 'tailfile tailfile
		 'repair (config 'repair #f)
		 'overwrite #f))
	 (n (length in)))
    (when (and (config 'rebuild) (file-exists? out))
      (onerror
	  (move-file! out (glom out ".bak"))
	  (lambda (ex)
	    (logwarn |BackupFailed|
	      "Couldn't back up " out " to " (glom out ".bak"))
	    (reraise ex)))
      (logwarn |FileExists|
	"Moved existing file " out " " "to " (glom out ".bak")))
    (doseq (indexfile in i)
      (config! 'appid (stringout "merge(" (basename indexfile) ")[" i "/" n))
      (index/merge! indexfile out opts)
      (let ((archive-file 
	     (cond ((not archive) #f)
		   ((and (string? archive) (file-directory? archive))
		    (mkpath archive (basename indexfile)))
		   ((and (string? archive) (has-prefix archive "."))
		    (glom indexfile archive))
		   ((empty-string? archive))
		   ((string? archive) (glom indexfile "." archive))
		   (else (glom indexfile ".bak")))))
	(when archive-file
	  (onerror (move-file! indexfile archive-file)
	      (lambda (ex)
		(logerr |ArchiveFailed|
		  "Couldn't archive " (write indexfile) " to " (write archive-file))
		(reraise ex)))
	  (logwarn |Archived|
	    "Archived " (write indexfile) " to " (write archive-file)))))))

(when (config 'optimize #t)
  (optimize! '{logger ezrecords fifo engine})
  (optimize! '{knodb/indexes knodb/hashindexes})
  (optimize!))
