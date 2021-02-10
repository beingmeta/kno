;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(use-module '{optimize varconfig logger kno/mttools})
(use-module '{knodb knodb/indexes knodb/flexindex})

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

(define (main infile outfile (rarefile #f))
  (config! 'appid (stringout "merge(" (basename infile ".index") ")"))
  (let* ((in (knodb/ref infile #[register #t]))
	 (out (knodb/ref outfile `#[register #t maxload ,(config 'maxload 2.0)
				    keyslot ,(dbctl in 'keyslot)
				    addkeys ,(dbctl in 'metadata 'keys)
				    minsize ,(config 'minsize #mib)
				    create #t]))
	 (rare (and rarefile
		    (flex/open-index rarefile
				     `#[register #t maxload ,(config 'maxload 1.5)
					partsize
					,(->exact (* (config 'maxload 1.5)
						     (max (dbctl in 'metadata 'keys)
							  (dbctl in 'metadata 'buckets)
							  #4mib)))
					keyslot ,(dbctl in 'keyslot)
					addkeys ,(dbctl in 'metadata 'keys)
					minsize ,(config 'minsize #mib)
					create #t])))
	 (opts (frame-create #f
		 'minthresh (config 'minthresh #f)
		 'maxthresh (config 'maxthresh #f)
		 'rarethresh (config 'rarethresh #f))))
    (merge-index in out opts rare)))

(when (config 'optimize #t)
  (optimize! '{logger ezrecords fifo engine})
  (optimize! '{knodb knodb/indexes knodb/hashindexes knodb/flexindex})
  (optimize!))
