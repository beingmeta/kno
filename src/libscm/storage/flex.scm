;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/flex)

(use-module '{ezrecords stringfmts logger texttools})
(use-module '{storage/adjuncts storage/filenames})
(use-module '{storage/flexpools storage/flexindexes 
	      storage/adjuncts})

(module-export! '{pool/ref index/ref db/ref pool/copy})

(define-init %loglevel %notice%)

(define flexpool-suffix #("." (isxdigit+) ".pool" (eos)))
(define flexindex-suffix #("." (isxdigit+) ".index" (eos)))

(define tld `(isalnum+))

(define network-host
  `#((+ #((isalnum+) ".")) ,tld))

(define network-source 
  `{#((label host,network-host) ":" (label port (isdigit+)))
    #((label ttport (isalnum)) "@" (label host ,network-host))})

(define (get-source arg (opts #f) (rootdir))
  (default! rootdir (getopt opts 'rootdir))
  (cond ((not (string? arg)) arg)
	((textmatch network-source arg) arg)
	((not rootdir) (abspath arg))
	(else (mkpath rootdir arg))))

(define (w/adjuncts pool)
  (unless (or (adjunct? pool) (exists? (poolctl pool 'props 'adjuncts)))
    (adjuncts/init! pool))
  pool)

(define (resolve-dbref source opts)
  (cond ((not source) {})
	((or (position #\: source) (position #\@ source))
	 (cond ((or (testopt opts 'type 'index)
		    (testopt opts 'index))
		(if (testopt opts 'background)
		    (use-index source)
		    (open-index source)))
	       ((or (testopt opts 'type 'pool)
		    (testopt opts 'pool))
		(if (testopt opts 'adjunct)
		    (open-pool source opts)
		    (w/adjuncts (use-pool source opts))))
	       (else {})))
	((or (has-suffix source ".pool")
	     (testopt opts 'pool)
	     (testopt opts 'type 'pool))
	 (if (getopt opts 'adjunct)
	     (open-pool source opts)
	     (w/adjuncts (use-pool source opts))))
	((or (has-suffix source ".flexpool")
	     (testopt opts 'flexpool)
	     (testopt opts 'type 'flexpool))
	 (flexpool/ref source opts))
	((or (has-suffix source ".index")
	     (testopt opts 'index)
	     (testopt opts 'type 'index))
	 (flex/index source opts))
	((exists? (flex/file source "index"))
	 (flex/index source opts))
	(else {})))

(define (db/ref spec (opts #f))
  (cond ((pool? spec) (w/adjuncts spe))
	((or (index? spec) (hashtable? spec)) spec)
	(else (let* ((opts
		      (cond ((and opts (table? spec)) (cons spec opts))
			    ((table? spec) spec)
			    (else opts)))
		     (source
		      (get-source
		       (if (string? spec) spec
			   (try (get spec 'pool) 
				(get spec 'index)
				(get spec 'source)
				#f))
		       opts)))
		(resolve-dbref source opts)))))
(define flex/db db/ref)

;;; Pool ref

(define (pool/ref spec (opts #f))
  (when (and (table? spec) (not (pool? spec)) (not opts))
    (set! opts spec)
    (set! spec (getopt opts 'pool (getopt opts 'source #f))))
  (cond ((pool? spec)
	 (w/adjuncts spec))
	((not (string? spec)) (irritant spec |InvalidPoolSpec|))
	((or (position #\@ spec) (position #\: spec))
	 (w/adjuncts (if (getopt opts 'adjunct)
			 (open-pool spec opts)
			 (use-pool spec opts))))
	((and (file-exists? spec) (has-suffix spec ".pool"))
	 (w/adjuncts (if (getopt opts 'adjunct)
			 (open-pool spec opts)
			 (use-pool spec opts))))
	((and (file-exists? spec) (has-suffix spec ".flexpool")) 
	 (flexpool/open spec opts))
	((file-exists? (glom spec ".flexpool"))
	 (flexpool/ref (glom spec ".flexpool") opts))
	((file-exists? (glom spec ".pool"))
	 (w/adjuncts (open-pool (glom spec ".pool") opts)))
	((not (or (getopt opts 'create) (getopt opts 'make)))
	 #f)
	((has-suffix spec ".flexpool")
	 (flexpool/make spec opts))
	((has-suffix spec ".pool")
	 (w/adjuncts (make-pool spec opts)))
	((or (test opts 'flexpool) 
	     (test opts 'type 'flexpool)
	     (test opts '{flexpool step flexstep partsize partition}))
	 (flexpool/make spec opts))
	(else (w/adjuncts (make-pool spec opts)))))

;;; Index refs

(define (index/ref spec (opts #f))
  (if (index? spec) spec
      (begin
	(when (and (table? spec) (not opts))
	  (set! opts spec)
	  (set! spec (getopt opts 'index (getopt opts 'source #f))))
	(cond ((index? spec) spec)
	      ((not (string? spec)) (irritant spec |InvalidIndexSpec|))
	      (else
	       (let ((baseindex (ref-index spec opts)))
		 (let* ((source (index-source baseindex))
			(next (glom (textsubst source flexindex-suffix "")
				".001.index"))
			(indexes {})
			(count 1))
		   (while (file-exists? next)
		     (set+! indexes (ref-index next opts))
		     (flexpool/open next opts)
		     (set! count (1+ count))
		     (set! next (glom (textsubst source flexindex-suffix "")
				  "." (padnum count 3 16) ".index")))
		   (lognotice |FlexIndex| "Found " count " indexes based at " baseindex)
		   (indexctl baseindex 'props 'seealso indexes)
		   (indexctl indexes 'props 'base baseindex))
		 baseindex))))))

(define (ref-index path opts)
  (if (file-exists? path)
      (open-index path opts)
      (make-index path opts)))

;;; Copying OIDs between pools

(define (pool/copy from to (opts #f) (batchsize) (logcopy #f))
  (default! batchsize (getopt opts 'batchsize (config 'BATCHSIZE 0x10000)))
  (let* ((tocopy (pool-elts to))
	 (n (choice-size tocopy))
	 (n-batches (1+ (quotient n batchsize)))
	 (copy-start (elapsed-time)))
    (lognotice |PoolCopy/Start| 
      "Copying " ($num n) " OIDs in "
      ($num n-batches) " batches of up to "  ($num batchsize)
      " OIDs into " (write (pool-source to)))
    (dotimes (i n-batches)
      (let* ((batch-started (elapsed-time))
	     (oidvec (choice->vector (pick-n tocopy batchsize (* i batchsize))))
	     (valvec (pool/fetchn from oidvec)))
	(pool/storen! to oidvec valvec)
	(when logcopy 
	  (logcopy to (length oidvec) n (elapsed-time batch-started)))))
    (commit to)
    (swapout to)
    (poolctl to 'cachelevel 0)
    (lognotice |PoolCopy/Done| 
      "Copied " ($num n) " OIDs into " (write (pool-source to))
      " in " (secs->string (elapsed-time copy-start))
      " (" ($num (->exact (/~ n (elapsed-time copy-start)))) " OIDs/second)")))

