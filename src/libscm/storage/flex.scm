;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/flex)

(use-module '{ezrecords stringfmts logger texttools reflection})
(use-module '{storage/adjuncts storage/registry storage/filenames})
(use-module '{storage/flexpool storage/flexindexes 
	      storage/adjuncts})

(module-export! '{pool/ref index/ref db/ref pool/copy flex/wrap flex/partitions flex/save!})

(module-export! '{flex/container flex/container!})

(define-init %loglevel %notice%)

(define flexpool-suffix #("." (isxdigit+) ".pool" (eos)))
(define flexindex-suffix #("." (isxdigit+) ".index" (eos)))

(define tld `(isalnum+))

(define network-host
  `#((+ #((isalnum+) ".")) ,tld))

(define (make-opts opts)
  (if (table? (getopt opts 'make))
      (cons (getopt opts 'make) opts)
      opts))

(define network-source 
  `{#((label host,network-host) ":" (label port (isdigit+)))
    #((label ttport (isalnum)) "@" (label host ,network-host))})

(define (get-source arg (opts #f) (rootdir))
  (default! rootdir (getopt opts 'rootdir))
  (cond ((not (string? arg)) arg)
	((textmatch network-source arg) arg)
	((not rootdir) (abspath arg))
	(else (mkpath rootdir arg))))

(define (flex/partitions arg)
  (cond ((flexpool? arg) (flexpool/partitions arg))
	((pool? arg) (flexpool/partitions arg))
	(else (fail))))

(define (flex/wrap pool)
  (unless (or (adjunct? pool) (exists? (poolctl pool 'props 'adjuncts)))
    (adjuncts/init! pool))
  pool)

(define (resolve-dbref source opts)
  (cond ((not source) {})
	((or (position #\: source) (position #\@ source))
	 (cond ((or (testopt opts 'type 'index)
		    (testopt opts 'index))
		(if (testopt opts 'background)
		    (use-index source opts)
		    (open-index source opts)))
	       ((or (testopt opts 'type 'pool)
		    (testopt opts 'pool))
		(if (testopt opts 'adjunct)
		    (open-pool source opts)
		    (flex/wrap (use-pool source opts))))
	       (else {})))
	((or (has-suffix source ".pool")
	     (testopt opts 'pool)
	     (testopt opts 'type 'pool))
	 (if (getopt opts 'adjunct)
	     (open-pool source opts)
	     (flex/wrap (use-pool source opts))))
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
  (when (and (table? spec) (not (or (pool? spec) (index? spec))))
    (if opts (set! opts (cons opts spec)) (set! opts spec))
    (set! spec (getopt opts 'index (getopt opts 'pool (getopt opts 'source #f)))))
  (cond ((pool? spec) (flex/wrap spec))
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
  (when (and (table? spec) (not (or (pool? spec) (index? spec))))
    (if opts (set! opts (cons opts spec)) (set! opts spec))
    (set! spec (getopt opts 'pool (getopt opts 'source #f))))
  (cond ((pool? spec)
	 (flex/wrap spec))
	((not (string? spec)) (irritant spec |InvalidPoolSpec|))
	((or (position #\@ spec) (position #\: spec))
	 (flex/wrap (if (getopt opts 'adjunct)
			 (open-pool spec opts)
			 (use-pool spec opts))))
	((and (file-exists? spec) (has-suffix spec ".pool"))
	 (flex/wrap (if (getopt opts 'adjunct)
			 (open-pool spec opts)
			 (use-pool spec opts))))
	((and (file-exists? spec) (has-suffix spec ".flexpool")) 
	 (flexpool/open spec opts))
	((file-exists? (glom spec ".flexpool"))
	 (flexpool/ref (glom spec ".flexpool") opts))
	((file-exists? (glom spec ".pool"))
	 (flex/wrap (open-pool (glom spec ".pool") opts)))
	((not (or (getopt opts 'create) (getopt opts 'make)))
	 #f)
	((has-suffix spec ".flexpool")
	 (flexpool/make spec opts))
	((has-suffix spec ".pool")
	 (flex/wrap (make-pool spec (make-opts opts))))
	((or (test opts 'flexpool) 
	     (test opts 'type 'flexpool)
	     (test opts '{flexpool step flexstep partsize partition}))
	 (flexpool/make spec opts))
	(else (flex/wrap (make-pool spec (make-opts opts))))))

;;; Index refs

(define (index/ref spec (opts #f))
  (if (index? spec) spec
      (begin
	(when (and (table? spec) (not (or (pool? spec) (index? spec))))
	  (if opts (set! opts (cons opts spec)) (set! opts spec))
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
		     (set! count (1+ count))
		     (set! next (glom (textsubst source flexindex-suffix "")
				  "." (padnum count 3 16) ".index")))
		   (if (> count 1)
		       (lognotice |FlexIndex| "Found " count " indexes based at " baseindex)
		       (loginfo |FlexIndex| "Found one index based at " baseindex))
		   (indexctl baseindex 'props 'seealso indexes)
		   (indexctl indexes 'props 'base baseindex))
		 baseindex))))))

(define (ref-index path opts)
  (if (file-exists? path)
      (open-index path opts)
      (begin (lognotice |NewIndex| "Creating new index file " path)
	(make-index path (make-opts opts)))))

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

;;; Generic DB saving

(define (flex/mods arg)
  (cond ((registry? arg) (pick arg registry/modified?))
	((flexpool/record arg) (pick (flexpool/partitions arg) modified?))
	((exists? (db->registry arg)) (pick (db->registry arg) registry/modified?))
	((or (pool? arg) (index? arg)) (pick arg modified?))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) arg)
	((and (pair? arg) (applicable? (car arg))) arg)
	(else (logwarn |CantSave| "No method for saving " arg) {})))


(define (flex/commit arg)
  (onerror (cond ((registry? arg) (save-registry arg))
		 ((flexpool/record arg)
		  (thread/wait 
		   (thread/call safe-commit (pick (flexpool/partitions arg) modified?))))
		 ((or (pool? arg) (index? arg)) (commit arg))
		 ((and (applicable? arg) (zero? (procedure-min-arity arg))) (arg))
		 ((and (pair? arg) (applicable? (car arg)))
		  (apply (car arg) (cdr arg)))
		 (else (logwarn |CantSave| "No method for saving " arg)))
      (lambda (ex)
	(logwarn |CommitError| "Error committing " arg ": " ex)
	#f)))

(defambda (flex/save! . args)
  (dolist (arg args)
    (let ((dbs (flex/mods arg))
	  (started (elapsed-time)))
      (cond ((exists? dbs)
	     (lognotice |Saving|
	       "Saving " (choice-size dbs) " data stores:"
	       (do-choices (db dbs) (printout "\n    " db)))
	     (let ((threads (thread/call flex/commit dbs)))
	       (thread/wait! threads)
	       (lognotice |Saved|
		 "Saved " (choice-size dbs) " data stores in "
		 (secs->string (elapsed-time started)))
	       (thread/result threads)))))))

(define (safe-commit arg)
  (when (modified? arg)
    (onerror (commit arg)
	(lambda (ex)
	  (logwarn |CommitError| "Error committing " arg ": " ex)
	  #f))))
(define (save-registry arg)
  (onerror (registry/save! arg)
      (lambda (ex)
	(logwarn |CommitError| "Error saving registry " arg ": " ex)
	#f)))


