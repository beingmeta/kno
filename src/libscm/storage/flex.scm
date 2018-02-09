;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'storage/flex)

(use-module '{ezrecords stringfmts logger texttools fifo mttools reflection})
(use-module '{storage/adjuncts storage/registry storage/filenames})
(use-module '{storage/flexpool storage/flexindex storage/adjuncts})

(module-export! '{pool/ref index/ref pool/copy 
		  flex/dbref db/ref flex/db flex/ref
		  flex/make
		  flex/wrap flex/partitions
		  flex/save! flex/commit!})

(module-export! '{flex/container flex/container!})

(define-init %loglevel %notice%)

(define flexpool-suffix #("." (isxdigit+) ".pool" (eos)))
(define flexindex-suffix #("." (isxdigit+) ".index" (eos)))

(define tld `(isalnum+))

(define network-host
  `#((+ #((isalnum+) ".")) ,tld))

(define (make-opts opts)
  (let ((add (or (deep-copy (getopt opts 'make)) `#[])))
    (when (getopt opts 'w/adjuncts)
      (store! opts 'adjuncts (getopt opts 'w/adjuncts)))
    (cons add opts)))

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
  (cond ((pool? arg) (poolctl arg 'partitions))
	((index? arg) (or (indexctl arg 'partitions) {}))
	((string? arg) (flex/partition-files arg))
	(else (fail))))

(define (flex/wrap pool (opts #f))
  (unless (or (adjunct? pool) (exists? (poolctl pool 'props 'adjuncts)))
    (if (getopt opts 'make)
	(adjuncts/setup! pool)
	(adjuncts/init! pool)))
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
		    (flex/wrap (use-pool source opts) opts)))
	       (else {})))
	((or (has-suffix source ".pool")
	     (testopt opts 'pool)
	     (testopt opts 'type 'pool))
	 (if (file-exists? source)
	     (if (getopt opts 'adjunct)
		 (open-pool source opts)
		 (flex/wrap (use-pool source opts) opts))
	     (if (not (getopt opts 'create))
		 (irritant source |NoSuchPool| flex/dbref)
		 (make-pool source opts))))
	((or (has-suffix source ".index")
	     (testopt opts 'index)
	     (testopt opts 'indextype))
	 (if (file-exists? source)
	     (if (getopt opts 'adjunct)
		 (open-index source opts)
		 (use-index source opts))
	     (if (not (getopt opts 'create))
		 (irritant source |NoSuchIndex| flex/dbref)
		 (make-index source opts))))
	((or (has-suffix source ".flexpool")
	     (testopt opts 'flexpool)
	     (testopt opts 'type 'flexpool))
	 (flexpool/ref source opts))
	((or (has-suffix source ".flexindex")
	     (getopt opts 'flexindex)
	     (identical? (downcase (getopt opts 'type {})) "flexindex")
	     (textsearch #("." (isdigit+) ".index") source))
	 (flex/open-index source opts))
	((exists? (flex/partition-files source "index"))
	 (flex/open-index source opts))
	(else {})))

(define (flex/dbref spec (opts #f))
  (when (and (table? spec) (not (or (pool? spec) (index? spec))))
    (if opts (set! opts (cons opts spec)) (set! opts spec))
    (set! spec (getopt opts 'index (getopt opts 'pool (getopt opts 'source #f)))))
  (cond ((pool? spec) (flex/wrap spec opts))
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
(define flex/db flex/dbref)
(define flex/ref flex/dbref)
(define db/ref flex/dbref)

(define (flex/make spec (opts #f))
  (flex/dbref spec (if opts (cons #[create #t] opts) #[create #t])))

;;; Pool ref

(define (pool/ref spec (opts #f))
  (when (and (table? spec) (not (or (pool? spec) (index? spec))))
    (if opts (set! opts (cons opts spec)) (set! opts spec))
    (set! spec (getopt opts 'pool (getopt opts 'source #f))))
  (cond ((pool? spec)
	 (flex/wrap spec opts))
	((not (string? spec)) (irritant spec |InvalidPoolSpec|))
	((or (position #\@ spec) (position #\: spec))
	 (flex/wrap (if (getopt opts 'adjunct)
			 (open-pool spec opts)
			 (use-pool spec opts))
		    opts))
	((and (file-exists? spec) (has-suffix spec ".pool"))
	 (flex/wrap (if (getopt opts 'adjunct)
			 (open-pool spec opts)
			 (use-pool spec opts))
		    opts))
	((and (file-exists? spec) (has-suffix spec ".flexpool")) 
	 (flexpool/open spec opts))
	((file-exists? (glom spec ".flexpool"))
	 (flexpool/ref (glom spec ".flexpool") opts))
	((file-exists? (glom spec ".pool"))
	 (flex/wrap (open-pool (glom spec ".pool") opts) opts))
	((not (or (getopt opts 'create) (getopt opts 'make)))
	 #f)
	((has-suffix spec ".flexpool")
	 (flexpool/make spec opts))
	((has-suffix spec ".pool")
	 (flex/wrap (make-pool spec (make-opts opts)) opts))
	((or (test opts 'flexpool) 
	     (test opts 'type 'flexpool)
	     (test opts '{flexpool step flexstep partsize partition}))
	 (flexpool/make spec opts))
	(else (flex/wrap (make-pool spec (make-opts opts)) opts))))

;;; Index refs

(define (index/ref spec (opts #f))
  (if (index? spec) spec
      (begin
	(when (and (table? spec) (not (or (pool? spec) (index? spec))))
	  (if opts (set! opts (cons opts spec)) (set! opts spec))
	  (set! spec (getopt opts 'index (getopt opts 'source #f))))
	(cond ((index? spec) spec)
	      ((not (string? spec)) (irritant spec |InvalidIndexSpec|))
	      ((or (identical? (downcase (getopt opts 'type {})) "flexindex")
		   (textsearch #("." (isdigit+) ".index") spec)
		   (has-suffix spec ".flexindex"))
	       (flex/open-index spec opts))
	      ((or (position #\@ spec) (position #\: spec)
		   (file-exists? spec))
	       (if (testopt opts 'background)
		   (use-index spec opts)
		   (open-index spec opts)))
	      ((getopt opts 'create)
	       (make-index spec opts))
	      ((getopt opts 'err) (irritant spec |IndexRefFailed|))
	      (else #f)))))

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

(define (flex/modified? arg)
  (cond ((registry? arg) (registry/modified? arg))
	((flexpool/record arg) (exists? (pick (flexpool/partitions arg) modified?)))
	((pool? arg) (modified? arg))
	((index? arg) (modified? arg))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) #t)
	((and (pair? arg) (applicable? (car arg))) #t)
	(else #f)))

(define (get-modified arg)
  (cond ((registry? arg) (tryif (registry/modified? arg) arg))
	((flexpool/record arg) (pick (flexpool/partitions arg) modified?))
	((pool? arg) 
	 (choice (tryif (modified? arg) arg)
		 (get-modified (poolctl arg 'partitions))))
	((index? arg)
	 (choice (tryif (modified? arg) arg)
		 (get-modified (indexctl arg 'partitions))))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) arg)
	((and (pair? arg) (applicable? (car arg))) arg)
	(else {})))

(define commit-threads #t)

(defambda (flex/commit! dbs (opts #f))
  (let ((modified (get-modified dbs))
	(started (elapsed-time)))
    (when (exists? modified)
      (let* ((timings (make-hashtable))
	     (fifo (fifo/make modified))
	     (spec-threads (mt/threadcount (getopt opts 'threads commit-threads)))
	     (n-threads (and spec-threads (min spec-threads (choice-size modified)))))
	(lognotice |FLEX/Commit|
	  "Saving " (choice-size modified) " dbs using " (or n-threads "no") " threads:"
	  (do-choices (db modified) (printout "\n\t" db)))
	(cond ((not n-threads)
	       (do-choices (db modified) (commit-db db opts timings)))
	      ((>= n-threads (choice-size modified))
	       (set! n-threads (choice-size modified))
	       (let ((threads (thread/call commit-db modified opts timings)))
		 (thread/wait! threads)))
	      (else
	       (let ((threads {}))
		 (dotimes (i n-threads)
		   (set+! threads (thread/call commit-queued fifo opts timings)))
		 (thread/wait! threads))))
	(lognotice |Flex/Commit|
	  "Committed " (choice-size (getkeys timings)) " dbs "
	  "in " (secs->string (elapsed-time started)) " "
	  "using " (or n-threads "no") " threads: "
	  (do-choices (db (getkeys timings))
	    (let ((time (get timings db)))
	      (if (>= time 0)
		  (printout "\n\t" ($num time 1) "s \t" db)
		  (printout "\n\tFAILED after " ($num time 1) "s:\t" db)))))))))

(defambda (flex/save! . args)
  (dolist (arg args)
    (flex/commit! arg)))

(define (inner-commit arg timings start)
  (cond ((registry? arg) (registry/save! arg))
	((pool? arg) (commit arg))
	((index? arg) (commit arg))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) (arg))
	((and (pair? arg) (applicable? (car arg)))
	 (apply (car arg) (cdr arg)))
	(else (logwarn |CantSave| "No method for saving " arg) #f))
  (store! timings arg (elapsed-time start))
  arg)

(define (commit-db arg opts timings (start (elapsed-time)))
  (onerror (inner-commit arg timings start)
      (lambda (ex)
	(store! timings arg (- (elapsed-time start)))
	(logwarn |CommitError| "Error committing " arg ": " ex)
	ex)))

(define (commit-queued fifo opts timings)
  (let ((db (fifo/pop fifo)))
    (commit-db db opts timings)))




