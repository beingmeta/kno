;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'flexdb)

(use-module '{ezrecords stringfmts logger texttools fifo mttools reflection})
(use-module '{flexdb/adjuncts flexdb/registry flexdb/filenames})
(use-module '{flexdb/flexpool flexdb/flexindex})

(module-export! '{pool/ref index/ref pool/copy 
		  flexdb/ref flex/dbref db/ref flex/db flex/ref
		  flexdb/make flexdb/partitions flexdb/commit! flexdb/save!
		  flex/make flex/partitions flex/commit! flex/save!
		  flexdb/pool})

(module-export! '{flex/container flex/container!})

(define-init %loglevel %notice%)

(define flexpool-suffix #("." (isxdigit+) ".pool" (eos)))
(define flexindex-suffix #("." (isxdigit+) ".index" (eos)))

(define tld `(isalnum+))

(define network-host
  `#((+ #((isalnum+) ".")) ,tld))

(define network-source 
  `{#((label host,network-host) ":" (label port (isdigit+)))
    #((label ttport (isalnum)) "@" (label host ,network-host))})

(define (checkdir source opts)
  (cond ((exists position {#\@ #\:} source) source)
	((not (position #\/ source)) source)
	((file-directory? (dirname source)) source)
	((getopt opts 'mkdir #f)
	 (mkdirs (dirname source))
	 source)
	(else (irritant source |MissingDirectory|))))

(define (get-db-source arg (opts #f) (rootdir))
  (default! rootdir (getopt opts 'rootdir))
  (cond ((not (string? arg)) arg)
	((textmatch network-source arg) arg)
	((not rootdir) (abspath arg))
	(else (mkpath rootdir arg))))

(define (make-opts opts)
  (let ((addopts (or (deep-copy (getopt opts 'make)) `#[])))
    (when (getopt opts 'adjuncts)
      (store! addopts 'metadata (or (deep-copy (getopt opts 'metadata)) #[]))
      (store! (getopt addopts 'metadata) 'adjuncts (getopt opts 'adjuncts)))
    (cons addopts opts)))

(define (flexdb/partitions arg)
  (cond ((pool? arg) (poolctl arg 'partitions))
	((index? arg) (or (indexctl arg 'partitions) {}))
	((string? arg) (flexdb/partition-files arg))
	(else (fail))))
(define flex/partitions flexdb/partitions)

(define (flexdb/pool pool (opts #f))
  (debug%watch "FLEXDB/POOL" pool opts)
  (unless (or (adjunct? pool) (exists? (poolctl pool 'props 'adjuncts)))
    (if (or (getopt opts 'make) (getopt opts 'create))
	(adjuncts/setup! pool 
			 (getopt opts 'adjuncts (poolctl pool 'metadata 'adjuncts))
			 opts)
	(adjuncts/init! pool
			(getopt opts 'adjuncts (poolctl pool 'metadata 'adjuncts))
			opts)))
  pool)

(define (wrap-index index (opts #f))
  index)

(define (flex-open source opts)
  (debug%watch "FLEX-OPEN" source "\nOPTS" opts)
  (cond ((pool? source) (flexdb/pool source opts))
	((or (index? source) (hashtable? source))
	 (wrap-index source opts))
	((or (has-suffix source ".flexpool")
	     (testopt opts 'flexpool)
	     (testopt opts 'type 'flexpool))
	 (flexpool/ref source opts))
	((or (has-suffix source ".flexindex")
	     (getopt opts 'flexindex)
	     (identical? (downcase (getopt opts 'type {})) "flexindex")
	     (textsearch #("." (isdigit+) ".index") source))
	 (flex/open-index source opts))
	((or (has-suffix source ".pool")
	     (testopt opts 'pool)
	     (testopt opts 'pooltype)
	     (testopt opts 'type 'pool))
	 (if (or (testopt opts 'adjunct)
		 (not (getopt opts 'background #t)))
	     (flexdb/pool (open-pool source opts) opts)
	     (flexdb/pool ( use-pool source opts) opts)))
	((or (has-suffix source ".index")
	     (testopt opts 'index)
	     (testopt opts 'indextype)
	     (testopt opts 'type 'index))
	 (if (testopt opts 'background)
	     (wrap-index (use-index source opts) opts)
	     (wrap-index (open-index source opts) opts)))
	((exists? (flexdb/partition-files source "index"))
	 (flex/open-index source opts))
	((exists? (flexdb/partition-files source "pool"))
	 (if (or (testopt opts 'adjunct)
		 (not (getopt opts 'background #t)))
	     (flexdb/pool (open-pool (flexdb/partition-files source "pool") opts) opts)
	     (flexdb/pool (use-pool (flexdb/partition-files source "pool") opts) opts)))
	(else (irritant source |UnknownDBType|))))

(define (flexdb/ref dbsource (opts #f))
  (when (and (table? dbsource) (not (or (pool? dbsource) (index? dbsource))))
    (if opts (set! opts (cons opts dbsource))
	(set! opts dbsource))
    (set! dbsource
      (cond ((testopt opts 'index) (get-db-source (getopt opts 'index) opts))
	    ((testopt opts 'pool) (get-db-source (getopt opts 'pool) opts))
	    ((testopt opts 'source) (get-db-source (getopt opts 'source) opts))
	    (else (irritant dbsource |NoDBSource| flex/dbref)))))
  (info%watch "FLEXDB/REF" dbsource "\nOPTS" opts)
  (cond ((pool? dbsource) (flexdb/pool dbsource opts))
	((or (index? dbsource) (hashtable? dbsource)) (wrap-index dbsource opts))
	((not (string? dbsource))
	 (irritant dbsource |BadDBSource| flex/dbref))
	((or (testopt opts '{flexindex flexpool})
	     (testopt opts 'type '{flexindex flexpool}))
	 (flex-open dbsource opts))
	((or (file-exists? dbsource) (textmatch (qc network-source) dbsource)
	     (exists? (flexdb/partition-files dbsource "index"))
	     (exists? (flexdb/partition-files dbsource "pool")))	     
	 (flex-open dbsource opts))
	((not (or (getopt opts 'create) (getopt opts 'make)))
	 (if (getopt opts 'err)
	     (irritant dbsource |NoSuchDatabase| flex/dbref)
	     #f))
	((textmatch (qc network-source) dbsource)
	 (irritant dbsource |CantCreateRemoteDB|))
	((or (has-suffix dbsource ".pool")
	     (testopt opts 'pool)
	     (testopt opts 'pooltype)
	     (testopt opts 'type 'pool))
	 (flexdb/pool (make-pool (checkdir dbsource opts) (make-opts opts))
		      opts))
	((or (has-suffix dbsource ".index")
	     (testopt opts 'index)
	     (testopt opts 'indextype)
	     (testopt opts 'type 'index))
	 (wrap-index (make-index (checkdir dbsource opts) (make-opts opts))
		     opts))
	(else (irritant (cons [source dbsource] opts) 
		  |CantDetermineDBType| flex/dbref))))
(define flex/dbref flexdb/ref)
(define flex/db flexdb/ref)
(define flex/ref flexdb/ref)
(define db/ref flexdb/ref)

;;; Variants

(define (flexdb/make spec (opts #f))
  (flexdb/ref spec (if opts (cons #[create #t] opts) #[create #t])))
(define flex/make flexdb/make)

(define (pool/ref spec (opts #f))
  (flexdb/ref spec 
	      (if (testopt opts 'pooltype) 
		  opts
		  (opt+ opts 'pooltype (getopt opts 'type 'bigpool)))))

(define (index/ref spec (opts #f))
  (flexdb/ref spec 
	      (if (testopt opts 'indextype) opts
		  (opt+ opts 'indextype (getopt opts 'type 'hashindex)))))

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

(define (flexdb/mods arg)
  (cond ((registry? arg) (pick arg registry/modified?))
	((flexpool/record arg) (pick (flexpool/partitions arg) modified?))
	((exists? (db->registry arg)) (pick (db->registry arg) registry/modified?))
	((or (pool? arg) (index? arg)) (pick arg modified?))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) arg)
	((and (pair? arg) (applicable? (car arg))) arg)
	(else (logwarn |CantSave| "No method for saving " arg) {})))
(define flex/mods flexdb/mods)

(define (flexdb/modified? arg)
  (cond ((registry? arg) (registry/modified? arg))
	((flexpool/record arg) (exists? (pick (flexpool/partitions arg) modified?)))
	((pool? arg) (modified? arg))
	((index? arg) (modified? arg))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) #t)
	((and (pair? arg) (applicable? (car arg))) #t)
	(else #f)))
(define flex/modified? flexdb/modified?)

(define (get-modified arg)
  (cond ((registry? arg) (tryif (registry/modified? arg) arg))
	((flexpool/record arg) (get-modified (flexpool/partitions arg)))
	((pool? arg) 
	 (let ((partitions (poolctl arg 'partitions))
	       (adjuncts (getvalues (poolctl arg 'adjuncts))))
	   (choice (tryif (modified? arg) arg)
		   (get-modified partitions)
		   (get-modified adjuncts)
		   (get-modified (for-choices (adjunct adjuncts) (dbctl adjunct 'partitions)))
		   (get-modified (for-choices (partition partitions)
				   (getvalues (dbctl partition 'adjuncts)))))))
	((index? arg)
	 (choice (tryif (modified? arg) arg)
		 (get-modified (indexctl arg 'partitions))))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) arg)
	((and (pair? arg) (applicable? (car arg))) arg)
	(else {})))

(define commit-threads #t)

(defambda (flexdb/commit! dbs (opts #f))
  (let ((modified (get-modified dbs))
	(started (elapsed-time)))
    (when (exists? modified)
      (let* ((timings (make-hashtable))
	     (fifo (fifo/make (choice->vector modified)))
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
(define flex/commit! flexdb/commit!)

(defambda (flexdb/save! . args)
  (dolist (arg args)
    (flexdb/commit! arg)))
(define flex/save! flexdb/save!)

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
    (while (and (exists? db) db)
      (commit-db db opts timings)
      (set! db (fifo/pop fifo)))))


