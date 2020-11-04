;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb)

(use-module '{texttools reflection})
(use-module '{gpath fifo mttools ezrecords stringfmts logger varconfig})
(use-module '{knodb/adjuncts knodb/registry knodb/filenames})
(use-module '{knodb/flexpool knodb/flexindex})

(module-export! '{knodb/ref knodb/make knodb/commit! knodb/save! 
		  knodb/partitions knodb/getindex
		  knodb/pool knodb/wrap-index
		  knodb/id knodb/source
		  knodb/mods knodb/modified?
		  pool/ref index/ref pool/copy
		  pool/getindex pool/getindexes
		  knodb/index!})

(module-export! '{knodb:pool knodb:index})

(module-export! '{knodb/commit})

(module-export! '{knodb/subpath knodb/mksubpath})

(define-init %loglevel %notice%)

(define-init debugging-knodb #f)
(varconfig! knodb:debug debugging-knodb config:boolean)

;;; Patterns

(define flexpool-suffix #("." (isxdigit+) ".pool" (eos)))
(define flexindex-suffix #("." (isxdigit+) ".index" (eos)))

(define tld `(isalnum+))

(define network-host `#((+ #((isalnum+) ".")) ,tld))

(define network-source 
  `{#((label host,network-host) ":" (label port (isdigit+)))
    #((label ttport (isalnum)) "@" (label host ,network-host))})

;;; Utility functions

(define (checkdbpath source opts (suffix #f))
  (cond ((exists position {#\@ #\:} source) source)
	((not (position #\/ source)) (opt-suffix source suffix))
	((file-directory? (dirname source)) source)
	((getopt opts 'mkdir #f)
	 (mkdirs (dirname source))
	 (opt-suffix source suffix))
	(else (irritant source |MissingDirectory|))))

(define (get-dbsource opts (rootdir))
  (default! rootdir (getopt opts 'rootdir))
  (if (pair? opts)
      (try (get-dbsource (car opts) rootdir)
	   (get-dbsource (cdr opts) rootdir))
      (let ((source (try (get opts 'index) (get opts 'pool) (get opts 'source))))
	(cond ((fail? source) source)
	      ((not (string? source)) source)
	      ((textmatch network-source source) source)
	      ((not rootdir) (abspath source))
	      ((has-prefix source {"~" "./" "../"}) (abspath source))
	      (else (->gpath source rootdir))))))

(define (make-opts opts)
  (let ((addopts (or (deep-copy (getopt opts 'make)) `#[])))
    (when (and (testopt opts 'indextype) (not (testopt opts 'type)))
      (store! addopts 'type (getopt opts 'indextype)))
    (when (and (testopt opts 'pooltype) (not (testopt opts 'type)))
      (store! addopts 'type (getopt opts 'pooltype)))
    (when (getopt opts 'adjuncts)
      (store! addopts 'metadata (or (deep-copy (getopt opts 'metadata)) #[]))
      (store! (getopt addopts 'metadata) 'adjuncts (getopt opts 'adjuncts)))
    (cons addopts opts)))

(define (knodb/subpath prefix . namespecs)
  (if (file-directory? prefix)
      (mkpath prefix (apply glom namespecs))
      (if (has-suffix prefix {"_" "-" "."})
	  (glom prefix (apply glom namespecs))
	  (glom prefix "_" (apply glom namespecs)))))
(define (knodb/mksubpath prefix . namespecs)
  (let ((subpath (apply knodb/subpath prefix namespecs)))
    (unless (file-directory? (dirname subpath)) (mkdirs subpath))
    subpath))

;;;; Simple wrappers

(define (knodb/id arg (err #f))
  (cond ((pool? arg) (pool-id arg))
	((index? arg) (index-id arg))
	(err (irritant db |BadKnoDB|))
	(else (stringout arg))))

(define (knodb/source arg (err #f))
  (cond ((pool? arg) (pool-source arg))
	((index? arg) (index-source arg))
	(err (irritant db |BadKnoDB|))
	(else #f)))

;;;; knodb/ref

(define (knodb/ref spec (opts #f))
  (cond ((pool? spec) (knodb/pool spec opts))
	((or (index? spec) (hashtable? spec)) 
	 (knodb/wrap-index spec opts))
	((table? spec)
	 (resolve-dbref (get-dbsource (cons spec opts)) (cons opts spec)))
	((and (string? spec) (testopt opts 'rootdir))
	  (resolve-dbref (mkpath (getopt opts 'rootdir) spec) opts))
	(else (resolve-dbref spec opts))))

(define (resolve-dbref dbsource opts)
  (info%watch "RESOLVE-DBREF" dbsource "\nOPTS" opts)
  (cond ((or (testopt opts '{flexindex flexpool})
	     (testopt opts 'type '{flexindex flexpool}))
	 (flex-open dbsource opts))
	((or (file-exists? dbsource) (textmatch (qc network-source) dbsource)
	     (exists? (knodb/partition-files dbsource "index"))
	     (exists? (knodb/partition-files dbsource "pool")))	     
	 (flex-open dbsource opts))
	((not (or (getopt opts 'create) (getopt opts 'make)))
	 (if (getopt opts 'err)
	     (irritant dbsource |NoSuchDatabase| knodb/refref)
	     #f))
	((textmatch (qc network-source) dbsource)
	 (irritant dbsource |CantCreateRemoteDB|))
	((or (has-suffix dbsource ".pool")
	     (testopt opts 'pool)
	     (testopt opts 'pooltype)
	     (testopt opts 'type 'pool))
	 (knodb/pool (make-pool (checkdbpath dbsource opts ".pool")
		       (make-opts opts))
		     opts))
	((or (has-suffix dbsource ".index")
	     (testopt opts 'index)
	     (testopt opts 'indextype)
	     (testopt opts 'type 'index))
	 (knodb/wrap-index
	  (make-index (checkdbpath dbsource opts ".index")
	    (make-opts opts))
	  opts))
	(else (irritant (cons [source dbsource] opts) 
		  |CantDetermineDBType| knodb/ref))))

(define (knodb/make spec (opts #f))
  (knodb/ref spec (if opts (cons #[create #t] opts) #[create #t])))

(define (knodb/pool pool (opts #f))
  (debug%watch "KNODB/POOL" pool opts)
  (unless (pool? pool) (irritant pool |NotAPool|))
  (unless (or (adjunct? pool) (exists? (poolctl pool 'props 'adjuncts)))
    (if (or (getopt opts 'make) (getopt opts 'create))
	(adjuncts/setup! pool 
			 (getopt opts 'adjuncts (poolctl pool 'metadata 'adjuncts))
			 opts)
	(adjuncts/init! pool
			(getopt opts 'adjuncts (poolctl pool 'metadata 'adjuncts))
			opts)))
  (when (getopt opts 'searchable #t)
    (let ((indexes (or (try (poolctl pool 'props 'indexes) 
			    (get-indexes-for-pool pool))
		       (fail))))
      (when (exists? indexes)
	(loginfo |Indexes| 
	  "Linking " ($count (|| indexes) "index" "indexes") " for " pool))))
  pool)

(define (get-indexes-for-pool pool (opts #f))
  (or (try (poolctl pool 'props 'indexes)
	   (let* ((rootdir (dirname (pool-source pool)))
		  (indexrefs (poolctl pool 'metadata 'indexes))
		  (indexes (pick (for-choices (ref indexrefs)
				   (if debugging-knodb
				       (index/ref ref (opt+ 'rootdir rootdir opts))
				       (onerror (index/ref ref (opt+ 'rootdir rootdir opts))
					   (lambda (ex)
					     (logwarn |IndexRefError|
					       (exception-summary ex) " REF=\n" (listdata ref)
					       "\nrootdir=" (write rootdir) " OPTS=\n" (listdata opts))
					     #f))))
			     
			     index?)))
	     (do-choices (partition (poolctl pool 'partitions))
	       (set+! indexes (get-indexes-for-pool partition opts)))
	     (poolctl pool 'props 'indexes indexes)
	     indexes))
      (fail)))

(define (get-index-for-pool pool (opts #f))
  (or (try (poolctl pool 'props 'index)
	   (let* ((indexes (pick (get-indexes-for-pool pool opts) index?))
		  (index (tryif (exists? indexes)
			   (if (singleton? indexes) indexes
			       (pick (make-aggregate-index indexes opts)
				 index?)))))
	     (poolctl pool 'props 'index index)
	     index))
      (fail)))

(define (pool/getindex pool (opts #f))
  (try (pick pool index?)
       (poolctl pool 'props 'index)
       (get-index-for-pool pool opts)))

(define (pool/getindexes pool (opts #f))
  (try (pick pool index?)
       (poolctl pool 'props 'indexes)
       (get-indexes-for-pool pool opts)
       (pool/getindex pool opts)))

(define (knodb/wrap-index index (opts #f)) index)

(define (knodb/getindex arg (opts-arg #f))
  (cond ((index? arg) arg)
	((pool? arg) (pool/getindex arg))
	((string? arg) (index/ref arg opts-arg))
	(else (index/ref arg))))

(defambda (knodb/index! frames slots (values))
  (do-choices (pool (oid->pool frames))
    (if (bound? values)
	(index-frame (pool/getindex pool) (pick frames pool) slots values)
	(index-frame (pool/getindex pool) (pick frames pool) slots))))

;;; Getting partitions

(define (knodb/partitions arg)
  (cond ((pool? arg) (poolctl arg 'partitions))
	((index? arg) (indexctl arg 'partitions))
	((string? arg) (knodb/partition-files arg))
	(else (fail))))

;;;; FLEX-OPEN

(define (flex-open source opts)
  (debug%watch "FLEX-OPEN" source "\nOPTS" opts)
  (cond ((pool? source) (knodb/pool source opts))
	((or (index? source) (hashtable? source))
	 (knodb/wrap-index source opts))
	((or (has-suffix source ".flexpool")
	     (testopt opts 'flexpool)
	     (testopt opts 'type 'flexpool))
	 (flexpool/ref source opts))
	((or (has-suffix source ".flexindex")
	     (getopt opts 'flexindex)
	     (identical? (downcase (getopt opts 'type {})) "flexindex")
	     (textsearch #("." (isdigit+) ".index") source))
	 (flex/open-index source opts))
	((has-suffix source ".pool")
	 (if (or (testopt opts 'adjunct)
		 (not (getopt opts 'background #t)))
	     (knodb/pool (open-pool source opts) opts)
	     (knodb/pool (use-pool source opts) opts)))
	((has-suffix source ".index")
	 (if (testopt opts 'background)
	     (knodb/wrap-index (use-index source opts) opts)
	     (knodb/wrap-index (open-index source opts) opts)))
	((or (testopt opts 'pool)
	     (testopt opts 'pooltype)
	     (testopt opts 'type 'pool))
	 (if (or (testopt opts 'adjunct)
		 (not (getopt opts 'background #t)))
	     (knodb/pool (open-pool source opts) opts)
	     (knodb/pool (use-pool source opts) opts)))
	((or (testopt opts 'index)
	     (testopt opts 'indextype)
	     (testopt opts 'type 'index))
	 (if (testopt opts 'background)
	     (knodb/wrap-index (use-index source opts) opts)
	     (knodb/wrap-index (open-index source opts) opts)))
	((exists? (knodb/partition-files source "index"))
	 (flex/open-index source opts))
	((exists? (knodb/partition-files source "pool"))
	 (if (or (testopt opts 'adjunct)
		 (not (getopt opts 'background #t)))
	     (knodb/pool (open-pool (knodb/partition-files source "pool") opts) opts)
	     (knodb/pool (use-pool (knodb/partition-files source "pool") opts) opts)))
	(else (irritant source |UnknownDBType|))))

;;; Variants

(define (pool/ref spec (opts #f))
  (knodb/ref spec 
	     (if (testopt opts 'pooltype) opts
		 (opt+ opts 'pooltype (getopt opts 'type 'kpool)))))

(define (index/ref spec (opts #f))
  (knodb/ref spec 
	     (if (testopt opts 'indextype) opts
		 (opt+ opts 'indextype (getopt opts 'type 'kindex)))))

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

(define (knodb/mods arg)
  (cond ((registry? arg) (pick arg registry/modified?))
	((flexpool/record arg) (pick (flexpool/partitions arg) modified?))
	((exists? (db->registry arg)) (pick (db->registry arg) registry/modified?))
	((or (pool? arg) (index? arg)) (pick arg modified?))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) arg)
	((and (pair? arg) (applicable? (car arg))) arg)
	(else (logwarn |CantSave| "No method for saving " arg) {})))

(define (knodb/modified? arg)
  (cond ((registry? arg) (registry/modified? arg))
	((flexpool/record arg) (exists? (pick (flexpool/partitions arg) modified?)))
	((pool? arg) (modified? arg))
	((index? arg) (modified? arg))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) #t)
	((and (pair? arg) (applicable? (car arg))) #t)
	(else #f)))

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
				   (getvalues (or (dbctl partition 'adjuncts) {})))))))
	((index? arg)
	 (choice (tryif (modified? arg) arg)
		 (get-modified (indexctl arg 'partitions))))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) arg)
	((and (pair? arg) (applicable? (car arg))) arg)
	(else {})))

(define (get-all-dbs)
  (let* ((alldbs {(config 'pools) (config 'indexes)})
	(registries (db->registry alldbs))
	(unregistered (reject alldbs db->registry)))
    {unregistered registries}))

;;; Committing

(define commit-threads #t)

(defambda (knodb/commit! (dbs (get-all-dbs)) (opts #f))
  (let ((modified (get-modified dbs))
	(started (elapsed-time)))
    (when (exists? modified)
      (let* ((timings (make-hashtable))
	     (fifo (fifo/make (choice->vector modified)))
	     (spec-threads 
	      (mt/threadcount (getopt opts 'threads commit-threads)))
	     (n-threads (and spec-threads
			     (min spec-threads (choice-size modified)))))
	(lognotice |FLEX/Commit|
	  "Saving " (choice-size modified) " dbs using "
	  (or n-threads "no") " threads:"
	  (when (log>? %notify%)
	    (do-choices (db modified) (printout "\n\t" db))))
	(cond ((not n-threads)
	       (do-choices (db modified) (commit-db db opts timings)))
	      ((>= n-threads (choice-size modified))
	       (set! n-threads (choice-size modified))
	       (let ((threads (thread/call commit-db modified opts timings)))
		 (thread/wait! threads)))
	      (else
	       (let ((threads {}))
		 (dotimes (i n-threads)
		   (set+! threads 
		     (thread/call commit-queued fifo opts timings)))
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
(define knodb/commit knodb/commit!)

(defambda (knodb/save! . args)
  (dolist (arg args)
    (when arg (knodb/commit! arg))))

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
  (if debugging-knodb
      (inner-commit arg timings start)
      (onerror (inner-commit arg timings start)
	  (lambda (ex)
	    (store! timings arg (- (elapsed-time start)))
	    (logwarn |CommitError| "Error committing " arg ": " ex)
	    ex))))

(define (commit-queued fifo opts timings)
  (let ((db (fifo/pop fifo)))
    (while (and (exists? db) db)
      (commit-db db opts timings)
      (set! db (fifo/pop fifo)))))

;;;; Configs

(define (usedb-config var (val))
  (if (bound? val)
      (knodb/ref val)
      (|| {(config 'pools) (config 'indexes)})))

(config-def! 'usedb usedb-config)

;;;; Defaults

(define (knodb:pool ref)
  (when (symbol? ref)
    (if (config ref)
	(set! ref (config ref))
	(irritant ref |UndefinedPoolConfigRef|)))
  (cond ((pool? ref) ref)
	((index? ref) (irritant ref |InvalidPoolRef|))
	((and (opts? ref) (testopt ref '{source pool}))
	 (pool/ref (getopt ref 'pool (opt-suffix (getopt ref 'source) ".pool"))
		   ref))
	((string? ref) (pool/ref (opt-suffix ref ".pool")))
	(else (irritant ref |InvalidPoolRef|))))

(define (knodb:index ref)
  (when (symbol? ref)
    (if (config ref)
	(set! ref (config ref))
	(irritant ref |UndefinedIndexConfigRef|)))
  (cond ((index? ref) ref)
	((pool? ref) (irritant ref |InvalidIndexRef|))
	((hashtable? ref) ref)
	((string? ref) (index/ref (opt-suffix ref ".index")))
	((and (opts? ref) (testopt ref '{source index}))
	 (index/ref (getopt ref 'index (opt-suffix (getopt ref 'source) ".index"))
		    ref))
	(else (irritant ref |InvalidIndexRef|))))
