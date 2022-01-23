;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'knodb)

(use-module '{texttools reflection})
(use-module '{gpath fifo kno/mttools ezrecords text/stringfmts logger varconfig})
(use-module '{knodb/adjuncts knodb/registry knodb/filenames knodb/indexes})
(use-module '{knodb/flexpool knodb/flexindex})

(module-export! '{knodb/ref knodb/make knodb/commit! knodb/save! 
		  knodb/partitions knodb/components knodb/getindexes knodb/getindex
		  knodb/pool knodb/wrap-index
		  knodb/id knodb/source
		  knodb/mods knodb/alldbs knodb/modified? knodb/get-modified
		  pool/ref index/ref pool/copy
		  pool/index/find pool/index/probe pool/index/open pool/index/target
		  pool/getindex pool/getindexes
		  pool/index/spec pool/index/add!
		  knodb/readonly! knodb/writable!
		  knodb/open-index
		  knodb/checkpath
		  knodb/makedb
		  knodb/index-frame
		  knodb/index-frame!
		  knodb/index!
		  knodb/find})

(module-export! '{knodb:pool knodb:index knodb:indexes})

(module-export! '{knodb/commit})

(module-export! '{knodb/mkpath knodb/subpath knodb/mksubpath})

(define-init %loglevel %notice%)

(define-init debugging-knodb #f)
(varconfig! knodb:debug debugging-knodb config:boolean)

(define-init init-index-size 100000)
(varconfig! knodb:initbuckets init-index-size)

(define default-repack #f)
(varconfig! knodb:repack default-repack)

;;; Patterns

(define flexpool-suffix #("." (isxdigit+) ".pool" (eos)))
(define flexindex-suffix #("." (isxdigit+) ".index" (eos)))

(define tld `(isalnum+))

(define network-host `#((+ #((isalnum+) ".")) ,tld))

(define network-source 
  `{#((label host,network-host) ":" (label port (isdigit+)))
    #((label ttport (isalnum)) "@" (label host ,network-host))})

(define (flexindex-spec? spec (opts #f))
  (or (and (string? spec) (has-suffix spec ".flexindex"))
      (and (testopt opts 'flexindex) (getopt opts 'flexindex))
      (testopt opts 'type 'flexindex)))

(define (flexpool-spec? spec (opts #f))
  (or (and (string? spec) (has-suffix spec ".flexpool"))
      (and (testopt opts 'flexpool) (getopt opts 'flexpool))
      (testopt opts 'type 'flexpool)))

;;; Utility functions

;;; This makes missing directories
(define (knodb/checkpath source opts (suffix #f))
  "If *source* looks like a path, this checks that it's intermediate "
  "directories all exist."
  (cond ((exists position {#\@ #\:} source) source)
	((not (position #\/ source)) (opt-suffix source suffix))
	((and (has-suffix source "/") (file-directory? source)) source)
	((has-suffix source "/")
	 (mkdirs source)
	 source)
	((file-directory? (dirname source)) source)
	((getopt opts 'mkdir (getopt opts 'create #f))
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

(define (knodb/source opts (rootdir))
  (default! rootdir (getopt opts 'rootdir))
  (if (pair? opts)
      (try (get-dbsource (car opts) rootdir)
	   (get-dbsource (cdr opts) rootdir))
      (let ((source (try (tryif (string? opts) opts)
			 (get opts 'index)
			 (get opts 'pool)
			 (get opts 'source))))
	(cond ((fail? source) source)
	      ((not (string? source)) source)
	      ((textmatch network-source source) source)
	      ((not rootdir) (abspath source))
	      ((has-prefix source {"~" "./" "../"}) (abspath source))
	      ((and (string? rootdir) (has-suffix rootdir {"_" "-"}))
	       (->gpath (glom rootdir source)))
	      (else (->gpath source rootdir))))))

(define (find-dbtype opts (assume #f) (head))
  "Finds the dbtype from options passed to knodb. The trick here is to handle "
  "options which may mingle pooltypes and indextypes, and is consistent with "
  "database specs returned by get-dbsource."
  (default! head (if (pair? opts) (car opts) opts))
  ;; Skip any #f options
  (while (and (not head) (pair? opts)) (set! opts (cdr opts)))
  (cond ((not opts) {})
	((and (eq? assume 'pool) (test head 'pooltype))
	 (cons 'pooltype (get head 'pooltype)))
	((and (eq? assume 'index) (test head 'indextype))
	 (cons 'indextype (get head 'indextype)))
	((and (not assume) (test head 'pooltype))
	 (cons 'pooltype (get head 'pooltype)))
	((and (not assume) (test head 'indextype))
	 (cons 'indextype (get head 'indextype)))
	((pair? opts)
	 (find-dbtype (cdr opts) 
		      (or assume (if (test head 'pool) 'pool
				     (if (test head 'index) 'index
					 #f)))))
	(else {})))

(define (make-opts opts (basetype #f))
  (let ((addopts (or (deep-copy (getopt opts 'make)) `#[]))
	(usetype (find-dbtype opts basetype)))
    (store! addopts (car usetype) (cdr usetype))
    (when (testopt opts 'addkeys)
      (store! addopts 'size 
	(max (getopt opts 'addkeys 0) (getopt opts 'size init-index-size))))
    (when (or (getopt opts 'adjuncts) (getopt opts 'maxload))
      (store! addopts 'metadata (or (deep-copy (getopt opts 'metadata)) #[])))
    (when (getopt opts 'adjuncts)
      (store! (getopt addopts 'metadata) 'adjuncts (getopt opts 'adjuncts)))
    (when (getopt opts 'maxload)
      (store! (getopt addopts 'metadata) 'maxload (getopt opts 'maxload)))
    (when (getopt opts 'prealloc)
      (store! (getopt addopts 'metadata) 'prealloc (getopt opts 'prealloc)))
    (cons addopts opts)))

(define (knodb/mkpath rootdir source (opts #f))
  (cond ((or (fail? source) (not source)) source)
	((not (string? source)) source)
	((textmatch network-source source) source)
	((not rootdir) (abspath source))
	((has-prefix source {"~" "./" "../"}) (abspath source))
	((and (string? rootdir) (has-suffix rootdir {"_" "-"}))
	 (->gpath (glom rootdir source)))
	(else (->gpath source rootdir))))

(define (knodb/subpath prefix . namespecs)
  (if (not prefix)
      (apply glom namespecs)
      (if (or (file-directory? prefix) (has-suffix prefix "/"))
	  (mkpath prefix (apply glom namespecs))
	  (if (has-suffix prefix {"_" "-" "."})
	      (glom prefix (apply glom namespecs))
	      (glom prefix "_" (apply glom namespecs))))))
(define (knodb/mksubpath prefix . namespecs)
  (let ((subpath (apply knodb/subpath prefix namespecs)))
    (unless (file-directory? (dirname subpath)) (mkdirs subpath))
    subpath))

;;;; Simple wrappers

(define (knodb/id arg (err #f))
  (cond ((pool? arg) (pool-id arg))
	((index? arg) (index-id arg))
	(err (irritant arg |BadKnoDB|))
	(else (stringout arg))))

(define (knodb/source arg (err #f))
  (cond ((pool? arg) (pool-source arg))
	((index? arg) (index-source arg))
	(err (irritant arg |BadKnoDB|))
	(else #f)))

;;;; knodb/ref

(define (knodb/ref spec (opts #f))
  (cond ((pool? spec) (knodb/pool spec opts))
	((or (index? spec) (hashtable? spec)) 
	 (knodb/wrap-index spec opts))
	((table? spec)
	 (resolve-dbref (get-dbsource (cons spec opts)) (cons opts spec)))
	((and (string? spec) (textmatch (qc network-source) spec))
	 (if (testopt opts '{dbtype type} 'pool)
	     (knodb/pool spec opts)
	     (knodb/wrap-index (open-index spec opts) opts)))
	((and (string? spec) (testopt opts 'rootdir))
	 (resolve-dbref (mkpath (getopt opts 'rootdir) spec) opts))
	(else (resolve-dbref spec opts))))

(define (resolve-dbref dbsource opts)
  (info%watch "RESOLVE-DBREF" dbsource "\nOPTS" opts)
  (cond ((not dbsource) #f)
	((and (string? dbsource) (textmatch (qc network-source) dbsource))
	 (if (testopt opts '{dbtype type} 'pool)
	     (knodb/pool dbsource opts)
	     (knodb/wrap-index (open-index dbsource opts) opts)))
	((or (file-exists? dbsource)
	     (flexindex-spec? dbsource opts)
	     (flexpool-spec? dbsource opts))
	 (open-filedb dbsource opts))
	((not (or (getopt opts 'create) (getopt opts 'make)))
	 (if (getopt opts 'err)
	     (irritant dbsource |NoSuchDatabase| knodb/refref)
	     #f))
	((textmatch (qc network-source) dbsource)
	 (irritant dbsource |CantCreateRemoteDB|))
	((has-suffix dbsource ".pool")
	 (knodb/pool (make-pool (knodb/checkpath dbsource opts ".pool")
		       (make-opts opts 'pool))
		     opts))
	((has-suffix dbsource ".index")
	 (knodb/wrap-index
	  (make-index (knodb/checkpath dbsource opts ".index")
	    (make-opts opts 'index))
	  opts))
	(else (let ((dbtype (find-dbtype opts #f)))
		(cond ((fail? dbtype)
		       (irritant (cons [source dbsource] opts) 
			   |CantDetermineDBType| knodb/ref))
		      ((eq? (car dbtype) 'pooltype)
		       (knodb/pool (make-pool (knodb/checkpath dbsource opts ".pool")
				     (make-opts opts 'pool))
				   opts))
		      ((eq? (car dbtype) 'indextype)
		       (knodb/wrap-index
			(make-index (knodb/checkpath dbsource opts ".index")
			  (make-opts opts 'index))
			opts))
		      (else (irritant dbtype |CantDetermineDBType| knodb/ref)))))))

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
			    (get-indexes-for-pool pool opts))
		       (fail))))
      (when (exists? indexes)
	(loginfo |Indexes| 
	  "Linking " ($count (|| indexes) "indexes") " for " pool))))
  pool)

;;;; Pool indexes

(define open-pool-appendixes #f)

(define pool-index-opts #[register #t])

(define (open-pool-index pool spec (opts #f) (rootdir))
  (default! rootdir (or (getopt opts 'indexloc #f)
			(poolctl pool 'opts 'indexloc)
			(try (poolctl pool 'props 'indexloc) #f)
			(try (poolctl pool 'metadata 'indexloc) #f)
			(dirname (pool-source pool))))
  (cond ((string? spec)
	 (index/ref spec (cons [rootdir rootdir] pool-index-opts)))
	((not (table? spec))
	 (logwarn |BadIndexSpec| spec " for " pool)
	 (fail))
	((not (test spec 'path))
	 (logwarn |NoIndexPath| " in " spec " for " pool)
	 (fail))
	((and (test spec 'appendix)
	      (not (or (getopt opts 'appendixes)
		       (poolctl pool 'props 'appendixes)
		       (poolctl pool 'opts 'appendixes) 
		       open-pool-appendixes)))
	 (loginfo |SkippedAppendix|
	   "Skipping appendix "
	   (try (get spec 'name) (get spec 'path)) " for " pool)
	 (fail))
	(else (index/ref (get spec 'path) 
			 (cons [rootdir rootdir] (cons spec pool-index-opts))))))

(define (init-pool-indexes pool (opts #f))
  (let* ((rootdir (or (getopt opts 'indexloc)
		      (try (poolctl pool 'opts 'indexloc) #f)
		      (try (poolctl pool 'metadata 'indexloc) #f)
		      (dirname (pool-source pool))))
	 (indexrefs (poolctl pool 'metadata 'indexes))
	 (indexes (pick (for-choices (ref indexrefs)
			  (if debugging-knodb
			      (open-pool-index pool ref opts)
			      (onerror (open-pool-index pool ref opts)
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
(define-init sync-init-pool-indexes
  (slambda (pool (opts #f)) (init-pool-indexes pool opts)))

;;;; Finding pool indexes

(defambda (pool/index/find pool . args)
  (filter-choices (index (or (poolctl pool 'props 'indexes) (fail)))
    (match-index index args)))

(defambda (pool/index/probe pool . args)
  (try (filter-choices (index (or (poolctl pool 'props 'indexes) (fail)))
	 (match-index index args))
       (let ((specs (or (poolctl pool 'metadata 'indexes) (fail))))
	 (for-choices (spec (apply pick specs args))
	   (onerror (open-pool-index pool spec)
	       (lambda (ex)
		 (logwarn |BadPoolIndex|
		   "Error while resolving index"
		   (if (test spec 'name) (printout " '" (get spec 'name) "' "))
		   " for " pool "\n  " spec "\n " ex)
		 (fail)))))))

(defambda (pool/index/spec pool . args)
  (let ((specs (or (poolctl pool 'metadata 'indexes) (fail))))
    (apply pick specs args)))

(defambda (pool/index/open pool . args)
  (let ((specs (or (poolctl pool 'metadata 'indexes) (fail))))
    (for-choices (spec (apply pick specs args))
      (onerror (open-pool-index pool spec)
	  (lambda (ex)
	    (logwarn |BadPoolIndex|
	      "Error while resolving index"
	      (if (test spec 'name) (printout " '" (get spec 'name) "' "))
	      " for " pool "\n  " spec "\n " ex)
	    spec)))))

(define (pool/index/add! pool index (persist #f))
  (cond ((index? index)
	 (when persist (logwarn |CantPersist| "direct index " index " for " pool))
	 (unless (overlaps? index (pool/getindexes pool))
	   (poolctl pool 'props index 'add)
	   (when (exists? (difference (poolctl pool 'props 'index) index))
	     (if (aggregate-index? (poolctl pool 'props 'index))
		 (add-to-aggregate-index! (poolctl pool 'props 'index) index)
		 (poolctl pool 'props 'index (make-aggregate-index (poolctl pool 'props 'index) index))))))
	(else (let ((new-index (make-pool-index (getopt index 'source) pool index)))
		(pool/index/add! pool new-index)
		(when persist (poolctl pool 'metadata 'indexes index 'add))
		new-index))))

(define (match-index index args)
  (local matched #t)
  (while (and (pair? args) matched)
    (let ((slot (car args))
	  (val (and (pair? (cdr args)) (cadr args))))
      (set! matched
	(cond ((overlaps? slot '{keyslot source type})
	       (overlaps? (indexctl index slot) val))
	      ((eq? slot 'readonly)
	       (if (pair? (cdr args))
		   (equal? val (indexctl index 'readonly))
		   (indexctl index 'readonly)))
	      ((eq? slot 'writable)
	       (if (pair? (cdr args))
		   (equal? val (not (indexctl index 'readonly)))
		   (not (indexctl index 'readonly))))
	      (else (if (pair? (cdr args))
			(overlaps? val (indexctl index 'metadata slot))
			(indexctl index 'metadata slot)))))
      (set! args (cdr args))
      (when (pair? args) (set! args (cdr args)))))
  matched)

;;;; Used by pool/ref etc

(define (pool/getindex pool (opts #f))
  (try (pick pool index?)
       (poolctl pool 'props 'index)
       (get-index-for-pool pool opts)))

(define (pool/getindexes pool (opts #f))
  (try (pick pool index?)
       (poolctl pool 'props 'indexes)
       (get-indexes-for-pool pool opts)
       (pool/getindex pool opts)))

(define (get-indexes-for-pool pool (opts #f))
  (or (try (poolctl pool 'props 'indexes)
	   (let* ((indexes (sync-init-pool-indexes pool opts)))
	     (do-choices (partition (poolctl pool 'partitions))
	       (set+! indexes (get-indexes-for-pool partition opts)))
	     (poolctl pool 'props 'indexes indexes)
	     indexes))
      (fail)))

(define (get-index-for-pool pool (opts #f))
  (or (try (poolctl pool 'props 'index)
	   (let* ((indexes (pick (get-indexes-for-pool pool opts) index?))
		  (aggregate-opts
		   (if (or (not (poolctl pool 'readonly))
			   (some-writable? indexes))
		       (cons #[cachelevel 0] pool-index-opts)
		       pool-index-opts))
		  (index-opts (if opts (cons opts aggregate-opts) aggregate-opts))
		  (index (tryif (exists? indexes)
			   (if (singleton? indexes)
			       indexes
			       (pick (make-aggregate-index indexes aggregate-opts)
				 index?)))))
	     (poolctl pool 'props 'index index)
	     (poolctl pool 'props 'indexes indexes)
	     index))
      (fail)))

(defambda (some-writable? indexes)
  "Returns true if any of *indexes* is writable"
  (local result #f)
  (do-choices (index indexes)
    (unless (indexctl index 'readonly)
      (set! result #t)
      (break)))
  result)

;;;; Making indexes

(define (slots->string slotid)
  (cond ((ambiguous? slotid)
	 (stringout (do-choices (slot slotid i)
		      (printout (if (> i 0) "_") (slots->string slot)))))
	((symbol? slotid) (symbol->string slotid))
	(else #f)))

(define (roundup j) (->exact (ceiling j)))

(define (get-sizing base pool)
  (cond ((or (not (number? base)) (<= base 0)) #mib)
	((or (inexact? base) (< base 100))
	 (->exact (* base (1+ (quotient (+ (pool-load pool) (pool-capacity pool)) 2)))))
	((< base 100) #mib)
	(else base)))

(define (make-pool-index pool ref spec (rootdir))
  (default! rootdir
    (try (or (poolctl pool 'opts 'indexloc) (fail))
	 (poolctl pool 'props 'indexloc)
	 (poolctl pool 'metadata 'indexloc)
	 (dirname (pool-source pool))))
  (debug%watch "make-pool-index" rootdir ref rootdir pool "\n" spec)
  (let* ((capacity (get-sizing (getopt spec 'sizing 2.0) pool))
	 (keyslots (getopt spec 'keyslot))
	 (name (getopt spec 'name (or (slots->string keyslots) ref)))
	 (db-label (pool-label pool))
	 (add-opts (frame-create #f
		     'rootdir rootdir 'create #t
		     'size capacity
		     'keyslots (tryif keyslots keyslots)
		     'label (glom name "." db-label)
		     'metadata (frame-create #f 'name name 'tags (getopt spec 'tags {})))))
    (cond ((string? ref) (index/ref ref (cons add-opts spec)))
	  ((not (table? ref))
	   (logwarn |BadIndexRef| ref " for " pool)
	   (fail))
	  ((not (test ref 'path))
	   (logwarn |NoIndexPath| ref " for " pool)
	   (fail))
	  (else (index/ref (get ref 'path) (cons add-opts spec))))))

(defambda (pool/index/target pool . args)
  (let ((indexes (try (filter-choices (index (or (poolctl pool 'props 'indexes) (fail)))
			(and (not (indexctl index 'readonly)) (match-index index args)))
		      (let ((specs (or (poolctl pool 'metadata 'indexes) (fail))))
			(for-choices (spec (reject (apply pick specs args) 'readonly))
			  (make-pool-index pool (getopt spec 'path) spec))))))
    (knodb/writable! indexes)
    indexes))

;;; Indexfns
;;; Note that this is mostly replaced by knodb/index+! and database fuzz

(define-init default-indexslots {})
(varconfig! knodb:indexslots default-indexslots)

(define-init default-skipindex {})
(varconfig! knodb:skipindex default-skipindex)

(define-init default-indexall {})
(varconfig! knodb:indexall default-indexall config:boolean)

(define-init generic-indexfns {})
(define-init default-indexfns (make-hashtable))

(config-def! 'knodb:indexfns
  (lambda (var (val))
    (cond ((unbound? val) default-indexfns)
	  ((applicable? val) (set+! generic-indexfns val))
	  ((and (compound? val '%fnref) (> (compound-length val) 1))
	   (let* ((module (get-module (compound-ref val 0)))
		  (fn (and module (symbol? (compound-ref val 1))
			   (get module (compound-ref val 1)))))
	     (cond ((applicable? fn)
		    (set+! generic-indexfns val))
		   ((not module)
		    (logwarn |UnknownModule| (compound-ref val 0) " resolving " val))
		   (else
		    (logwarn |BadFnRef| fn " resolving " val)))))
	  ((not (pair? val)) (irritant val |InvalidIndexFnInit|))
	  (else (let ((fn (resolve-indexfn (cdr val))))
		  (if (applicable? fn)
		      (store! default-indexfns (car val) fn)
		      (irritant (cdr val) |BadIndexFn|)))))))

(define-init cached-indexfns (make-hashtable))
(define-init pending-indexfns (make-hashtable))

(defslambda (init-indexfns-for-pool pool)
  (try (poolctl pool 'props 'indexfns)
       (let ((fns (poolctl pool 'metadata 'indexfns)))
	 (cond ((fail? fns)
		(poolctl pool 'metadata 'props 'indexfns #f)
		#f)
	       (else (let ((indexfns (resolve-indexfns fns pool)))
		       (poolctl pool 'props 'indexfns indexfns)
		       indexfns))))))

(define (knodb/getindexfns pool)
  (try (poolctl pool 'props 'indexfns)
       (init-indexfns-for-pool pool)))

(define (resolve-indexfns template pool)
  (let ((resolved (frame-create #f)))
    (do-choices (key (getkeys template))
      (do-choices (spec (get template key))
	(let ((slot (if (pair? spec) (or (car spec) key) key))
	      (spec (if (pair? spec) (cdr spec) spec))
	      (fn (resolve-indexfn spec pool)))
	  (when fn
	    (add! resolved key (if slot (cons slot fn) fn))))))
    resolved))

(define (resolve-indexfn spec (pool #f))
  (cond ((applicable? spec) spec)
	((and (pair? spec) (get-module (car spec)))
	 (let ((fn (get (get-module (car spec)) (cdr spec))))
	   (if (applicable? fn)
	       (begin (add! cached-indexfns spec fn) fn)
	       (irritant spec |BadIndexFn|))))
	((and (compound? spec '%fnref) (> (compound-length spec) 1))
	 (let* ((module (get-module (compound-ref spec 0)))
		(fn (and module (symbol? (compound-ref spec 1))
			 (get module (compound-ref spec 1)))))
	   (and (applicable? fn) fn)))
	;; This should update pending-indexfns
	(else #f)))

;;; Index opening/wrapping/etc

(define (knodb/wrap-index index (opts #f)) index)

(define (knodb/open-index source opts)
  (or (source->index source)
      (if (and (or (testopt opts 'maxload) (getopt opts 'repack default-repack))
	       (file-exists? source) (file-writable? source))
	  ;; When we open an existing index with a 
	  (let* ((loaded (source->index source))
		 (index (open-index source (cons [register #f shared #f] opts)))
		 (repack (and (not loaded) (get-repack-size index opts)))
		 (backup (and repack
			      (getopt opts 'backup (config 'KNODB:BACKUP (CONFIG 'BACKUP))))))
	    (cond (repack
		   (logwarn |RepackingIndex| 
		     "Repacking index " (write source) " to " repack
		     " based on maxload " (getopt opts 'maxload))
		   (index/pack! index #f `#[newsize ,repack backup ,backup])
		   ;; The 'right' thing would be to reopen the index,
		   ;;  but reopening isn't currently supported, so
		   ;;  we try to force the index to be freed and then
		   ;;  open it again.
		   (set! index #f)
		   (set! index 
		     (if (testopt opts 'background)
			 (use-index source opts)
			 (open-index source opts))))
		  ((getopt opts 'register #t)
		   (set! index #f)
		   (set! index 
		     (if (testopt opts 'background)
			 (use-index source opts)
			 (open-index source opts))))
		  (else))
	    (knodb/wrap-index index opts))
	  (knodb/wrap-index (open-index source opts)))))

(define (knodb/getindex arg (opts-arg #f))
  (cond ((index? arg) arg)
	((pool? arg) (pool/getindex arg))
	((string? arg) (index/ref arg opts-arg))
	(else (index/ref arg))))

(define (knodb/getindexes arg (opts-arg #f))
  (cond ((index? arg) arg)
	((pool? arg) (pool/getindexes arg))
	((string? arg) (index/ref arg opts-arg))
	(else (index/ref arg))))

(defambda (knodb/index-frame frames slots (values))
  (do-choices (pool (oid->pool frames))
    (if (bound? values)
	(index-frame (pool/getindex pool) (pick frames pool) slots values)
	(index-frame (pool/getindex pool) (pick frames pool) slots))))
(define knodb/index-frame! (fcn/alias knodb/index-frame))

(define (index+ index frame slotid indexfn (values))
  (when indexfn
    (default! values (get frame slotid))
    (unless (fail? values)
      (let* ((indexslot (if (pair? indexfn) (or (car indexfn) slotid) slotid))
	     (indexfn (if (pair? indexfn) (cdr indexfn) indexfn))
	     (indexvals (cond ((eq? indexfn #t) values)
			      ((slotid? indexfn) (get values indexfn))
			      ((table? indexfn) (get indexfn values))
			      ((applicable? indexfn) (indexfn values))
			      (else (fail)))))
	(when (exists? indexvals)
	  (index-frame index frame slotid indexvals))))))

(defambda (knodb/index! frames (opts #f) (index) (indexfns))
  (default! index (getopt opts 'index #f))
  (default! indexfns (getopt opts 'indexfns #f))
  (do-choices (pool (oid->pool frames))
    (let ((index (or index (knodb/getindex pool)))
	  (indexfns (or indexfns (knodb/getindexfns pool)))
	  (gindexfns generic-indexfns)
	  (slots (getopt opts 'indexslots (poolctl pool 'metadata 'indexslots)))
	  (indexall (getopt opts 'indexall default-indexall))
	  (prefetch (getopt opts 'prefetch #t))
	  (frames (pick frames pool)))
      (when prefetch (prefetch-oids! frames))
      (cond ((fail? frames))
	    ((fail? slots)
	     (do-choices (frame frames)
	       (do-choices (slotid (getkeys frame))
		 (cond ((and (not indexfns)
			     (or default-indexall (overlaps? slotid default-indexslots))
			     (not (overlaps? slotid default-skipindex)))
			(index-frame index frame slotid)
			(index+ index frame slotid gindexfns))
		       ((or indexall (test indexfns slotid))
			(index+ index frame slotid {(get indexfns slotid) gindexfns}))
		       (else)))))
	    (else
	     (do-choices (slotid (difference slots (getopt opts 'skipindex {})))
	       (let ((indexers {(tryif indexfns (get indexfns slotid))
				(tryif (and (or default-indexall (overlaps? slotid default-indexslots))
					    (not (overlaps? slotid default-skipindex)))
				  gindexfns)}))
		 (do-choices (frame frames) (index+ index frame slotid indexers)))))))))

(defambda (knodb/find index . slotvals)
  (cond ((index? index) (apply find-frames index slotvals))
	((pool? index) (apply find-frames (pool/getindex index) slotvals))
	((oid? index) (apply find-frames (pool/getindex (oid->pool index)) slotvals))
	(else (apply find-frames #f index slotvals))))

;;; Prefetching

(defambda (knodb/prefetch-oids! oids (pool #f))
  (if pool
      (pool-prefetch! oids pool)
      (let ((pools (getpool oids))
	    (threads {}))
	(if (singleton? pools)
	    (prefetch-oids! oids pool)
	    (do-choices (pool pools)
	      (set+! threads (thread/call pool-prefetch! (pick oids pool) pool))))
	(thread/wait threads))))

;;;; Auto repacking
;;;; When we open an index 

(define (get-repack-size index opts (filename))
  (default! filename (indexctl index 'filename))
  (and filename 
       (file-exists? filename) (file-writable? filename)
       (not (dbctl index 'readonly))
       (let* ((n-buckets (onerror (indexctl index 'metadata 'buckets) #f))
	      (n-keys (and n-buckets (onerror (indexctl index 'metadata 'keys) #f)))
	      (maxload (getopt opts 'maxload (indexctl index 'metadata 'maxload)))
	      (target-keys (+ (max (getopt opts 'minkeys 1961) n-keys)
			      (getopt opts 'addkeys 0)))
	      (target-buckets (and maxload target-keys (->exact (/~ target-keys maxload))))
	      (min-buckets (and (or target-buckets (getopt opts 'minsize))
				(->exact
				 (max (or target-buckets 0) (getopt opts 'minsize 0)
				      (/~ (getopt opts 'minkeys 0) maxload))))))
	 (and n-buckets n-keys min-buckets (< n-buckets min-buckets)
	      (begin (logwarn |RepackIndex| 
		       "Repacking index " (write filename) 
		       " to " ($num min-buckets) " buckets (current=" ($num n-buckets) "),\n"
		       "given keycount=" ($num n-keys) "/" ($num target-keys)
		       " (cur/target) and maxload=" maxload)
		min-buckets)))))

;;; Getting partitions

(define (gather-partitions-loop db set)
  (unless (get set db)
    (hashset-add! set db)
    (cond ((or (pool? db) (index? db))
	   (gather-partitions-loop (dbctl db 'partitions) set))
	  ((string? db)
	   (gather-partitions-loop (knodb/partition-files db) set))
	  (else))))

(defambda (knodb/partitions arg)
  (local set (make-hashset))
  (gather-partitions-loop arg set)
  (pick (hashset-elts set) {index? pool?}))

(define (gather-components-loop db set indexes)
  (unless (get set db)
    (hashset-add! set db)
    (when (or (pool? db) (index? db))
      (gather-components-loop (dbctl db 'partitions) set indexes))
    (gather-components-loop (getvalues (dbctl db 'adjuncts)) set indexes)))

(defambda (knodb/components arg (opts #f))
  (local set (make-hashset)
	 include-indexes (or (eq? opts #t) (getopt opts 'indexes #f))
	 filter (getopt opts 'filter {index? pool?}))
  (gather-components-loop arg set include-indexes)
  (pick (hashset-elts set) filter))

(defambda (knodb/readonly! db flag (opts #f) (persist))
  (if (eq? opts #t)
      (begin (set! persist #t) (set! opts #f))
      (default! persist (getopt opts 'persist #f)))
  (local components (getopt opts 'components [filter {pool? index?}])
	 count 0)
  (do-choices (component (if components
			     (knodb/components db components)
			     db))
    (let ((cur (dbctl component 'readonly))
	  (curconfig (dbctl component 'metadata 'readonly)))
      (cond ((and flag persist curconfig)) ;; No-op
	    ((and flag persist)
	     (dbctl component 'metadata 'readonly #t)
	     (dbctl component 'readonly #t))
	    (flag (dbctl component 'readonly #t))
	    (curconfig 
	     (if (dbctl component 'metadata 'readonly #f)
		 (dbctl component 'readonly #f)
		 (logwarn |ReadModeLocked|
		   "Unable to change the readonly setting of " component)))
	    (else (dbctl component 'readonly #f))))
    (set! count (1+ count)))
  count)

(defambda (knodb/writable! db (opts #f))
  (knodb/readonly! db #f opts))

;;;; OPEN-FILEDB

(define (open-filedb source opts)
  (debug%watch "OPEN-FILEDB" source "\nOPTS" opts)
  (cond ((pool? source) (knodb/pool source opts))
	((or (index? source) (hashtable? source))
	 (knodb/wrap-index source opts))
	((flexpool-spec? source opts)
	 (flexpool/ref source opts))
	((flexindex-spec? source opts)
	 (flexindex/ref source opts))
	((has-suffix source ".pool")
	 (if (or (testopt opts 'adjunct)
		 (not (getopt opts 'background #t)))
	     (knodb/pool (open-pool source opts) opts)
	     (knodb/pool (use-pool source opts) opts)))
	((has-suffix source ".index")
	 (knodb/open-index source opts))
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
	 (flexindex/ref source opts))
	((exists? (knodb/partition-files source "pool"))
	 (if (or (testopt opts 'adjunct)
		 (not (getopt opts 'background #t)))
	     (knodb/pool (open-pool (knodb/partition-files source "pool") opts) opts)
	     (knodb/pool (use-pool (knodb/partition-files source "pool") opts) opts)))
	(else (irritant source |UnknownDBType|))))

;;; makefiledb

(define-init makedb-defaults
  [capacity #mib size #4mib pooltype 'kpool indextype 'kindex])

(define (knodb/makedb base (opts #f))
  (let ((create-opts #[create #t mkdir #t]))
    (set! base (textsubst base #("." (isalnum+) (eos)) ""))
    (let* ((opts (opt+ create-opts opts makedb-defaults))
	   (index (index/ref (glom base ".index") opts))
	   (pool (pool/ref (glom base ".pool") 
			   (cons [metadata `#[indexes ,(glom (basename base) ".index")]]
				 opts))))
      pool)))

;;; Variants

(define (pool/ref spec (opts #f))
  (knodb/ref spec 
	     (if (testopt opts 'pooltype) opts
		 (opt+ opts 'pooltype (getopt opts 'type 'kpool)))))

(define (index/ref spec (opts #f))
  (knodb/ref spec (if (testopt opts 'indextype) opts
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

(define skip-indexes 'tempindex)

(define (get-modified arg)
  "Returns all modified databases under *arg*, partitions and adjuncts"
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
	((aggregate-index? arg) (get-modified (indexctl arg 'partitions)))
	((index? arg)
	 (tryif (not (overlaps? (dbctl arg 'type) skip-indexes))
	   (choice (tryif (modified? arg) arg)
		   (get-modified (indexctl arg 'partitions)))))
	((and (applicable? arg) (zero? (procedure-min-arity arg))) arg)
	((and (pair? arg) (applicable? (car arg))) arg)
	(else {})))
(define knodb/get-modified (fcn/alias get-modified))

(define (get-all-dbs)
  (let* ((alldbs {(config 'pools) (config 'indexes)})
	(registries (db->registry alldbs))
	(unregistered (reject alldbs db->registry)))
    {unregistered registries}))
(define knodb/alldbs (fcn/alias get-all-dbs))

;;; Committing

(define commit-loglevel {})
(varconfig! knodb:commit:loglevel commit-loglevel config:loglevel)
(varconfig! knodb:commit:log commit-loglevel config:loglevel)

(define commit-threads #t)
(varconfig! KNODB:COMMIT:THREADS commit-threads)
(varconfig! COMMIT:THREADS commit-threads)

(define skipdbs 'tempindex)

(define (getdbload db)
  "This returns the key used to sort databases for commit. "
  "The idea is to have bigger databases first so that they have "
  "all the time they need. This may be adjusted if saving them at "
  "the same time causes performance issues."
  (cond ((pool? db) (* 4 (change-load db)))
	((index? db) (change-load db))
	(else 1)))

(define (getdbtype db)
  "This returns database type tag."
  (cond ((pool? db) {(poolctl db 'type) (poolctl db 'props 'tags) (poolctl db 'metadata 'tags)})
	((index? db) {(indexctl db 'type) (indexctl db 'props 'tags) (indexctl db 'metadata 'tags)})
	(else {})))

(defambda (knodb/commit! (dbs (get-all-dbs)) (opts #f))
  "Commits all of the modified *dbs* and their components. "
  "Multiple threads may be used for saving the databases. "
  "Other options include:\n"
  "* `threads` indicating how many threads to use and configured by the `KNODB:COMMIT:THREADS`\n"
  "* `loglevel` (for knodb/commit! itself, configured as `KNODB:COMMIT:LOGLEVEL`)\n"
  "* `skipdbs` indicating dbs or db types or tags to skip\n"
  "* `commit` specifies options to be passed to individual `commit` calls"
  (let* ((skipdbs {(getopt opts 'skipdbs {}) 'tempindex})
	 (modified (difference (reject (get-modified dbs) getdbtype skipdbs) skipdbs))
	 (%loglevel (getopt opts 'loglevel (try commit-loglevel %loglevel)))
	 (threads-arg (mt/threadcount (getopt opts 'threads commit-threads)))
	 (started (elapsed-time)))
    (when (exists? modified)
      (let* ((timings (make-hashtable))
	     (loads (make-hashtable))
	     (fifo (fifo/make (rsorted modified getdbload)))
	     (n-threads (and threads-arg (min threads-arg (choice-size modified)))))
	(lognotice |Commit|
	  "Saving " (choice-size modified) " dbs using "
	  (or n-threads "no") " threads:"
	  (when (log>? %notify%)
	    (do-choices (db modified) (printout "\n\t" db))))
	(cond ((not n-threads)
	       (do-choices (db modified) (commit-db db timings loads)))
	      ((>= n-threads (choice-size modified))
	       (set! n-threads (choice-size modified))
	       (let ((threads (thread/call commit-db modified timings loads)))
		 (thread/wait! threads)))
	      (else
	       (let ((threads {}))
		 (dotimes (i n-threads)
		   (set+! threads 
		     (thread/call commit-queued fifo timings loads)))
		 (thread/wait! threads))))
	(lognotice |Commit|
	  "Committed " (choice-size (getkeys timings)) " dbs "
	  "in " (secs->string (elapsed-time started)) " "
	  "using " (or n-threads "no") " threads: "
	  (do-choices (db (getkeys timings))
	    (let ((time (get timings db))
		  (load (get loads db)))
	      (if (>= time 0)
		  (printout "\n\t" ($num time 1) "s \t" load "\t\t" db)
		  (printout "\n\tFAILED after " ($num time 1) "s for " load "items:\t" db)))))))))
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

(define (commit-db arg timings loads (start (elapsed-time)))
  (store! loads arg (change-load arg))
  (if debugging-knodb
      (inner-commit arg timings start)
      (onerror (inner-commit arg timings start)
	  (lambda (ex)
	    (store! timings arg (- (elapsed-time start)))
	    (logwarn |CommitError| "Error committing " arg ": " ex)
	    ex))))
(define (commit-queued fifo timings loads)
  (let ((db (fifo/pop fifo)))
    (while (and (exists? db) db)
      (commit-db db timings loads)
      (set! db (fifo/pop fifo)))))

;;;; Configs

(define (usedb-config var (val))
  (if (bound? val)
      (knodb/ref val)
      (|| {(config 'pools) (config 'indexes)})))

(config-def! 'usedb usedb-config)

;;;; Defaults

(define (knodb:pool ref)
  "Returns a pool based on 'ref', designed to be called when "
  "processing configuration values. If REF is a symbol, it is "
  "taken to be a config setting which is resolved. Otherwise, "
  "the KNODB pool/ref handler is used."
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
  "Returns an index based on 'ref', designed to be called when "
  "processing configuration values. If REF is a symbol, it is "
  "taken to be a config setting which is resolved. Otherwise, "
  "the KNODB index/ref handler is used. Note that this treats "
  "a simple hashtable as an index."
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

(define (knodb:indexes ref)
  "Returns a set of indexes based on 'ref', designed to be called when "
  "processing configuration values. If REF is a symbol, it is "
  "taken to be a config setting which is resolved. Otherwise, "
  "knodb/ref is used; if it returns an index, that is returned; "
  "if it returns a pool, it returns the index for the pool."
  (when (symbol? ref)
    (if (config ref)
	(set! ref (config ref))
	(irritant ref |UndefinedIndexConfigRef|)))
  (cond ((index? ref) ref)
	((pool? ref)
	 (try (get-indexes-for-pool ref)
	      (irritant ref |InvalidIndexRef|)))
	((hashtable? ref) ref)
	((or (string? ref) (opts? ref))
	 (let* ((name (if (string? ref) ref 
			  (getopt ref 'source (getopt ref 'index (getopt ref 'pool)))))
		(ref-opts (and (opts? ref) ref))
		(db (and name (knodb/ref name ref-opts))))
	   (if (not db) (irritant ref |InvalidIndexRef|)
	       (choice (pick db index?) (pool/getindexes (pick db pool?))))))
	(else (irritant ref |InvalidIndexRef|))))
