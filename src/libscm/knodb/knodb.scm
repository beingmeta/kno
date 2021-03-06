;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'knodb)

(use-module '{texttools reflection})
(use-module '{gpath fifo kno/mttools ezrecords text/stringfmts logger varconfig})
(use-module '{knodb/adjuncts knodb/registry knodb/filenames knodb/indexes})
(use-module '{knodb/flexpool knodb/flexindex})

(module-export! '{knodb/ref knodb/make knodb/commit! knodb/save! 
		  knodb/partitions knodb/components knodb/getindexes knodb/getindex
		  knodb/pool knodb/wrap-index
		  knodb/id knodb/source
		  knodb/mods knodb/modified? knodb/get-modified
		  pool/ref index/ref pool/copy
		  pool/getindex pool/getindexes knodb/add-index!
		  knodb/readonly!
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
  (cond ((and (string? dbsource) (textmatch (qc network-source) dbsource))
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
	  "Linking " ($count (|| indexes) "index" "indexes") " for " pool))))
  pool)

(define (get-indexes-for-pool pool (opts #f))
  (or (try (poolctl pool 'props 'indexes)
	   (let* ((indexes (sync-init-pool-indexes pool opts)))
	     (do-choices (partition (poolctl pool 'partitions))
	       (set+! indexes (get-indexes-for-pool partition opts)))
	     (poolctl pool 'props 'indexes indexes)
	     indexes))
      (fail)))

(define (init-pool-indexes pool (opts #f))
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
(define-init sync-init-pool-indexes
  (slambda (pool (opts #f)) (init-pool-indexes pool opts)))

(define pool-index-opts
  #[register #t])

(defambda (some-writable? indexes (result #f))
  (do-choices (index indexes)
    (unless (indexctl index 'readonly)
      (set! result #t)
      (break)))
  result)

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

(define (pool/getindex pool (opts #f))
  (try (pick pool index?)
       (poolctl pool 'props 'index)
       (get-index-for-pool pool opts)))

(define (pool/getindexes pool (opts #f))
  (try (pick pool index?)
       (poolctl pool 'props 'indexes)
       (get-indexes-for-pool pool opts)
       (pool/getindex pool opts)))

;;; Indexfns

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
      (if (testopt opts 'maxload)
	  (let* ((loaded (source->index source))
		 (index (open-index source (cons [register #f] opts)))
		 (repack (and (not loaded) (get-repack-size index opts))))
	    (cond (repack
		   (logwarn |RepackingIndex| 
		     "Repacking index " (write source) " based on maxload " (getopt opts 'maxload))
		   (index/pack! index #f `#[newsize ,repack])
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

(defambda (knodb/add-index! pool indexes)
  (let ((cur (poolctl pool 'props 'indexes))
	(new {(pick indexes index?)
	      (index/ref (reject indexes index?))})
	(combined (poolctl pool 'props 'index)))
    (unless (identical? cur new)
      (poolctl pool 'props indexes {cur new})
      (cond ((fail? combined))
	    ((aggregate-index? combined)
	     (add-to-aggregate-index!
	      combined (difference new (dbctl combined 'partitions))))
	    (else (poolctl pool 'props 'index
			   (make-aggregate-index {combined cur new} pool-index-opts)))))))

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

;;;; Repacking

;; (define (needs-repack? index opts (filename))
;;   (default! filename (indexctl index 'filename))
;;   (and filename (file-exists? filename) (file-writable? filename)
;;        (let* ((n-buckets (onerror (indexctl index 'metadata 'buckets) #f))
;; 	      (n-keys (and n-buckets (onerror (indexctl index 'metadata 'keys) #f)))
;; 	      (maxload (getopt opts 'maxload (indexctl index 'metadata 'maxload)))
;; 	      (loadsize (and maxload n-keys
;; 			     (->exact
;; 			      (* maxload (+ (max (getopt opts 'minkeys n-keys) n-keys)
;; 					    (getopt opts 'addkeys 0))))))
;; 	      (minsize (and (or loadsize (getopt opts 'minsize))
;; 			    (max (or loadsize 0) (getopt opts 'minsize 0)
;; 				 (getopt opts 'minkeys 0)))))
;; 	 (and n-buckets n-keys minsize (< n-buckets minsize)))))

(define (get-repack-size index opts (filename))
  (default! filename (indexctl index 'filename))
  (and filename (file-exists? filename) (file-writable? filename)
       (let* ((n-buckets (onerror (indexctl index 'metadata 'buckets) #f))
	      (n-keys (and n-buckets (onerror (indexctl index 'metadata 'keys) #f)))
	      (maxload (getopt opts 'maxload (indexctl index 'metadata 'maxload)))
	      (target-keys (+ (max (getopt opts 'minkeys 1961) n-keys) (getopt opts 'addkeys 0)))
	      (target-load (and maxload n-keys (->exact (* target-keys maxload))))
	      (min-buckets (and (or target-load (getopt opts 'minsize))
				(max (or target-load 0) (getopt opts 'minsize 0)
				     (getopt opts 'minkeys 0)))))
	 (and n-buckets n-keys min-buckets (< n-buckets min-buckets)
	      (begin (logwarn |RepackIndex| 
		       "Repacking index " (write filename) " to " ($num min-buckets) " buckets (current=" ($num n-buckets) "),\n"
		       "given keycount=" ($num n-keys) "/" ($num target-keys) " (cur/target) and maxload=" maxload)
		min-buckets)))))

;; (define (repack-index source opts old)
;;   (let* ((n-buckets (onerror (indexctl index 'metadata 'buckets) #f))
;; 	 (n-keys (and n-buckets (onerror (indexctl index 'metadata 'keys) #f)))
;; 	 (maxload (getopt opts 'maxload (indexctl index 'metadata 'maxload)))
;; 	 (loadsize (and maxload n-keys
;; 			(->exact
;; 			 (* maxload (+ (max (getopt opts 'minkeys n-keys) n-keys)
;; 				       (getopt opts 'addkeys 0))))))
;; 	 (minsize (and (or loadsize (getopt opts 'minsize))
;; 		       (max (or loadsize 0) (getopt opts 'minsize 0)
;; 			    (getopt opts 'minkeys 0))))
;; 	 (maxload (getopt opts 'maxload (dbctl old 'metadata 'maxload)))
;; 	 (pack-opts `(#[maxload ,maxload] . ,opts)))
    
;;     (index/pack! old #f pack-opts)))

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

(defambda (knodb/readonly! db flag (opts #f))
  (local component-opts (getopt opts 'components [filter {pool? index?}])
	 count 0)
  (do-choices (component (if component-opts (knodb/components db component-opts)
			     db))
    (unless (if flag (dbctl component 'metadata 'readonly)
		(not (dbctl component 'metadata 'readonly)))
      (dbctl component 'metadata 'readonly flag))
    (dbctl component 'readonly flag)
    (set! count (1+ count)))
  count)

;;;; OPEN-FILEDB

(define (open-filedb source opts)
  (debug%watch "OPEN-FILEDB" source "\nOPTS" opts)
  (cond ((pool? source) (knodb/pool source opts))
	((or (index? source) (hashtable? source))
	 (knodb/wrap-index source opts))
	((flexpool-spec? source opts)
	 (flexpool/ref source opts))
	((flexindex-spec? source opts)
	 (flex/open-index source opts))
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
	 (flex/open-index source opts))
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

;;; Committing

(define commit-threads #t)
(varconfig! COMMIT:THREADS commit-threads)

(define skipdbs 'tempindex)

(defambda (knodb/commit! (dbs (get-all-dbs)) (opts #f))
  (let ((modified (get-modified dbs))
	(%loglevel (getopt opts 'loglevel %loglevel))
	(threads-arg (mt/threadcount (getopt opts 'threads commit-threads)))
	(started (elapsed-time)))
    (when (exists? modified)
      (let* ((timings (make-hashtable))
	     (fifo (fifo/make (choice->vector modified)))
	     (n-threads (and threads-arg (min threads-arg (choice-size modified)))))
	(lognotice |Commit|
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
	(lognotice |Commit|
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
