;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb/adjuncts)

(use-module '{kno/reflect texttools regex
	      logger logctl fifo
	     kno/mttools text/stringfmts opts})
(use-module 'knodb)

(define %loglevel %warn%)

(define dbfile-suffix
  {#("." (opt #((isxdigit+) ".")) "pool" (eos))
   #("." (opt #((isxdigit) (isxdigit) (isxdigit+) ".")) "index" (eos))
   #(".pool" (eos)) 
   #(".index" (eos))})

(module-export! '{adjuncts/init! adjuncts/setup! adjuncts/add!})

(define (adjuncts/init! pool (adjuncts) (opts #f))
  (unless (exists? (poolctl pool 'props 'adjuncts))
    (default! adjuncts (poolctl pool 'metadata 'adjuncts))
    (let ((cur (get-adjuncts pool))
	  (open-opts (getopt opts 'open
			     (frame-create #f
			       'rootdir (dirname (pool-source pool))
			       'cachelevel (getopt opts 'cachelevel {})
			       'loglevel (getopt opts 'loglevel {})))))
      (info%watch "ADJUNCTS/INIT!" pool adjuncts cur open-opts)
      (do-choices (slotid (getkeys adjuncts))
	(cond ((not (test cur slotid))
	       ;; No currently defined adjunct, go ahead and try to open one
	       (let* ((spec (get adjuncts slotid))
		      (adjopts (getadjopts pool slotid spec))
		      (usedb (knodb/ref (getopt adjopts 'source spec)
					(cons adjopts open-opts))))
		 (cond ((exists? usedb) (adjunct! pool slotid usedb))
		       ((getopt opts 'require_adjuncts)
			(irritant (get adjuncts slotid) |MissingAdjunct|
			  "for pool " pool))
		       (else
			(logwarn |MissingAdjunct| 
			  "Adjunct " (get adjuncts slotid) " couldn't be resolved for\n  "
			  pool)))))
	      ;; A currently defined adjunct which is consistent with the
	      ;; spec in the metadata, leav it.
	      ((consistent? (get cur slotid) 
			    (getadjopts pool slotid (get adjuncts slotid))
			    (dirname (pool-source pool))))
	      ;; An inconsistent adjunct and we're generating an error on confict
	      ((testopt opts 'override 'error)
	       (irritant `#[pool ,pool slotid ,slotid 
			    cur ,(get cur slotid)
			    new ,(get adjuncts slotid)]
		   |AdjunctConflict|
		 adjunct/init!))
	      ;; An inconsistent adjunct but we're just ignoring it, except for a log message
	      ((testopt opts 'override '{ignore keep})
	       (logwarn |AdjunctConflict| 
		 "Keeping existing adjunct for " slotid " of " pool ":"
		 "\n   keeping:   " (get cur slotid) 
		 "\n   ignoring:  " (get adjuncts slotid)))
	      ;; An inconsistent adjunct and the current one is modified, so we'll signal an error
	      ;;  rather than lose those modifications
	      ((and (modified? (get cur slotid))
		    (not (testopt opts 'force)))
	       (irritant `#[pool ,pool slotid ,slotid 
			    cur ,(get cur slotid)
			    new ,(get adjuncts slotid)]
		   |ModifiedAdjunctConflict|
		 adjuncts/init!))
	      (else 
	       ;; Try to replace the adunct
	       (let* ((spec (get adjuncts slotid))
		      (adjopts (getadjopts pool slotid spec))
		      (usedb (knodb/ref (getopt adjopts 'source spec)
					(cons adjopts open-opts))))
		 (cond ((exists? usedb)
			(unless (test cur slotid usedb)
			  (logwarn |AdjunctConflict| 
			    "Overriding existing adjunct for " slotid " of " pool ":" 
			    "\n   using:      " (get adjuncts slotid)
			    "\n   dropping:   " (get cur slotid)))
			(adjunct! pool slotid usedb))
		       ((getopt opts 'require_adjuncts)
			;; Signal an error here, even though there's another adjunct
			(irritant (get adjuncts slotid) |MissingAdjunct|
			  "for pool " pool))
		       (else
			(logwarn |MissingAdjunct| 
			  "New adjunct for " slotid " in " pool
			  " couldn't be resolved from " spec)))))))
      (poolctl pool 'props 'adjuncts (get-adjuncts pool)))))

(define (consistent? adjunct spec (dir))
  (unless (or (index? adjunct) (pool? adjunct))
    (irritant adjunct |NotAPoolOrIndex|))
  (if (index? adjunct)
      (and (test spec 'index)
	   (or (eq? adjunct (get spec 'index))
	       (equal? (index-source adjunct) (get spec 'index))
	       (equal? (realpath (index-source adjunct) dir)
		       (realpath (get spec 'index) dir))))
      (if (pool? adjunct)
	  (and (test spec 'pool)
	       (or (eq? adjunct (get spec 'pool))
		   (equal? (pool-source adjunct) (get spec 'pool))
		   (equal? (realpath (pool-source adjunct))
			   (realpath (get spec 'pool))))
	       (or (not (test spec 'base))
		   (equal? (get spec 'base) (pool-base adjunct))
		   (irritant adjunct |WrongPoolBase|
		     "not " (getopt spec 'base) ": " spec))
	       (or (not (test spec 'capacity))
		   (= (get spec 'capacity) (pool-capacity adjunct))
		   (irritant adjunct |WrongPoolCapacity|
		     "not " (getopt spec 'capacity) ": " spec)))
	  )))

(define (adjuncts/setup! pool (adjuncts) (opts #f))
  (default! adjuncts (poolctl pool 'metadata 'adjuncts))
  (let ((cur (get-adjuncts pool)))
    (debug%watch "ADJUNCTS/SETUP!" adjuncts cur)
    (do-choices (slotid (getkeys adjuncts))
      (cond ((not (test cur slotid))
	     (adjunct-setup! pool slotid (get adjuncts slotid) opts))
	    ((consistent? (get cur slotid) (get adjuncts slotid)
			  (dirname (pool-source pool))))
	    ((testopt opts 'override 'error)
	     (irritant `#[pool ,pool slotid ,slotid 
			  cur ,(get cur slotid)
			  new ,(get adjuncts slotid)]
		 |AdjunctConflict|
	       adjuncts/setup!))
	    ((testopt opts 'override '{ignore keep})
	     (logwarn |AdjunctConflict| 
	       "Keeping existing adjunct for " slotid " of " pool ":"
	       "\n   keeping:   " (get cur slotid) 
	       "\n   ignoring:  " (get adjuncts slotid)))
	    ((and (modified? (get cur slotid))
		  (not (testopt opts 'force)))
	     (irritant `#[pool ,pool slotid ,slotid 
			  cur ,(get cur slotid)
			  new ,(get adjuncts slotid)]
		 |ModifiedAdjunctConflict|
	       adjuncts/setup!))
	    (else 
	     (logwarn |AdjunctConflict| 
	       "Overriding existing adjunct for " slotid " of " pool ":" 
	       "\n   using:      " (get adjuncts slotid)
	       "\n   dropping:   " (get cur slotid))
	     (adjunct-setup! pool slotid (get adjuncts slotid) opts))))))

(define (adjunct-setup! pool slotid spec opts)
  (info%watch "ADJUNCT-SETUP!" pool slotid spec opts)
  (adjunct! pool slotid 
	    (ref-adjunct pool (cons (getadjopts pool slotid spec) opts))))

(define suffix-pat #((opt #("." (isxdigit+))) "." (isalpha+) (eos)))

(define (replace-suffix file suffix)
  (if (textsearch suffix-pat file)
      (textsubst file suffix-pat suffix)
      (glom file suffix)))

(define (ref-adjunct pool opts)
  (if (or (getopt opts 'index)
	  (overlaps? (downcase (getopt opts 'type {}))
		     {"hashindex" "fileindex" "logindex" "index"}))
      (if (file-exists? (abspath (getopt opts 'index (getopt opts 'source)) 
				 (dirname (pool-source pool))))
	  (open-index (abspath (getopt opts 'index)) opts)
	  (make-index (abspath (getopt opts 'index)) opts))
      ;; Assume it's a pool
      (let* ((source-suffix (gather suffix-pat (pool-source pool)))
	     (poolfile (getopt opts 'pool))
	     (filename
	      (abspath (replace-suffix (getopt opts 'pool) source-suffix)
		       (dirname (pool-source pool)))))
	(info%watch "REF-ADJUNCT" (pool-source pool) filename)
	(cond ((file-exists? filename) (open-pool filename opts))
	      ((or (getopt opts 'make) (getopt opts 'create))
	       (let ((adj-dir (abspath filename (dirname (pool-source pool)))))
		 (unless (file-directory? (dirname adj-dir))
		   (mkdir (dirname adj-dir)))
		 (make-pool filename opts)))
	      ((getopt opts 'err #t)
	       (irritant filename |MissingAdjunct| REF-ADJUNCT))
	      (else {})))))

(define (adjuncts/add! pool slotid spec)
  (when (string? spec)
    (set! spec
      (if (has-suffix spec ".pool")
	  `#[pool ,spec type bigpool adjunct ,slotid]
	  `#[index ,spec type hashindex adjunct ,slotid])))
  (let ((current (poolctl pool 'metadata 'adjuncts)))
    (cond ((fail? current)
	   (set! current `#[,slotid ,spec]))
	  ((not (test current slotid))
	   (store! current slotid spec))
	  (else
	   (irritant (get current slotid) |ExistingAdjunct| adjuncts/add!)))
    (poolctl pool 'metadata 'adjuncts current)
    (adjuncts/setup! pool `#[,slotid ,spec])
    (commit pool)))

(define (getadjopts pool slotid spec)
  (debug%watch "GETADJOPTS" pool slotid spec)
  (let* ((relpath (try (tryif (string? spec) spec)
		       (tryif (not (or (slotmap? spec) (schemap? spec)))
			 (irritant spec |InvalidAdjunctSpec| getadjopts))
		       (pickstrings
			(try (get spec 'index)
			     (get spec 'pool)
			     (get spec 'source)
			     (get spec 'path)))
		       (tryif (symbol? slotid) (symbol->string slotid))
		       (tryif (oid? slotid) (number->string (oid-addr slotid) 16))))
	 (poolsrc (pool-source pool))
	 (path (mkpath (dirname poolsrc) relpath))
	 (opts (if (string? spec) #[] 
		   (if (slotmap? spec) (deep-copy spec)
		       (if (schemap? spec) (schemap->slotmap spec)
			   (irritant spec |InvalidAdjunctSpec| getadjopts)))))
	 (isindex (or (test opts '{index indextype})
		      (has-suffix path ".index")
		      (test opts 'type 'index)
		      (exists indextype? (get opts 'type))))
	 (metadata (getopt opts 'metadata)))
    (unless metadata
      (set! metadata (frame-create #f))
      (store! opts 'metadata metadata))
    (store! opts 'adjunct slotid)
    (store! metadata 'adjunct slotid)
    (when isindex
      (store! opts '{index source} (get-adjindex-path poolsrc path))
      (store! opts 'index path)
      (store! opts '{type indextype}
	(try (difference (get opts 'type) 'index)
	     'hashindex))
      (unless (test opts 'size)
	(store! opts 'size (pool-capacity pool)))
      (unless (indextype? (get opts 'indextype))
	(irritant opts '|BadIndexType| getadjopts)))
    (unless isindex
      (store! opts '{pool source} (get-adjpool-path poolsrc path))
      (store! opts '{type pooltype}
	(try (difference (get opts 'type) 'pool)
	     (poolctl pool 'type)
	     'bigpool))
      (store! opts 'base (pool-base pool))
      (store! opts 'capacity (pool-capacity pool))
      (unless (pooltype? (get opts 'pooltype))
	(irritant opts |InvalidPoolType| getadjopts)))
    opts))

(define (get-adjpool-path poolsrc path)
  (mkpath (dirname poolsrc) 
	  (glom (textsubst path #((opt #("." (isxdigit+))) "." (isalpha+) (eos)) "")
	    (gather #((opt #("." (isxdigit+))) "." (isalpha+) (eos))
		    poolsrc))))

(define (get-adjindex-path poolsrc path)
  (if (position #\/ path)
      (mkpath (dirname poolsrc) (glom path ".index"))
      (glom (basename poolsrc #t)
	(and (not (has-prefix path "_")) "_")
	(strip-suffix path ".index")
	".index")))
