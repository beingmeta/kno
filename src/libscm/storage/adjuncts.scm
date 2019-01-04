;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'storage/adjuncts)

(use-module '{reflection texttools regex
	      logger logctl fifo
	      mttools stringfmts opts})
(use-module 'storage/flex)

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
	       (let ((usedb (db/ref (get adjuncts slotid) open-opts)))
		 (cond ((exists? usedb) (adjunct! pool slotid usedb))
		       ((getopt opts 'require_adjuncts)
			(irritant (get adjuncts slotid) |MissingAdjunct|
			  "for pool " pool))
		       (else
			(logwarn |MissingAdjunct| 
			  "Adjunct " (get adjuncts slotid) " couldn't be resolved for\n  "
			  pool)))))
	      ((consistent? (get cur slotid) (get adjuncts slotid)))
	      ((testopt opts 'override 'error)
	       (irritant `#[pool ,pool slotid ,slotid 
			    cur ,(get cur slotid)
			    new ,(get adjuncts slotid)]
		   |AdjunctConflict|
		 adjunct/init!))
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
		 adjuncts/init!))
	      (else 
	       (let ((spec (get adjuncts slotid))
		     (usedb (db/ref (get adjuncts slotid) open-opts)))
		 (cond ((exists? usedb)
			(unless (test cur slotid usedb)
			  (logwarn |AdjunctConflict| 
			    "Overriding existing adjunct for " slotid " of " pool ":" 
			    "\n   using:      " (get adjuncts slotid)
			    "\n   dropping:   " (get cur slotid)))
			(adjunct! pool slotid usedb))
		       ((getopt opts 'require_adjuncts)
			(irritant (get adjuncts slotid) |MissingAdjunct|
			  "for pool " pool))
		       (else
			(logwarn |MissingAdjunct| 
			  "New adjunct " (get adjuncts slotid)
			  " couldn't be resolved for\n  "
			  pool)))))))
      (poolctl pool 'props 'adjuncts (get-adjuncts pool)))))

(define (consistent? adjunct spec)
  (unless (or (index? adjunct) (pool? adjunct))
    (irritant adjunct |NotAPoolOrIndex|))
  (if (index? adjunct)
      (and (test spec 'index)
	   (or (eq? adjunct (get spec 'index))
	       (equal? (index-source adjunct) (get spec 'index))
	       (equal? (realpath (index-source adjunct))
		       (realpath (get spec 'index)))))
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
		   (< (get spec 'capacity) (pool-capacity adjunct))
		   (irritant adjunct |WrongPoolCapacity|
		     "not " (getopt spec 'capacity) ": " spec)))
	  )))

(define (adjuncts/setup! pool (adjuncts) (opts #f))
  (default! adjuncts (poolctl pool 'metadata 'adjuncts))
  (let ((cur (get-adjuncts pool)))
    (do-choices (slotid (getkeys adjuncts))
      (cond ((not (test cur slotid))
	     (adjunct-setup! pool slotid (get adjuncts slotid) opts))
	    ((consistent? (get cur slotid) (get adjuncts slotid)))
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

(define (adjunct-setup! pool slotid adjopts opts)
  (when (string? adjopts)
    (set! adjopts
      (cond ((has-prefix adjopts ".index") `#[index ,adjopts])
	    ((has-prefix adjopts ".pool")
	     `#[pool ,adjopts
		base ,(pool-base pool) 
		capacity ,(pool-capacity pool)])
	    (else `#[index ,adjopts]))))
  (unless (or (getopt adjopts 'pool)  (getopt adjopts 'index))
    (irritant adjopts |InvalidAdjunct|))
  (adjunct! pool slotid 
	    (ref-adjunct pool
			 (cons `#[adjunct ,slotid
				  base ,(pool-base pool) 
				  capacity ,(pool-capacity pool)
				  metadata #[adjunct ,slotid adjuncts #[]]
				  make #t] 
			       adjopts))))

(define (ref-adjunct pool opts)
  (if (or (getopt opts 'index)
	  (overlaps? (downcase (getopt opts 'type {}))
		     {"hashindex" "fileindex" "memindex" "index"}))
      (if (file-exists? (abspath (getopt opts 'index (getopt opts 'source)) 
				 (dirname (pool-source pool))))
	  (open-index (abspath (getopt opts 'index)) opts)
	  (make-index (abspath (getopt opts 'index)) opts))
      ;; Assume it's a pool
      (let* ((source-suffix (gather (qc dbfile-suffix) (pool-source pool)))
	     (poolfile (getopt opts 'pool))
	     (filename
	      (abspath (textsubst (getopt opts 'pool) 
				  (qc dbfile-suffix) source-suffix)
		       (dirname (pool-source pool)))))
	(info%watch "REF-ADJUNCT" (pool-source pool) filename)
	(cond ((file-exists? filename) (open-pool filename opts))
	      ((getopt opts 'make) (make-pool filename opts))
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


