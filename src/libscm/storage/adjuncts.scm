;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/adjuncts)

(use-module '{reflection texttools regex
	      logger logctl fifo
	      mttools stringfmts opts})

(define %loglevel %warn%)

(define dbfile-suffix
  {#("." (opt #((isxdigit+) ".")) "pool" (eos))
   #("." (opt #((isxdigit) (isxdigit) (isxdigit+) ".")) "index" (eos))
   #(".pool" (eos)) 
   #(".index" (eos))})

(module-export! '{adjuncts/init! adjuncts/setup! adjuncts/add!})

(define (adjuncts/init! pool (adjuncts) (opts #f))
  (default! adjuncts (poolctl pool 'metadata 'adjuncts))
  (let ((cur (get-adjuncts pool)))
    (do-choices (slotid (getkeys adjuncts))
      (cond ((not (test cur slotid))
	     (adjunct! pool slotid
		       (db/ref (get adjuncts slotid) opts)))
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
	       "\n   ignoring:  " (get adjunct slotid)))
	    ((and (modified? (get cur slotid))
		  (not (testopt opts 'force)))
	     (irritant `#[pool ,pool slotid ,slotid 
			  cur ,(get cur slotid)
			  new ,(get adjuncts slotid)]
		 |ModifiedAdjunctConflict|
	       adjuncts/init!))
	    (else 
	     (let ((usedb (db/ref (get adjuncts slotid) opts)))
	       (logwarn |AdjunctConflict| 
		 "Overriding existing adjunct for " slotid " of " pool ":" 
		 "\n   using:      " (get adjunct slotid)
		 "\n   dropping:   " (get cur slotid))
	       (adjunct! pool slotid usedb)))))))

(define (consistent? adjunct spec)
  (if (index? adjunct)
      (and (test spec 'index)
	   (or (equal? (index-source adjunct) (get spec 'index))
	       (equal? (realpath (index-source adjunct))
		       (realpath (get spec 'index)))))
      (if (pool? adjunct)
	  (and (test spec 'pool)
	       (or (equal? (pool-source adjunct) (get spec 'pool))
		   (equal? (realpath (pool-source adjunct))
			   (realpath (get spec 'pool))))
	       (or (not (test spec 'base))
		   (equal? (get spec 'base) (pool-base adjunct))
		   (irritant adjunct |WrongPoolBase|
		     "not " (getopt spec 'base) ": " spec))
	       (or (not (test spec 'capacity))
		   (< (get spec 'capacity) (pool-capacity adjunct))
		   (irritant adjunct |WrongPoolCapacity|
		     "not " (getopt spec 'capacity) ": " spec))))))

(define (adjuncts/setup! pool (adjuncts) (opts #f))
  (default! adjuncts (poolctl pool 'metadata 'adjuncts))
  (let ((cur (get-adjuncts pool)))
    (do-choices (slotid (getkeys adjuncts))
      (cond ((not (test cur slotid))
	     (setup-adjunct! pool slotid (get adjuncts slotid) opts))
	    ((consistent? (get cur slotid) (get adjuncts slotid)))
	    ((testopt opts 'override 'error)
	     (irritant `#[pool ,pool slotid ,slotid 
			  cur ,(get cur slotid)
			  new ,(get adjuncts slotid)]
		 |AdjunctConflict|
	       setup-adjuncts!))
	    ((testopt opts 'override '{ignore keep})
	     (logwarn |AdjunctConflict| 
	       "Keeping existing adjunct for " slotid " of " pool ":"
	       "\n   keeping:   " (get cur slotid) 
	       "\n   ignoring:  " (get adjunct slotid)))
	    ((and (modified? (get cur slotid))
		  (not (testopt opts 'force)))
	     (irritant `#[pool ,pool slotid ,slotid 
			  cur ,(get cur slotid)
			  new ,(get adjuncts slotid)]
		 |ModifiedAdjunctConflict|
	       setup-adjuncts!))
	    (else 
	     (logwarn |AdjunctConflict| 
	       "Overriding existing adjunct for " slotid " of " pool ":" 
	       "\n   using:      " (get adjunct slotid)
	       "\n   dropping:   " (get cur slotid))
	     (setup-adjunct! pool slotid (get adjuncts slotid) opts))))))

(define (adjunct-setup! pool slotid adjopts opts)
  (when (string? adjopts)
    (set! adjopts
      (cond ((has-prefix adjopts ".index")  `#[index ,adjopts])
	    ((has-prefix adjopts ".pool")
	     `#[pool ,adjopts
		base ,(pool-base pool) 
		capacity ,(pool-capacity pool)
		adjunct ,slotid])
	    (else `#[index ,adjopts]))))
  (unless (or (getopt adjopts 'pool)  (getopt adjopts 'index))
    (irritant adjopts |InvalidAdjunct|))
  (adjunct! pool slotid (ref-adjunct pool (cons adjopts opts))))

(define (ref-adjunct pool opts)
  (if (getopt opts 'index)
      (if (file-exists? (abspath (getopt opts 'index)))
	  (open-index (abspath (getopt opts 'index)) opts)
	  (make-index (abspath (getopt opts 'index)) opts))
      (let ((source-suffix (gather (qc dbfile-suffix) (pool-source pool)))
	    (poolfile (getopt opts 'pool))
	    (filename
	     (abspath (textsubst (getopt opts 'pool) 
				 (qc dbfile-suffix) source-suffix))))
	(if (file-exists? filename)
	    (open-pool filename opts)
	    (make-pool filename opts)))))

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
	   (irritant (get current slotid) |ExistingAdjunct| adjuncts/add!))))
  (adjuncts/setup! pool slotid spec)
  (poolctl pool 'metadata 'adjuncts adjuncts))


