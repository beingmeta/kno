;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/flexpools)

(use-module '{reflection texttools regex
	      logger logctl 
	      mttools stringfmts opts})

(define %loglevel %info%)

(module-export! '{flexpool/next flexpool/start flexpool/open})

(define (flexpool/open spec (opts #f) (metadata))
  (when (and (table? spec) (not opts))
    (set! opts spec)
    (set! spec (getopt opts 'pool (getopt opts 'source))))
  (when (not (string? spec)) (irritant spec |InvalidPoolSpec|))
  (set! opts (deep-copy opts))
  (set! metadata (getopt opts 'metadata #f))
  (when (or (testopt opts 'adjuncts) (testopt opts 'indexes))
    (unless metadata
      (set! metadata #[])
      (store! opts 'metadata metadata))
    (when (test opts 'adjuncts)
      (store! metadata 'adjuncts (getopt opts 'adjuncts))
      (drop! opts 'adjuncts))
    (when (test opts 'indexes)
      (store! metadata 'indexes (getopt opts 'indexes))
      (drop! opts 'indexes)))
  (let* ((pool (ref-pool spec opts))
	 (adjuncts (poolctl pool 'metadata 'adjuncts))
	 (indexes (poolctl pool 'metadata 'indexes)))
    (when (exists? adjuncts)
      (let ((current (get-adjuncts pool)))
	(let ((resolved (ref-adjuncts adjuncts pool)))
	  (loginfo |AddAdjuncts| 
	    "For " pool ": "
	    "\n\t" adjuncts
	    "\n\t" resolved)
	  (do-choices (slotid (getkeys resolved))
	    (adjunct! pool slotid (get resolved slotid))))))
    ;; TODO: Fill this in
    (when (exists? indexes))
    pool))

(define (ref-pool file opts)
  (if (file-exists? file)
      (let ((pool (check-pool (open-pool file opts) opts)))
	(if (or (adjunct? pool) (testopt opts 'adjunct))
	    pool
	    (use-pool pool)))
      (begin
	(unless (testopt opts 'base)
	  (irritant opts |NoPoolBase| "For creating " file))
	(unless (testopt opts 'capacity)
	  (irritant opts |NoPoolCapacity| "For creating " file))
	(unless (testopt opts 'type)
	  (irritant opts |NoPoolType| "For creating " file))
	(let* ((more-opts (frame-create #f 
			    'adjuncts (getopt opts 'adjuncts)
			    'indexes (getopt opts 'indexes)))
	       (pool (make-pool file (cons more-opts opts))))
	  (lognotice |CreatedPool| pool)
	  (if (or (adjunct? pool) (testopt opts 'adjunct))
	      pool
	      (use-pool pool))))))

(define (check-pool pool opts)
  (let ((opt-base (getopt opts 'base #f))
	(opt-capacity (getopt opts 'capacity #f))
	(metadata (poolctl pool 'metadata)))
    (when (and opt-base (not (eq? opt-base (pool-base pool))))
      (irritant pool |InconsistentBaseOID|
	"Pool's base OID (" (oid->string (pool-base pool)) ") "
	"is not the same as specified in opts (" opt-base ")"))
    (when (and opt-capacity (not (eq? opt-capacity (pool-capacity pool))))
      (irritant pool |InconsistentCapacity|
	"Pool's capacity (" (pool-capacity pool) ") "
	"is not the same as specified in opts (" opt-capacity ")"))
    pool))
    
(define (ref-index file opts)
  (if (file-exists? file)
      (open-index file opts)
      (make-index file opts)))

(define (ref-adjuncts adjuncts pool)
  (let ((new (frame-create #f))
	(base (and pool (pool-base pool)))
	(cap (and pool (pool-capacity pool)))
	(current (get-adjuncts pool)))
    (do-choices (slotid (getkeys adjuncts))
      (if (test current slotid)
	  (logwarn |DuplicateAdjuncts|
	    "The adjunct for " slotid "(" (pool-id pool) ") "
	    "is already to " (get current slotid) ", ignoring "
	    "specification " (get adjuncts slotid))
	  (let ((adj (get adjuncts slotid)))
	    (store! new slotid
		    (cond ((or (index? adj) (pool? adj)) adj)
			  ((and (string? adj) (has-suffix adj ".index"))
			   (ref-index adj `#[type hashindex size ,(* cap 7)]))
			  ((and (string? adj) (has-suffix adj ".pool"))
			   (ref-pool adj `#[base ,base capacity ,cap
					    type bigpool adjunct #t]))
			  ((not (table? adj))
			   (irritant adj |BadAdjunctSpec|))
			  ((test adj 'pool)
			   (ref-pool (get adj 'pool)
				     (cons `#[base ,base capacity ,cap adjunct #t]
					   adj)))
			  ((test adj 'index)
			   (ref-index (get adj 'index) adj))
			  (else (irritant adj |BadAdjunctSpec|)))))))
    new))

;;; Flexpool/next

(define flexpool-suffix-regex
  #/[.][0-9][0-9][0-9][.]pool$/)

(define flexpool-suffix
  #("." (opt #((isxdigit) (isxdigit) (isxdigit) ".")) "pool" (eos)))

(define (flexpool/next pool)
  (let ((base (pool-base pool))
	(capacity (pool-capacity pool))
	(metadata (poolctl pool 'metadata))
	(source (pool-source pool)))
    (let* ((flexbase (try (get metadata 'flexbase) base))
	   (new-base (oid-plus base capacity))
	   (flexoff (quotient (oid-offset new-base flexbase) capacity))
	   (basepath (textsubst source flexpool-suffix ""))
	   (new-suffix (glom "." (padnum flexoff 3 16) ".pool"))
	   (newpath (glom basepath new-suffix))
	   (new-metadata
	    (frame-create #f 
	      'flexbase (getopt metadata 'flexbase base)
	      'indexes (get metadata 'indexes)
	      'adjuncts (next-adjuncts (get metadata 'adjuncts) new-suffix)))
	   (opts (cons (frame-create #f 
			 'base new-base 'capacity capacity
			 'adjunct (test metadata 'flags 'adjunct)
			 'register (test metadata 'flags 'registered)
			 'metadata new-metadata)
		       (getopt metadata 'opts))))
      (flexpool/open newpath opts))))

(define (next-adjuncts adjuncts new-suffix)
  (let ((new-adjuncts (frame-create #f)))
    (do-choices (slotid (getkeys adjuncts))
      (let ((adj (get adjuncts slotid)))
	(store! new-adjuncts slotid
		(cond ((and (string? adj) (has-suffix adj ".pool"))
		       (glom (textsubst adj flexpool-suffix "") new-suffix))
		      ((and (table? adj) (test adj 'pool))
		       (let ((new-spec (deep-copy adj))
			     (source (get adj 'pool))
			     (new-path
			       (glom (textsubst (get adj 'pool) flexpool-suffix "")
				 new-suffix)))
			 (store! new-spec 'pool new-path)
			 new-spec))
		      (else adj)))))
    new-adjuncts))

(define (flexpool/start basename base capacity (opts #f))
  (let* ((flexbase (try (getopt opts 'flexbase {})
			base))
	 (flexoff (remainder (oid-lo base) flexbase))
	 (basepath (if (regex/search flexpool-suffix-regex basename)
		       (slice basename 0 -8)
		       (strip-suffix basename ".pool")))
	 (newpath (glom basepath "." (padnum flexoff 3) ".pool"))
	 (newopts (if opts
		      (cons`#[flexbase ,flexbase] opts)
		      `#[flexbase ,flexbase]))
	 (init-adjuncts (getopt opts 'adjuncts)))
    (let ((pool
	   (if (file-exists? newpath)
	       (check-flexpool (open-pool newpath (cons #[register #t] newopts))
			       base capacity)
	       (make-flexpool newpath base capacity #f newopts))))
      (unless (getopt opts 'adjunct #f)
	(when init-adjuncts
	  (do-choices (slotid (getkeys init-adjuncts))
	    (let ((adjunct (flexpool/start (get init-adjuncts slotid)
					   base capacity
					   (cons #[register #t] newopts))))))))
      pool)))

(define (make-flexpool filename base capacity (prev #f) (newopts #f) (metadata))
  (default! metadata (if prev (poolctl prev 'metadata) #f))
  (let* ((flexbase (getopt newopts 'flexbase
			   (getopt metadata 'flexbase 
				   (if prev (pool-base prev) base))))
	 (type (getopt newopts 'type
		       (try (get metadata 'type) "bigpool")))
	 (adjuncts (getopt metadata 'adjuncts {}))
	 (opts `#[type ,type base ,base capacity ,capacity
		  metadata  #[flexbase ,flexbase]]))
    (when (getopt newopts 'adjunct (test metadata 'flags 'adjunct))
      (store! opts 'adjunct #t))
    (when (getopt newopts 'register (test metadata 'flags 'registered))
      (store! opts 'register #t))
    (set! opts (merge-opts opts (getopt metadata 'opts)))
    (let* ((new-pool (make-pool filename opts))
	   (new-metadata (poolctl new-pool 'metadata))
	   (new-adjuncts (getopt new-metadata 'adjuncts {})))
      (unless (getopt new-metadata 'adjunct #f)
	(do-choices (new-adjunct (getkeys new-adjuncts))
	  (if (file-exists? (get new-adjuncts new-adjunct))
	      )
	(when metadata
	  (let ((adjuncts (get metadata 'adjuncts))
		(adjunct-opts (cons #[adjunct #t] opts)))
	    (do-choices (slotid (getkeys adjuncts))
	      (when (pool? (get adjuncts slotid))
		(let ((adjunct (flexpool/next (get adjuncts slotid))))
		  (use-adjunct new-pool slotid adjunct))))))))
      new-pool)))

(define (merge-opts new existing)
  (cond ((not new) existing)
	((not existing) new)
	(else (let ((merged #[])
		    (newkeys (getkeys new))
		    (oldkeys (getkeys exisiting)))
		(do-choices (key newkeys)
		  (store! merged key (get new key)))
		(do-choices (key (difference oldkeys newkeys))
		  (store! merged key (get existing key)))
		merged))))

(define (check-flexpool pool base capacity)
  "Checks for whether a new flexpool member is consistent with the flexpool."
  (if (eq? (pool-base pool) base)
      (if (= (pool-capacity pool) capacity)
	  pool
	  (irritant pool |BadFlexpool| 
	    "The capacity of " pool " is " (pool-capacity pool) ", "
	    "not " capacity))
      (irritant pool |BadFlexpool| 
	"The base OID of " pool " is " (oid->string (pool-base pool)) ", "
	"not " (oid->string base))))





