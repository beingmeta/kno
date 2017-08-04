;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/flexpools)

(use-module '{reflection logger logctl mttools stringfmts regex})

(define %loglevel %info%)

(module-export! '{flexpool/next flexpool/start})

(define flexpool-suffix
  #/[.][0-9][0-9][0-9][.]pool$/)

(define (flexpool/next pool)
  (let ((base (pool-base pool))
	(capacity (pool-capacity pool))
	(metadata (poolctl pool 'metadata))
	(source (pool-source pool)))
    (let* ((flexbase (try (get metadata 'flexbase) base))
	   (new-base (oid-plus base capacity))
	   (flexoff (remainder (oid-lo new-base) flexbase))
	   (basepath (if (regex/search flexpool-suffix source)
			 (slice source 0 -8)
			 (strip-suffix source ".pool")))
	   (newpath (glom basepath "." (padnum flexoff 3) ".pool"))
	   (opts (merge-opts `#[base ,new-base capacity ,capacity
				adjunct ,(get metadata 'adjunct)
				register ,(test metadata 'flags 'registered)]
			     (getopt metadata 'opts))))
      (if (file-exists? newpath)
	  (open-flexpool newpath opts pool)
	  (make-flexpool newpath new-base capacity pool metadata
			 (getopt metadata 'opts))))))

(define (open-flexpool file opts (prev #f) (base) (capacity))
  (let ((pool (open-pool file opts)))
    (default! base (getopt opts 'base (pool-base pool)))
    (default! capacity (getopt opts 'capacity (pool-capacity pool)))
    (cond ((not (eq? (pool-base pool) base))
	   (irritant pool |BadFlexpool| 
	     "The base OID of " pool " is " (oid->string (pool-base pool)) ", "
	     "not " (oid->string base)))
	  ((not (= (pool-capacity pool) capacity))
	   (irritant pool |BadFlexpool| 
	     "The capacity of " pool " is " (pool-capacity pool) ", "
	     "not " capacity)))))

(define (flexpool/start basename base capacity (opts #f))
  (let* ((flexbase (try (getopt opts 'flexbase {})
			base))
	 (flexoff (remainder (oid-lo base) flexbase))
	 (basepath (if (regex/search flexpool-suffix basename)
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





