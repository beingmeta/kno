;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/flexpools)

(use-module '{reflection texttools regex
	      logger logctl fifo
	      mttools stringfmts opts})

(define %loglevel %notice%)

(module-export! '{flexpool/next flexpool/start 
		  flexpool/open flexpool/def
		  flexpool/alloc flex/make
		  flexpool/split
		  flexpool/ref})

(define flexpool-suffix-regex
  #/[.][0-9][0-9][0-9][.]pool$/)

(define flexpool-suffix
  #("." (opt #((isxdigit) (isxdigit) (isxdigit) ".")) "pool" (eos)))

;;; Opening flexpools
;;; This opens the pool and creates adjuncts (and eventually indexes) as specified
;;; in the pool's metadata.

(define (flexpool/open spec (opts #f) (metadata) (adjuncts {}))
  (when (and (table? spec) (not opts))
    (set! opts spec)
    (set! spec (getopt opts 'pool (getopt opts 'source))))
  (when (not (string? spec)) (irritant spec |InvalidPoolSpec|))
  (if opts
      (set! opts (deep-copy opts))
      (set! opts `#[pool ,spec]))
  (set! metadata (getopt opts 'metadata #f))
  (unless (getopt opts 'type) (opt/set! opts 'type 'bigpool))
  (unless metadata
    (set! metadata #[])
    (opt/set! opts 'metadata metadata))
  (when (getopt opts 'adjuncts)
    (store! metadata 'adjuncts (getopt opts 'adjuncts))
    (set! adjuncts (getopt opts 'adjuncts))
    (drop! (opt/find opts 'adjuncts) 'adjuncts))
  (when (getopt opts 'indexes)
    (store! metadata 'indexes (getopt opts 'indexes))
    (drop! (opt/find opts 'indexes) 'indexes))
  (when (getopt opts 'flexbase)
    (store! metadata 'flexbase (getopt opts 'flexbase))
    (drop! (opt/find opts 'flexbase) 'flexbase))
  (when (getopt opts 'base)
    (store! metadata 'flexbase (getopt opts 'base)))
  (let ((pool (ref-pool spec opts)))
    (unless (or (fail? pool) (adjunct? pool))
      (let ((adjuncts (choice (poolctl pool 'metadata 'adjuncts) adjuncts)))
	(when (exists? adjuncts)
	  (let ((resolved (ref-adjuncts adjuncts pool)))
	    (when (exists? resolved)
	      (loginfo |AddAdjuncts| 
		"For " pool ": "
		"\n\t" adjuncts
		"\n\t" resolved)
	      (do-choices (adjmap resolved)
		(do-choices (slotid (getkeys adjmap))
		  (adjunct! pool slotid (get adjmap slotid)))))))))
    pool))

(define (ref-pool file opts)
  (cond ((file-exists? file)
	 (let ((pool (check-pool (open-pool file opts) opts)))
	   (if (or (adjunct? pool) (testopt opts 'adjunct))
	       pool
	       (use-pool pool))))
	((or (getopt opts 'probe)
	     (not (getopt opts 'create #t)))
	 (fail))
	(else
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
	(current (get-adjuncts pool))
	(root (dirname (pool-source pool))))
    (do-choices (slotid (getkeys adjuncts))
      (if (test current slotid)
	  (begin (unless (same-adjunct? (get adjuncts slotid) (get current current) root)
		   (logwarn |DuplicateAdjuncts|
		     "Using existing " slotid " adjunct for " pool
		     "\n    using    " (get current slotid)
		     "\n    ignoring " (get adjuncts slotid)))
	    (store! new slotid (get current slotid)))
	  (let ((adj (get adjuncts slotid)))
	    (store! new slotid
		    (cond ((or (index? adj) (pool? adj)) adj)
			  ((and (string? adj) (has-suffix adj ".index"))
			   (ref-index (abspath adj root) `#[type hashindex size ,(* cap 7)]))
			  ((and (string? adj) (has-suffix adj ".pool"))
			   (ref-pool (abspath adj root)
				     `#[base ,base capacity ,cap
					type bigpool adjunct #t]))
			  ((not (table? adj))
			   (irritant adj |BadAdjunctSpec|))
			  ((test adj 'pool)
			   (ref-pool (abspath (get adj 'pool) root)
				     (cons `#[base ,base capacity ,cap adjunct #t]
					   adj)))
			  ((test adj 'index)
			   (ref-index (abspath (get adj 'index) root) adj))
			  (else (irritant adj |BadAdjunctSpec|)))))))
    new))

(define (same-adjunct? spec current root)
  (let ((source (cond ((pool? current) (pool-source current))
		      ((index? current) (index-source current))
		      (else #f)))
	(path (cond ((string? spec)  spec) 
		    ((pool? current) (get spec 'pool))
		    ((index? current) (get spec 'index))
		    (else #f))))
    (and source path
	 (equal? (abspath source root) (abspath path root)))))

;;; Flexpool/next

(define (flexpool/next pool (opts #f))
  (try (poolctl pool 'props 'next)
       (next-flexpool pool opts)))

(define (next-flexpool pool (opts #f))
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
	   (top-opts (frame-create #f 
		       'base new-base 'capacity capacity
		       'adjunct (test metadata 'flags 'adjunct)
		       'register (test metadata 'flags 'registered)
		       'metadata new-metadata))
	   (opts (if opts
		     (cons* top-opts opts (getopt metadata 'opts))
		     (cons top-opts (getopt metadata 'opts)))))
      (let ((nextpool (flexpool/open newpath opts)))
	(when (exists? nextpool)
	  (poolctl pool 'props 'next nextpool))
	nextpool))))

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

;;; Flexpool/alloc

(define pool-exhausted-condition '|pool has no more OIDs|)

(define (flexpool/alloc pool (n 1))
  (try (tryif (<= (+ (pool-load pool) n) (pool-capacity pool))
	 (onerror (allocate-oids pool n)
	     (lambda (ex)
	       (if (eq? (error-condition ex) pool-exhausted-condition)
		   (fail)
		   ex))))
       (flexpool/alloc (poolctl pool 'props 'front) n)
       (let ((scan (flexpool/next pool)))
	 (while (and (exists? scan)
		     (> (+ (pool-load scan) n) (pool-capacity scan)))
	   (set! scan (flexpool/next scan)))
	 (tryif (exists? scan)
	   (begin (poolctl pool 'props 'front scan)
	     (flexpool/alloc scan n))))
       (irritant pool |NoMoreOIDs| flexpool/alloc)))

(defambda (flex/make pool . attribs)
  (if (null? attribs)
      (flexpool/alloc pool)
      (if (null? (cdr attribs))
	  (let ((oid (flexpool/alloc pool)))
	    (set-oid-value! oid (car attribs))
	    oid)
	  (if (even? (length attribs))
	      (let ((oid (flexpool/alloc pool))
		    (slotmap (frame-create #f))
		    (scan attribs))
		(set-oid-value! oid slotmap)
		(while (pair? attribs)
		  (add! slotmap (car attribs) (cadr attribs))
		  (set! attribs (cddr attribs))))
	      (irritant attribs |OddArgCount| flex/make
			"Odd number of arguments to FLEX/MAKE")))))

;;; flexpool/def
;;; Creates or uses a flexpool and any subsequent pools

(define (flexpool/ref spec (opts #f))
  (when (and (table? spec) (not opts))
    (set! opts spec)
    (set! spec (getopt opts 'pool (getopt opts 'source #f))))
  (when (not (string? spec)) (irritant spec |InvalidPoolSpec|))
  (let ((basepool (flexpool/open spec opts)))
    (let* ((source (pool-source basepool))
	   (next (glom (textsubst source flexpool-suffix "")
		   ".001.pool"))
	   (count 1))
      (while (file-exists? next)
	(flexpool/open next opts)
	(set! count (1+ count))
	(set! next (glom (textsubst source flexpool-suffix "")
		     "." (padnum count 3 16) ".pool")))
      (lognotice |FlexpoolDef| 
	"Found " count " pools based at " basepool))
    basepool))

;;; Splitting existing pools into smaller flexpools

(define (flexpool/split original capacity (outdir #f) (opts #f))
  (let* ((input (open-pool original (cons #[adjunct #t] opts)))
	 (source (pool-source input))
	 (base (pool-base input))
	 (load (pool-load input))
	 (outdir (or outdir (dirname source)))
	 (basepath (abspath (textsubst (basename source) flexpool-suffix "") outdir))
	 (alloids (pool-vector input))
	 (batchsize (getopt opts 'batchsize (config 'batchsize 250000)))
	 (nthreads (mt/threadcount (getopt opts 'nthreads (config 'nthreads (rusage 'ncpus))))))
    (let ((newcount (+ (quotient load capacity)
		       (if (zero? (remainder load capacity)) 0 1)))
	  (flexbase base))
      (lognotice |FLEXPOOL/SPLIT|
	"Splitting pool " source " into " newcount " pools of " ($num capacity) " OIDs")
      (dotimes (i newcount)
	(let* ((newpath (if (and (= i 0) (not (equal? (dirname source) outdir)))
			    (glom basepath ".pool")
			    (glom basepath "." (padnum i 3 16) ".pool")))
	       (start (* i capacity))
	       (end (min (* (1+ i) capacity) (length alloids)))
	       (opts+ `#[base ,(oid-plus flexbase (* i capacity))
			 batchsize ,batchsize
			 capacity ,capacity 
			 load ,(- end start)
			 flexbase ,flexbase])
	       (newpool (flexpool/open newpath (cons opts+ opts))))
	  (copy-oids input newpool alloids start end batchsize nthreads)
	  (loginfo |FLEXPOOL/SPLIT| "Copied " ($num (- end start)) " OIDs into " newpool)
	  (commit newpool)
	  (swapout newpool)))
      #f)))

(define (copy-oids from to oidvec start limit batchsize nthreads
		   (copy-start (elapsed-time)))
  (let* ((fifo (fifo/make (make-batches oidvec start limit batchsize)
			  `#[fillfn ,fifo/exhausted!]))
	 (counter 0)
	 (countup (slambda (n)
		    (set! counter (+ counter n))
		    (cons counter (elapsed-time copy-start)))))
    (loginfo |CopyOIDs|
      "Copying " ($num (- limit start)) " OIDs in batches of "
      ($num batchsize) " OIDs using " nthreads " threads.")
    (if (and nthreads (> nthreads 1))
	(let ((threads {})
	      (threadcount (min nthreads (fifo/load fifo))))
	  (dotimes (i threadcount)
	    (set+! threads (thread/call copier fifo from to countup)))
	  (thread/wait threads))
	(copier fifo from to countup))
    (swapout from)
    (commit to)
    (swapout to)))

(define (make-batches oidvec start end batchsize)
  (let* ((range (- end start))
	 (n-batches 
	  (+ (quotient range batchsize)
	     (if (zero? (remainder range batchsize)) 0 1)))
	 (batches (make-vector n-batches #f)))
    (loginfo |CopyBatches| 
      "Created " n-batches " of at least " ($num batchsize) " OIDs "
      "from " (oid->string (elt oidvec start)) " "
      "to " (oid->string (elt oidvec end)))
    (dotimes (i n-batches)
      (vector-set! batches i 
		   (qc (elts oidvec 
			     (+ start (* i batchsize))
			     (min (+ start (* (1+ i) batchsize))
				  end)))))
    batches))

(define (copier fifo from to countup)
  (loginfo |CopyThread/Init| 
    "Copying OIDs from " (write (pool-source from)) 
    " to " (write (pool-source to))
    " reading batches from " fifo)
  (let ((batch (fifo/pop fifo))
	(cycle-start (elapsed-time)))
    (while (exists? batch)
      (logdebug |CopyThread/Batch| "Copying " (choice-size batch) " OIDs")
      (pool-prefetch! from batch)
      (lock-oids! batch)
      (do-choices (oid batch)
	(set-oid-value! oid (get from oid)))
      (logdebug |CopyThread/Swapout|
	"Swapping out " (choice-size batch) " OIDs from " from)
      (swapout from batch)
      (logdebug |CopyThread/Commit| "Committing changes to " to)
      (commit to)
      (let ((progress (countup (choice-size batch))))
	(loginfo |CopyThread|
	  "Copied " (choice-size batch) "/" (car progress) " OIDs to " to 
	  " in " (->exact (elapsed-time cycle-start)) "/" (->exact (cdr progress)) 
	  " seconds"))
      (set! batch (fifo/pop fifo))
      (set! cycle-start (elapsed-time)))))


