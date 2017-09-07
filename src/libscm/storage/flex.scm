;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/flex)

(use-module '{ezrecords stringfmts logger texttools})
(use-module '{storage/adjuncts})

(module-export! '{flexpool/start flexpool/ref
		  flex/pools flex/poolcount
		  flex/front flex/zero
		  flexpool/delete!
		  pool/ref index/ref})

(module-export! '{flexpool-suffix flexindex-suffix})

(define-init %loglevel %warn%)

(define flexpool-suffix
  #("." (isxdigit+) ".pool" (eos)))
(define pool-suffix
  #("." (opt #((isxdigit) (isxdigit) (isxdigit) ".")) "pool" (eos)))

(define flexindex-suffix
  #("." (opt #((isxdigit) (isxdigit) (isxdigit) ".")) "index" (eos)))

(defrecord (flexpool mutable opaque)
  prefix base cap (step 0x100000) 
  (opts) (basemap (make-hashtable)) (pools {})
  (front #f) (zero #f) (last #f)
  (lock (make-condvar)))

(define-init flexdata (make-hashtable))
(define-init flexpools (make-hashtable))

(define (flex/zero fp)
  (cond ((flexpool? fp) (flexpool-zero fp))
	((test flexdata fp)
	 (flexpool-zero (get flexdata fp)))
	(else #f)))

(define (flex/last fp)
  (cond ((flexpool? fp) 
	 (or (flexpool-last fp) (flexpool-front fp)
	     (flexpool-start fp)))
	((test flexdata fp)
	 (flex/last (get flexdata fp)))
	(else #f)))

(define (flex/front fp)
  (cond ((flexpool? fp) (flexpool-front fp))
	((test flexdata fp)
	 (flexpool-front (test flexdata fp)))
	(else #f)))

(define (try-file . args)
  (let ((file (apply glom args)))
    (tryif (file-exists? file) file)))

(define (flex/file prefix)
  (set! prefix (textsubst prefix pool-suffix ""))
  (abspath
   (try (try-file prefix ".0000.pool")
	(try-file prefix ".000.pool")
	(try-file prefix ".00.pool")
	(try-file prefix ".0.pool"))))


(define (flexpool/ref prefix (opts #f) (base) (cap) (step))
  (default! base (getopt opts 'base #f))
  (default! cap (getopt opts 'flexcap (getopt opts 'capacity #f)))
  (default! step (getopt opts 'flexstep
			 (getopt opts 'step
				 (getopt opts 'capacity 0x100000))))
  (try (get flexpools (textsubst (realpath prefix) pool-suffix ""))
       (get flexpools (textsubst (abspath prefix)  pool-suffix ""))
       (let ((zero-pool-name (flex/file prefix)))
	 (tryif (exists? zero-pool-name)
	   (let* ((zero-pool (open-pool zero-pool-name #[register #f adjunct #t]))
		  (flexbase (try (poolctl zero-pool 'metadata 'flexbase) base))
		  (flexcap (try (poolctl zero-pool 'metadata 'flexcap) cap))
		  (flexstep (try (poolctl zero-pool 'metadata 'flexstep) step))
		  (flexopts (try (poolctl zero-pool 'metadata 'flexopts) step)))
	     (unique-flexpool prefix flexbase flexcap flexstep 
			      (try (cons flexopts opts) opts)))))))

(define (flexpool/start prefix (opts #f) (base) (cap) (step))
  (default! base (getopt opts 'base #f))
  (default! cap (getopt opts 'flexcap (getopt opts 'capacity #f)))
  (default! step (getopt opts 'flexstep
			 (getopt opts 'step
				 (getopt opts 'capacity 0x100000))))
  (try (get flexpools (textsubst (realpath prefix) pool-suffix ""))
       (get flexpools (textsubst (abspath prefix)  pool-suffix ""))
       (let ((zero-pool-name (flex/file prefix)))
	 (if (exists? zero-pool-name)
	     (let* ((zero-pool (open-pool zero-pool-name #[register #f adjunct #t]))
		    (flexbase (try (poolctl zero-pool 'flexbase) base))
		    (flexcap (try (poolctl zero-pool 'metadata 'flexcap) cap))
		    (flexstep (try (poolctl zero-pool 'metadata 'flexstep) step))
		    (flexopts (try (poolctl zero-pool 'metadata 'flexopts) step)))
	       (unique-flexpool prefix flexbase flexcap flexstep 
				(try (cons flexopts opts) opts)))
	     (unique-flexpool prefix base cap step opts)))))

(define-init unique-flexpool
  ;; Use define-init to avoid duplicate slambdas/locks
  (slambda args (apply init-flexpool args)))

(define (init-flexpool file-prefix base cap (step 0x100000) (opts #f))
  (let* ((prefix (textsubst file-prefix pool-suffix ""))
	 (padlen (get-padlen cap step))
	 (start-file (realpath (glom prefix "." (padnum 0 padlen 16) ".pool")))
	 (make-opts
	  `#[base ,base capacity ,step
	     type ,(getopt opts 'type 'bigpool)
	     metadata ,(make-metadata opts base cap step prefix 0)
	     label ,(glom (basename prefix) "." (make-string padlen #\0))])
	 (start-pool (if (file-exists? start-file)
			 (use-pool start-file opts)
			 (use-pool (make-pool start-file (cons make-opts opts)))))
	 (suffix-pat `#("/" ,(basename prefix)
			"." ,(make-vector padlen '(isxdigit)) ".pool"))
	 (matching-files
	  (pick (getfiles (dirname start-file))
		string-ends-with? suffix-pat))
	 (basemap (make-hashtable))
	 (pools start-pool)
	 (front start-pool)
	 (last start-pool))
    (lognotice |FlexPool| (write prefix) " = " (pool-source start-pool))
    (when (exists? (poolctl start-pool 'metadata 'adjuncts))
      (adjuncts/init! start-pool))
    (store! basemap (pool-base start-pool) start-pool)
    (do-choices (other matching-files)
      (unless (equal? other start-file)
	(let ((pool (use-pool other opts)))
	  (when (exists? (poolctl pool 'metadata 'adjuncts))
	    (adjuncts/init! pool))
	  (lognotice |FlexPool| (write prefix) " + " (pool-source pool))
	  (poolctl pool 'props 'flexbase base)
	  (set+! pools pool)
	  (store! basemap (pool-base pool) pool)
	  (set! front pool)
	  (when (> (oid-offset (pool-base pool))
		   (oid-offset (pool-base last)))
	    (set! last pool)))))
    (let ((state (cons-flexpool prefix base cap step
				opts basemap pools
				front start-pool last))
	  (flex-opts `#[adjunct #t 
			register #t
			allocfn ,flexpool-alloc
			getloadfn ,flexpool-load
			fetchfn ,flexpool-fetch
			cachelevel 0]))
      (let ((pool (make-procpool 
		   prefix base cap (cons flex-opts opts) state
		   (getopt opts 'load 0))))
	(store! flexdata pool state)
	(store! flexdata
		({abspath realpath} 
		 {prefix 
		  (strip-suffix prefix ".pool")
		  (glom (strip-suffix prefix ".pool") ".pool")})
		state)
	(store! flexpools 
		({abspath realpath} 
		 {prefix 
		  (strip-suffix prefix ".pool")
		  (glom (strip-suffix prefix ".pool") ".pool")})
		pool)
	pool))))

(define (flexpool/delete! start-file)
  (when (file-exists? start-file) (remove-file! start-file))
  (do-choices (other (pick (getfiles (dirname (abspath start-file)))
			   basename string-matches?
			   `#(,(strip-suffix start-file ".pool")
			      ,flexpool-suffix)))
    (unless (equal? other start-file)
      (remove-file! other))))

(define (flexpool-next fp)
  (let* ((base (oid-plus (pool-base (flexpool-front fp))
			 (flexpool-step fp)))
	 (flexbase (flexpool-base fp))
	 (step (flexpool-step fp))
	 (prefix (flexpool-prefix fp))
	 (padlen (get-padlen (flexpool-cap fp) step))
	 (serial (quotient (oid-offset base flexbase) step))
	 (opts `#[base ,base capacity ,step
		  metadata ,(make-metadata (flexpool-opts fp) base (flexpool-cap fp) step
					   prefix serial)
		  label ,(glom (basename prefix) "." (padnum serial padlen 16))])
	 (path (realpath (glom prefix "." (padnum serial padlen 16) ".pool"))))
    (let ((new (if (file-exists? path)
		   (use-pool path (cons opts (flexpool-opts fp)))
		   (make-pool path (cons opts (flexpool-opts fp))))))
      (when (exists? (poolctl new 'metadata 'adjuncts)) (adjuncts/setup! new))
      (set-flexpool-pools! fp (choice new (flexpool-pools fp)))
      (set-flexpool-front! fp new)
      new)))

;;; Getting info

(define (flex/pools fp)
  (let ((info (get flexdata fp)))
    (if (and (exists? info) (flexpool? info))
	(flexpool-pools info)
	(irritant fp |UnknownFlexPool| flex/pools))))
(define (flex/poolcount fp)
  (let ((info (get flexdata fp)))
    (if (and (exists? info) (flexpool? info))
	(choice-size (flexpool-pools info))
	(irritant fp |UnknownFlexPool| flex/pools))))

;;; flexpool/def
;;; Creates or uses a flexpool and any subsequent pools

(define (pool/ref spec (opts #f))
  (when (and (table? spec) (not opts))
    (set! opts spec)
    (set! spec (getopt opts 'pool (getopt opts 'source #f))))
  (cond ((pool? spec) spec)
	((not (string? spec)) (irritant spec |InvalidPoolSpec|))
	((or (position #\@ spec) (position #\: spec))
	 (if (getopt opts 'adjunct)
	     (open-pool spec opts)
	     (use-pool spec opts)))
	((file-exists? spec) (ref-pool spec opts))
	((file-exists? (flex/file spec))
	 (flexpool/ref spec opts))
	((file-exists? (glom spec ".pool"))
	 (ref-pool (glom spec ".pool") opts))
	((getopt opts 'create) 
	 (make-pool spec opts))
	(else #f)))

(define (index/ref spec (opts #f))
  (if (index? spec) spec
      (begin
	(when (and (table? spec) (not opts))
	  (set! opts spec)
	  (set! spec (getopt opts 'index (getopt opts 'source #f))))
	(cond ((index? spec) spec)
	      ((not (string? spec)) (irritant spec |InvalidIndexSpec|))
	      ((or (position #\@ spec) (position  #\: spec))
	       (if (getopt opts 'background)
		   (use-index spec opts)
		   (open-index spec opts)))
	      (else
	       (let ((baseindex (ref-index spec opts)))
		 (let* ((source (index-source baseindex))
			(next (glom (textsubst source flexindex-suffix "")
				".001.index"))
			(indexes {})
			(count 1))
		   (while (file-exists? next)
		     (set+! indexes (ref-index next opts))
		     (flexpool/open next opts)
		     (set! count (1+ count))
		     (set! next (glom (textsubst source flexindex-suffix "")
				  "." (padnum count 3 16) ".index")))
		   (lognotice |FlexIndex| "Found " count " indexes based at " baseindex)
		   (indexctl baseindex 'props 'seealso indexes)
		   (indexctl indexes 'props 'base baseindex))
		 baseindex))))))

(define (ref-index path opts)
  (if (file-exists? path)
      (if (getopt opts 'background)
	  (use-index path opts)
	  (open-index path opts))
      (if (getopt opts 'background)
	  (let ((ix (make-index path opts)))
	    (use-index ix))
	  (make-index path opts))))

(define (ref-pool spec opts)
  (let* ((probe (open-pool spec #[adjunct #t register #f]))
	 (metadata (poolctl probe 'metadata)))
    (if (test metadata 'flexbase)
	(flexpool/ref spec opts)
	(if (testopt opts 'adjunct)
	    (open-pool spec opts)
	    (use-pool spec opts)))))

;;; Support functions

(define (get-padlen cap chunk)
  (let ((n-chunks (quotient (or cap 0x100000000) chunk))
	(digits 1)
	(n 16))
    (while (<= n n-chunks)
      (set! digits (1+ digits))
      (set! n (* n 16)))
    digits))
	    
(define (make-metadata opts base cap step prefix serial)
  (let ((metadata (deep-copy (getopt opts 'metadata #[]))))
    (store! metadata 'flexbase base)
    (store! metadata 'flexcap cap)
    (store! metadata 'flexstep step)
    (store! metadata 'flexopts (deep-copy opts))
    (store! metadata 'prefix prefix)
    (store! metadata 'serial prefix)
    metadata))

;;; Handlers

(define (flexpool-alloc pool flexpool (n 1))
  (with-lock (flexpool-lock flexpool)
    (let ((front (flexpool-front flexpool)))
      (if (<= (+ (pool-load front) n) (pool-capacity front))
	  (allocate-oids front n)
	  (let* ((lower (- (pool-capacity front)
			   (pool-load front)))
		 (upper (- n lower)))
	    (choice (tryif (< (pool-load front) (pool-capacity front))
		      (allocate-oids front lower))
		    (allocate-oids (flexpool-next flexpool) upper)))))))

(define (flexpool-fetch pool flexpool oid) (oid-value oid))
(define (flexpool-storen pool flexpool n oidvec valvec) 
  (error |VirtualPool| flexpool-storen
	 "Can't store values in the virtual pool " p))
(define (flexpool-load pool flexpool (front))
  (set! front (flexpool-front flexpool))
  (if front
      (oid-offset (oid-plus (pool-base front) (pool-load front))
		  (pool-base pool))
      0))





