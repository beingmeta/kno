;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/flex)

(use-module '{ezrecords stringfmts logger texttools})
(use-module '{storage/adjuncts storage/filenames})

(module-export! '{flexpool/open flexpool/make 
		  flexpool/ref flexpool/ref
		  flex/pools flex/poolcount
		  flex/front flex/zero
		  flexpool/delete!
		  flex/pool flex/index flex/db
		  pool/ref index/ref db/ref
		  flex/metadata})

(module-export! '{flexpool-suffix flexindex-suffix})

(define-init %loglevel %notice%)

(define flexpool-suffix
  #("." (isxdigit+) ".pool" (eos)))
(define pool-suffix
  #("." (opt #((isxdigit) (isxdigit) (isxdigit) ".")) "pool" (eos)))

(define flexindex-suffix
  #("." (opt #((isxdigit) (isxdigit) (isxdigit) ".")) "index" (eos)))

(define (flexpool->string f)
  (stringout "#<FLEXPOOL " (flexpool-filename f) " " 
    (oid->string (flexpool-base f)) "+" 
    (flex/poolcount f) "*" (flexpool-step f) " "
    (flex/load f) (flexpool-capacity f) ">"))

(defrecord (flexpool mutable opaque `(stringfn . flexpool->string))
  filename prefix base capacity (step 0x100000) 
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
	     (flexpool-zero fp)))
	((test flexdata fp)
	 (flex/last (get flexdata fp)))
	(else #f)))

(define (flex/front fp)
  (cond ((flexpool? fp) (flexpool-front fp))
	((test flexdata fp)
	 (flexpool-front (test flexdata fp)))
	(else #f)))

(define (flexpool/find filename (prefix #f) (abs) (real))
  (set! abs (abspath filename))
  (set! real (realpath filename))
  (try (get flexpools abs)
       (get flexpools real)
       (get flexpools (strip-suffix abs ".flexpool"))
       (get flexpools (strip-suffix real ".flexpool"))
       (tryif prefix
	 (get flexpools (mkpath (dirname real) prefix)))
       (tryif prefix
	 (get flexpools (mkpath (dirname abs) prefix)))))

(define (flexpool/open filename (opts #f))
  (unless (has-prefix filename "/")
    (set! filename (abspath filename)))
  (try (flexpool/find filename)
       (tryif (and (file-exists? filename) (not (file-directory? filename)))
	 (let* ((def (file->dtype filename))
		(opts (if opts (cons opts def) def)))
	   (let ((prefix (getopt def 'prefix))
		 (base (getopt def 'base))
		 (step (getopt def 'step))
		 (cap (getopt def 'capacity)))
	     (if (and (exists? prefix) (exists? base) (exists? cap) (exists? step))
		 (try (get flexpools (mkpath (dirname (abspath filename)) prefix))
		      (get flexpools (mkpath (dirname (realpath filename)) prefix))
		      (unique-flexpool (realpath filename) prefix base cap step opts))
		 (irritant opts |BadFlexpoolData|)))))))

(define (make-flexpool filename opts)
  (let ((base (getopt opts 'base))
	(cap (getopt opts 'capacity))
	(prefix (getopt opts 'prefix (basename filename)))
	(step (getopt opts 'step 0x100000)))
    (if (and (exists? prefix) (exists? base) (exists? cap) (exists? step))
	(let ((saved (frame-create #f
		       'created (timestamp)
		       'init (config 'sessionid)
		       'base base 'capacity cap 'step step
		       'prefix prefix
		       'load (getopt opts 'load {})
		       'partitions (getopt opts 'partitions {})
		       'metadata (getopt opts 'metadata {}))))
	  (dtype->file saved filename)
	  (unique-flexpool (if (readlink filename)
			       (abspath filename)
			       (realpath filename)) 
			   prefix base cap step opts))
	(irritant opts |IncompleteFlexpoolDef|))))

(define (flexpool/make filename opts)
  (unless (has-prefix filename "/")
    (set! filename (abspath filename)))
  (let ((existing (flexpool/find filename)))
    (if (exists? existing)
	(irritant filename |AlreadyExists|)
	(make-flexpool filename opts))))

(define (flexpool/ref filename opts)
  (unless (has-prefix filename "/")
    (set! filename (abspath filename)))
  (try (flexpool/open filename)
       (make-flexpool filename opts)))

(define unique-flexpool ;; -init
  ;; Use define-init to avoid duplicate slambdas/locks when reloading
  (slambda (filename prefix . args)
    (try (flexpool/find filename prefix)
	 (apply init-flexpool filename prefix args))))

(define (init-flexpool filename file-prefix base cap (step 0x100000) (opts #f))
  (unless (has-prefix filename "/")
    (set! filename (abspath filename)))
  (let* ((prefix (textsubst file-prefix pool-suffix ""))
	 (padlen (get-padlen cap step))
	 (start-file (realpath (mkpath (dirname filename) (glom prefix "." (padnum 0 padlen 16) ".pool"))))
	 (partition-opts (cons (if (getopt opts 'adjunct)
				   `#[adjuncts {} adjunct ,(getopt opts 'adjunct)]
				   #[adjuncts {}])
			       opts))
	 (start-pool (tryif (file-exists? start-file)
		       (use-pool start-file partition-opts)))
	 (suffix-pat `#("/" ,(basename prefix)
			"." ,(make-vector padlen '(isxdigit)) ".pool"))
	 (matching-files (pick (getfiles (dirname start-file))
			       string-ends-with? suffix-pat))
	 (adjopts (getopt opts 'adjopts
			  (frame-create #f
			    'adjunct #t
			    'adjuncts #f
			    'cachelevel (getopt opts 'cachelevel {})
			    'loglevel (getopt opts 'loglevel {})
			    'readonly (getopt opts 'readonly {}))))
	 (basemap (make-hashtable))
	 (load (getopt opts 'load))
	 (pools start-pool)
	 (front start-pool)
	 (last start-pool))

    (unless (exists? start-pool)
      (let ((zero-opts
	     `#[base ,base
		capacity ,step
		load ,(min load step)
		type ,(getopt opts 'type 'bigpool)
		metadata ,(make-metadata (dirname filename) opts base cap step prefix 0)
		label ,(glom (basename prefix) "." (make-string padlen #\0))]))
	(set! start-pool (use-pool (make-pool start-file (cons zero-opts opts))))
	(lognotice |NewPool| "Created initial pool partition " (write start-file))
	(logdebug |NewPoolOpts| "For " (write start-file) "\n" (pprint zero-opts))
	(set! pools start-pool)
	(set! front start-pool)
	(set! last start-pool)))

    (loginfo |PoolPartition| "= " (pool-source start-pool))

    (when (exists? (poolctl start-pool 'metadata 'adjuncts))
      (if (getopt opts 'build)
	  (adjuncts/setup! start-pool (poolctl start-pool 'metadata 'adjuncts) adjopts)
	  (adjuncts/init! start-pool (poolctl start-pool 'metadata 'adjuncts) adjopts)))
    (store! basemap (pool-base start-pool) start-pool)

    (do-choices (other matching-files)
      (unless (equal? other start-file)
	(let ((pool (use-pool other partition-opts)))
	  (when (exists? (poolctl pool 'metadata 'adjuncts))
	    (if (getopt opts 'build)
		(adjuncts/setup! pool (poolctl pool 'metadata 'adjuncts) adjopts)
		(adjuncts/init! pool (poolctl pool 'metadata 'adjuncts) adjopts)))
	  (loginfo |PoolPartition| "+ " (pool-source pool))
	  (poolctl pool 'props 'flexbase base)
	  (set+! pools pool)
	  (store! basemap (pool-base pool) pool)
	  (when (> (oid-offset (pool-base pool)) (oid-offset (pool-base last)))
	    (set! front pool)
	    (set! last pool)))))

    (when (testopt opts 'load)
      (let ((load (getopt opts 'load 0))
	    (serial 1))
	(while (> load (* serial step))
	  (unless (exists? (getpool (oid-plus base (* serial step))))
	    (let* ((filebase (glom prefix "." (padnum serial padlen 16) ".pool"))
		   (file (mkpath (dirname filename) filebase))
		   (adjusted-load (- load (* serial step)))
		   (make-opts
		    `#[base ,(oid-plus base (* serial step)) 
		       load ,(min adjusted-load step)
		       capacity ,step
		       type ,(if (testopt opts 'type 'flexpool) 
				 'bigpool
				 (getopt opts 'type 'bigpool))
		       metadata ,(make-metadata (dirname filename) opts base cap step prefix serial)
		       label ,(glom (basename prefix) "." (padnum serial padlen 16))])
		   (pool (make-pool file (cons make-opts partition-opts))))
	      (lognotice |NewPool| "Created pool partition " (write file))
	      (logdebug |NewPoolOpts|
		"For " (write file) "\n" (pprint make-opts))
	      (when (exists? (poolctl pool 'metadata 'adjuncts))
		(if (getopt opts 'build)
		    (adjuncts/setup! pool (poolctl pool 'metadata 'adjuncts) opts)
		    (adjuncts/init! pool (poolctl pool 'metadata 'adjuncts) opts)))
	      (unless (getopt opts 'adjunct) (use-pool pool partition-opts))
	      (set+! pools pool)
	      (loginfo |FlexPool| (write prefix) " + " (pool-source pool))
	      (poolctl pool 'props 'flexbase base)
	      (set+! pools pool)
	      (when (> (oid-offset (pool-base pool))
		       (oid-offset (pool-base last)))
		(set! front pool)
		(set! last pool))))
	  (set! serial (1+ serial)))))
				
    (let ((state (cons-flexpool filename prefix base cap step
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
	(lognotice |NewFlexpool|
	  "Using " (choice-size pools) " partitions for " pool)
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

(define (flexpool/delete! file (filebase))
  (set! file (abspath file))
  (if (and (file-exists? file) (has-suffix file ".flexpool"))
      (let ((info (file->dtype file)))
	(set! filebase (mkpath (dirname file) (get info 'prefix)))
	(remove-file! file))
      (set! filebase (textsubst file flexpool-suffix "")))
  (do-choices (other (pick (pick (getfiles (dirname filebase))
				 string-starts-with? filebase)
			   has-suffix ".pool"))
    (when (file-exists? other)
      (remove-file! other))))

(define (flexpool-next fp)
  (let* ((base (oid-plus (pool-base (flexpool-front fp))
			 (flexpool-step fp)))
	 (flexbase (flexpool-base fp))
	 (step (flexpool-step fp))
	 (prefix (flexpool-prefix fp))
	 (padlen (get-padlen (flexpool-capacity fp) step))
	 (serial (quotient (oid-offset base flexbase) step))
	 (opts `#[base ,base capacity ,step
		  metadata ,(make-metadata (dirname (flexpool-filename fp)) 
					   (flexpool-opts fp) 
					   base (flexpool-capacity fp) step
					   prefix serial)
		  label ,(glom (basename prefix) "." (padnum serial padlen 16))])
	 (path (realpath (glom prefix "." (padnum serial padlen 16) ".pool"))))
    (let ((new (if (file-exists? path)
		   (use-pool path (cons opts (flexpool-opts fp)))
		   (make-pool path (cons opts (flexpool-opts fp))))))
      (when (exists? (poolctl new 'metadata 'adjuncts)) 
	(adjuncts/setup! new))
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
  (let ((info (if (flexpool? fp) fp (get flexdata fp))))
    (if (and (exists? info) (flexpool? info))
	(choice-size (flexpool-pools info))
	(irritant fp |UnknownFlexPool| flex/pools))))
(define (flex/load flexpool (front))
  (set! front (flexpool-front flexpool))
  (if front
      (oid-offset (oid-plus (pool-base front) (pool-load front))
		  (flexpool-base flexpool))
      0))

;;; flexpool/def
;;; Creates or uses a flexpool and any subsequent pools

(define (flex/pool spec (opts #f))
  (when (and (table? spec) (not (pool? spec)) (not opts))
    (set! opts spec)
    (set! spec (getopt opts 'pool (getopt opts 'source #f))))
  (cond ((pool? spec) spec)
	((not (string? spec)) (irritant spec |InvalidPoolSpec|))
	((or (position #\@ spec) (position #\: spec))
	 (if (getopt opts 'adjunct)
	     (open-pool spec opts)
	     (use-pool spec opts)))
	((and (file-exists? spec) (not (file-directory? spec))) 
	 (flexpool/open spec opts))
	((file-exists? (glom spec ".flexpool"))
	 (flexpool/open (glom spec ".flexpool") opts))
	((or (getopt opts 'create) (getopt opts 'build)) 
	 (flexpool/make spec opts))
	(else #f)))
(define pool/ref flex/pool)

(define (flex/index spec (opts #f))
  (when (and (table? spec) (not (index? spec)) (not opts))
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
	       (set! count (1+ count))
	       (set! next (glom (textsubst source flexindex-suffix "")
			    "." (padnum count 3 16) ".index")))
	     (lognotice |FlexIndex| "Found " count " indexes based at " baseindex)
	     (indexctl baseindex 'props 'seealso indexes)
	     (indexctl indexes 'props 'base baseindex))
	   baseindex))))
(define index/ref flex/index)

(define (ref-index path opts)
  (if (file-exists? path)
      (if (getopt opts 'background)
	  (use-index path opts)
	  (open-index path opts))
      (if (getopt opts 'background)
	  (let ((ix (make-index path opts)))
	    (use-index ix))
	  (make-index path opts))))

(define (flex/db spec opts)
  (if (or (pool? spec) (index? spec) (hashtable? spec))
      spec
      (let* ((source (if (string? spec) spec
			 (try (get spec 'pool) 
			      (get spec 'index)
			      (get spec 'source)
			      #f)))
	     (xopts (if (table? spec) (cons spec opts) opts)))
	(cond ((not source) {})
	      ((or (position #\: source) (position #\@ source))
	       (cond ((or (testopt xopts 'type 'index)
			  (testopt xopts 'index))
		      (if (testopt xopts 'background)
			  (use-index source)
			  (open-index source)))
		     ((or (testopt xopts 'type 'pool)
			  (testopt xopts 'pool))
		      (if (testopt xopts 'adjunct)
			  (open-pool source xopts)
			  (use-pool source xopts)))
		     (else {})))
	      ((or (has-suffix source ".pool")
		   (testopt xopts 'pool)
		   (testopt xopts 'type 'pool))
	       (if (getopt xopts 'adjunct)
		   (open-pool source xopts)
		   (use-pool source xopts)))
	      ((or (has-suffix source ".flexpool")
		   (testopt xopts 'flexpool)
		   (testopt xopts 'type 'flexpool))
	       (flex/pool source xopts))
	      ((or (has-suffix source ".index")
		   (testopt xopts 'index)
		   (testopt xopts 'type 'index))
	       (flex/index source xopts))
	      ((exists? (flex/file source "index"))
	       (flex/index source xopts))
	      (else {})))))
(define db/ref flex/db)

;;; Support functions

(define (get-padlen cap chunk)
  (let ((n-chunks (quotient (or cap 0x100000000) chunk))
	(digits 1)
	(n 16))
    (while (<= n n-chunks)
      (set! digits (1+ digits))
      (set! n (* n 16)))
    digits))
	    
(define (make-metadata flexdir opts base cap step prefix serial)
  (let* ((metadata (deep-copy (getopt opts 'metadata #[])))
	 (padlen (get-padlen cap step))
	 (adjuncts (get metadata 'adjuncts))
	 (adjslots (getkeys adjuncts)))
    (when (test metadata 'type 'flexpool) (drop! metadata 'type))
    (store! metadata 'flexbase base)
    (store! metadata 'flexcap cap)
    (store! metadata 'flexstep step)
    (store! metadata 'flexopts (deep-copy opts))
    (store! metadata 'prefix prefix)
    (store! metadata 'serial serial)
    (when (exists? adjslots)
      (let ((converted (frame-create #f)))
	(do-choices (slot adjslots)
	  (let ((spec (get adjuncts slot)))
	    (if (or (and (string? spec) (has-suffix spec ".flexpool"))
		    (and (table? spec) (test spec 'pool) 
			 (string? (get spec 'pool))
			 (or (has-suffix (get spec 'pool) ".flexpool")
			     (test spec 'step)))
		    (and (table? spec) (test spec 'flexpool) 
			 (string? (get spec 'flexpool)))
		    (and (table? spec) (test spec 'pool) (string? (get spec 'pool))
			 (test spec 'prefix)))
		(let* ((copied (deep-copy spec))
		       (name (getopt spec 'prefix
				     (strip-suffix
				      (if (string? spec) spec 
					  (try (get spec 'flexpool) 
					       (get spec 'pool)
					       (get spec 'source)))
				      {".pool" ".flexpool"})))
		       (label (glom (getopt opts 'label (basename name))
				 "." (padnum serial padlen 16)))
		       (adjbase (glom name "." (padnum serial padlen 16) ".pool"))
		       (adjpath (mkpath flexdir adjbase)))
		  (if (string? copied)
		      (set! copied `#[pool ,adjpath])
		      (store! copied 'pool adjpath))
		  (store! copied 'label label)
		  (store! copied 'adjunct slot)
		  (store! copied 'metadata `#[adjunct ,slot label ,label])
		  (store! copied 'base (oid-plus base (* serial step)))
		  (store! copied 'capacity step)
		  ;; (%watch "CONVERTED" "spec" spec "converted" copied)
		  (store! converted slot copied))
		(store! converted slot spec))))
	(store! metadata 'adjuncts converted)))
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
