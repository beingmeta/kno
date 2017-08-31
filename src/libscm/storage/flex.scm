(in-module 'storage/flex)

(use-module '{ezrecords stringfmts logger texttools})

(module-export! '{flexpool/start flexpool/ref
		  flex/pools flex/poolcount
		  flexpool/delete!})

(define-init %loglevel %notice%)

(defrecord (flexpool mutable opaque)
  prefix base cap (step 0x100000) 
  (opts) (basemap (make-hashtable))
  (pools {}) (front #f) (zero #f)
  (lock (make-condvar)))

(define-init flexdata (make-hashtable))
(define-init flexpools (make-hashtable))

(define (flexpool/start prefix (opts #f) (base) (cap) (step))
  (default! base (getopt opts 'base #f))
  (default! cap (getopt opts 'capacity #f))
  (default! step (getopt opts 'step  0x100000))
  (try (get flexpools (realpath (strip-suffix prefix ".pool")))
       (get flexpools (realpath prefix))
       (get flexpools (abspath (strip-suffix prefix ".pool")))
       (get flexpools (abspath prefix))
       (tryif (and base cap)
	 (unique-flexpool prefix base cap step opts))
       (let* ((base-pool (use-pool prefix))
	      (flexbase (poolctl base-pool 'metadata 'flexbase))
	      (flexcap (poolctl base-pool 'metadata 'flexcap))
	      (flexstep (poolctl base-pool 'metadata 'flexstep))
	      (flexopts (poolctl base-pool 'metadata 'flexopts)))
	 (unique-flexpool prefix flexbase flexcap flexstep flexopts))))

(define flexpool-suffix #("." (isxdigit+) ".pool"))

(define (flexpool/ref prefix (opts #f))
  (try (get flexpools (realpath (strip-suffix prefix ".pool")))
       (get flexpools (realpath prefix))
       (get flexpools (abspath (strip-suffix prefix ".pool")))
       (get flexpools (abspath prefix))
       (let* ((base-pool (use-pool prefix))
	      (flexbase (poolctl base-pool 'metadata 'flexbase))
	      (flexcap (poolctl base-pool 'metadata 'flexcap))
	      (flexstep (poolctl base-pool 'metadata 'flexstep))
	      (flexopts (poolctl base-pool 'metadata 'flexopts)))
	 (unique-flexpool prefix flexbase flexcap flexstep flexopts))))

(define-init unique-flexpool
  (slambda args (apply init-flexpool args)))

(define (init-flexpool file base cap (step 0x100000) (opts #f))
  (let* ((prefix (strip-suffix file ".pool"))
	 (padlen (get-padlen cap step))
	 (start-file (realpath (glom prefix ".pool")))
	 (make-opts
	  `#[base ,base capacity ,step
	     type ,(getopt opts 'type 'bigpool)
	     metadata ,(make-metadata opts base cap step prefix 0)
	     label ,(glom (basename prefix) ".000")])
	 (start-pool (if (file-exists? start-file)
			 (use-pool start-file opts)
			 (make-pool start-file (cons make-opts opts))))
	 (file-pat `#(,prefix ,flexpool-suffix))
	 (matching-files
	  (pick (getfiles (dirname start-file))
		basename string-matches? file-pat))
	 (basemap (make-hashtable))
	 (pools start-pool)
	 (front start-pool))
    (lognotice |FlexPool| (write prefix) " = " start-pool)
    (store! basemap (pool-base start-pool) start-pool)
    (do-choices (other matching-files)
      (unless (equal? other start-file)
	(let ((pool (use-pool other opts)))
	  (lognotice |FlexPool| (write prefix) " + " pool)
	  (set+! pools pool)
	  (store! basemap (pool-base pool) pool)
	  (set! front pool))))
    (let ((state (cons-flexpool prefix base cap step
				opts basemap
				pools front start-pool))
	  (flex-opts `#[adjunct #t
			allocfn ,flexpool-alloc
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
  (remove-file! start-file)
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

;;; Support functions

(define (get-padlen cap chunk)
  (let ((n-chunks (quotient cap chunk))
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



