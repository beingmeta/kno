;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/flexpools)

(use-module '{ezrecords stringfmts logger varconfig fifo texttools})
(use-module '{storage/adjuncts storage/filenames})
(use-module '{storage/flex})

(module-export! '{flexpool/open flexpool/make 
		  flexpool/ref flexpool/record 
		  flexpool/zero flexpool/front flexpool/last flexpool/info
		  flexpool/partitions flexpool/partcount
		  flexpool/delete!
		  flexpool/adjunct!
		  flexpool/padlen
		  flex/zero flex/front flex/last flex/info})

(module-export! '{flexpool-suffix})

(define-init %loglevel %notify%)

(module-export! 'flexpool/split)

(define-init maxmem (quotient (rusage 'physical-memory) 2))
(varconfig! splitpool:maxmem maxmem)

(define-init thread-spacing 2)
(varconfig! splitpool:spacing thread-spacing config:boolean+)

(define-init default-partsize 0x100000)
(varconfig! flexpool:partsize default-partsize)

(define-init default-batchsize 0x40000)
(varconfig! splitpool:batchsize default-batchsize)

(define partition-suffix
  #("." (isxdigit+) ".pool" (eos)))
(define pool-suffix
  #("." (opt #((isxdigit+) ".")) "pool" (eos)))
	    
(define (get-partition-prefix filename)
  (let ((stripped (textsubst filename pool-suffix "")))
    (if (position #\/ stripped)
	stripped
	(mkpath (textsubst stripped #("." (isalnum+) (eos)) "")
		stripped))))

;;; Flexpool records

(define (flexpool->string f)
  (stringout "#<FLEXPOOL " (flexpool-filename f) " " 
    (oid->string (flexpool-base f)) "+" 
    (flexpool/partcount f) "*" (flexpool-partsize f) " "
    (flex/load f) (flexpool-capacity f) ">"))

(defrecord (flexpool mutable opaque `(stringfn . flexpool->string))
  filename prefix base capacity (partsize default-partsize) 
  (opts) (basemap (make-hashtable)) (partitions {})
  (front #f) (zero #f) (last #f)
  (lock (make-condvar)))

(define-init flexdata (make-hashtable))
(define-init flexpools (make-hashtable))

(define (flexpool/zero fp)
  (cond ((flexpool? fp) (flexpool-zero fp))
	((test flexdata fp)
	 (flexpool-zero (get flexdata fp)))
	(else #f)))
(define (flex/zero fp) (flexpool/zero fp))

(define (flexpool/front fp)
  (cond ((flexpool? fp) (flexpool-front fp))
	((test flexdata fp)
	 (flexpool-front (get flexdata fp)))
	(else #f)))
(define (flex/front fp) (flexpool/front fp))

(define (flexpool/last fp)
  (cond ((flexpool? fp) 
	 (or (flexpool-last fp) (flexpool-front fp)
	     (flexpool-zero fp)))
	((test flexdata fp)
	 (flex/last (get flexdata fp)))
	(else #f)))
(define (flex/last fp) (flexpool/last fp))

(define (flexpool/record fp)
  (cond ((flexpool? fp) fp)
	((test flexdata fp) (get flexdata fp))
	(else #f)))

(define (flexpool/info fp)
  (let* ((record (flexpool/record fp))
	 (front (flexpool-front record)))
    `#[base ,(flexpool-base record)
       load ,(- (+ (oid-addr (pool-base front)) (pool-load front))
		(oid-addr (flexpool-base record)))
       capacity ,(flexpool-capacity record)
       partsize ,(flexpool-partsize record)
       partcount ,(choice-size (flexpool-partitions record))
       front ,(and front (pool-source front))
       last ,(and (flexpool-last record)
		  (pool-source (flexpool-last record)))]))
(define (flex/info fp) (flexpool/info fp))

;;; Finding flexpools by filename

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
	   (let ((prefix (getopt def 'prefix (get-partition-prefix filename)))
		 (base (getopt def 'base))
		 (partsize (getopt def 'partsize (getopt def 'step default-partsize)))
		 (cap (getopt def 'capacity)))
	     (try (tryif (and (exists? prefix) prefix)
		    (get flexpools (mkpath (dirname (abspath filename)) prefix))
		    (get flexpools (mkpath (dirname (realpath filename)) prefix)))
		  (if (and (exists? prefix) prefix
			   (exists? base) base
			   (exists? cap) cap
			   (exists? partsize) partsize)
		      (unique-flexpool (realpath filename) prefix base cap partsize opts)
		      (irritant opts |BadFlexpoolData|))))))))

(define (make-flexpool filename opts)
  (let ((base (getopt opts 'base))
	(cap (getopt opts 'capacity))
	(prefix (getopt opts 'prefix (get-partition-prefix filename)))
	(partsize (getopt opts 'partsize (getopt opts 'step default-partsize))))
    (if (and (exists? prefix) (exists? base) (exists? cap) (exists? partsize))
	(let* ((metadata (getopt opts 'metadata {}))
	       (saved (frame-create #f
			'created (timestamp)
			'init (config 'sessionid)
			'base base 'capacity cap 'partsize partsize
			'prefix prefix
			'load (getopt opts 'load {})
			'partitions (getopt opts 'partitions {})
			'metadata metadata)))
	  (drop! metadata '{cachelevel poolid source cached locked registered load})
	  (drop! metadata '{%slotids readonly opts props})
	  (store! metadata 'flags 
		  (intersection (get metadata 'flags) '{isadjunct adjunct sparse}))
	  (unless (and (file-exists? (dirname prefix))
		       (file-directory? (dirname prefix)))
	    (logwarn |FlexpoolDir| 
	      "Creating a partitions directory " (write (dirname prefix))
	      " for " filename)
	    (mkdirs (dirname prefix)))
	  (dtype->file saved filename)
	  (unique-flexpool (if (readlink filename)
			       (abspath filename)
			       (realpath filename)) 
			   prefix base cap partsize opts))
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
       (make-flexpool filename
		      (if (getopt opts 'make #f)
			  opts
			  (cons #[make #t] opts)))))

(define unique-flexpool ;; -init
  ;; Use define-init to avoid duplicate slambdas/locks when reloading
  (slambda (filename prefix . args)
    (try (flexpool/find filename prefix)
	 (apply init-flexpool filename prefix args))))

(define (init-flexpool filename file-prefix flexbase flexcap 
		       (partsize default-partsize) (opts #f))
  (unless (has-prefix filename "/")
    (set! filename (abspath filename)))
  (let* ((prefix (textsubst file-prefix pool-suffix ""))
	 (padlen (flexpool/padlen flexcap partsize))
	 (flexdir  (dirname filename))
	 (start-ref (glom prefix "." (padnum 0 padlen 16) ".pool"))
	 (start-file (realpath (mkpath flexdir start-ref)))
	 (partition-opts (cons (if (getopt opts 'adjunct)
				   `#[adjuncts {} adjunct ,(getopt opts 'adjunct)]
				   #[adjuncts {}])
			       opts))
	 (start-pool (tryif (file-exists? start-file)
		       (if (getopt opts 'adjunct)
			   (open-pool start-file partition-opts)
			   (use-pool start-file partition-opts))))
	 (suffix-pat `#({"/" (bos)}
			,(basename prefix)
			"." ,(make-vector padlen '(isxdigit)) ".pool"))
	 (matching-files (pick (getfiles (dirname start-file))
			       string-ends-with? suffix-pat))
	 (adjopts (getopt opts 'adjopts
			  (frame-create #f
			    'adjunct #t
			    'adjuncts #f
			    'cachelevel (getopt opts 'cachelevel {})
			    'loglevel (getopt opts 'loglevel {})
			    'readonly (getopt opts 'readonly {})
			    'make (getopt opts 'make))))
	 (basemap (make-hashtable))
	 (load (getopt opts 'load 0))
	 (pools start-pool)
	 (front #f)
	 (last start-pool))

    (debug%watch "INIT-FLEXPOOL" filename file-prefix opts)

    (unless (exists? start-pool)
      (let ((zero-opts
	     `#[base ,flexbase
		capacity ,partsize
		load ,(min load partsize)
		adjunct ,(getopt opts 'adjunct)
		type ,(getopt opts 'type 'bigpool)
		metadata 
		,(make-partition-metadata start-ref opts flexbase flexcap partsize 0)
		label ,(glom (basename prefix) "." (make-string padlen #\0))]))
	(set! start-pool 
	  (if (getopt opts 'adjunct)
	      (open-pool (make-pool start-file (cons zero-opts opts)))
	      (use-pool (make-pool start-file (cons zero-opts opts)))))
	(lognotice |NewPartition|
	  "Created initial pool partition " (write start-file))
	(logdebug |PartitionOpts|
	  "For " (write start-file) "\n" (pprint zero-opts))
	;; Create an flexpool file for the adjunct
	(when (exists? (getkeys (get (get zero-opts 'metadata) 'adjuncts)))
	  (let* ((metadata (get zero-opts 'metadata))
		 (zero-adjuncts (get metadata 'adjuncts)))
	    (do-choices (adjslot (getkeys zero-adjuncts))
	      (let ((adjinfo (get zero-adjuncts adjslot)))
		(when (and (slotmap? adjinfo)
			   (test adjinfo 'pool)
			   (string? (get adjinfo 'pool)))
		  (let ((adj-flexinfo
			 (frame-create #f 
			   'base flexbase 'capacity flexcap 'partsize partsize
			   'created (gmtimestamp) 'init (config 'sessionid)
			   'prefix (glom (dirname prefix)
				     (try (get adjinfo 'prefix)
					  (textsubst (get adjinfo 'pool)
						     partition-suffix ""))))))
		    (dtype->file adj-flexinfo 
				 (textsubst (get adjinfo 'pool) 
					    partition-suffix ".flexpool"))))))))
	(set! pools start-pool)
	(set! last start-pool)))

    (when (> (pool-capacity start-pool) (pool-load start-pool))
      (set! front start-pool))

    (loginfo |PoolPartition| "= " (pool-source start-pool))

    (when (exists? (poolctl start-pool 'metadata 'adjuncts))
      (if (getopt opts 'make)
	  (adjuncts/setup! start-pool 
			   (poolctl start-pool 'metadata 'adjuncts) adjopts)
	  (adjuncts/init! start-pool
			  (poolctl start-pool 'metadata 'adjuncts) adjopts)))
    (store! basemap (pool-base start-pool) start-pool)

    (do-choices (other matching-files)
      (unless (equal? other start-file)
	(let ((pool (if (getopt opts 'adjunct)
			(open-pool other partition-opts)
			(use-pool other partition-opts))))
	  (when (exists? (poolctl pool 'metadata 'adjuncts))
	    (if (getopt opts 'make)
		(adjuncts/setup! pool (poolctl pool 'metadata 'adjuncts) adjopts)
		(adjuncts/init! pool (poolctl pool 'metadata 'adjuncts) adjopts)))
	  (loginfo |PoolPartition| "+ " (pool-source pool))
	  (poolctl pool 'props 'flexbase flexbase)
	  (set+! pools pool)
	  (store! basemap (pool-base pool) pool)
	  (when (and (not front) (< (pool-load pool) (pool-capacity pool)))
	    (set! front pool))
	  (when (> (oid-addr (pool-base pool))
		   (oid-addr (pool-base last)))
	    (set! last pool)))))

    (when (testopt opts 'load)
      (let ((load (getopt opts 'load 0))
	    (serial 1))
	(while (> load (* serial partsize))
	  (let* ((filebase (glom prefix "." (padnum serial padlen 16) ".pool"))
		 (file (mkpath (dirname filename) filebase)))
	    (unless (file-exists? file)
	      (let* ((adjusted-load (- load (* serial partsize)))
		     (make-opts
		      `#[base ,(oid-plus flexbase (* serial partsize)) 
			 load ,(min adjusted-load partsize)
			 capacity ,partsize
			 adjunct ,(getopt opts 'adjunct)
			 type ,(if (testopt opts 'type 'flexpool) 
				   'bigpool
				   (getopt opts 'type 'bigpool))
			 metadata
			 ,(make-partition-metadata 
			   file opts flexbase flexcap
			   partsize serial)
			 label
			 ,(glom (basename prefix) "." (padnum serial padlen 16))])
		     (pool (make-pool file (cons make-opts partition-opts))))
		(lognotice |NewPartition| "Created pool partition " (write file))
		(logdebug |PartitionOpts|
		  "For " (write file) "\n" (pprint make-opts))
		(when (exists? (poolctl pool 'metadata 'adjuncts))
		  (if (getopt opts 'make)
		      (adjuncts/setup! pool (poolctl pool 'metadata 'adjuncts) opts)
		      (adjuncts/init! pool (poolctl pool 'metadata 'adjuncts) opts)))
		(unless (getopt opts 'adjunct) (use-pool pool partition-opts))
		(when (and (not front) (< (pool-load pool) (pool-capacity pool)))
		  (set! front pool))
		(set+! pools pool)
		(loginfo |FlexPool| (write prefix) " + " (pool-source pool))
		(poolctl pool 'props 'flexbase flexbase)
		(set+! pools pool)
		(when (and (not front) (< (pool-load pool) (pool-capacity pool)))
		  (set! front pool))
		(when (> (oid-addr (pool-base pool))
			 (oid-addr (pool-base last)))
		  (set! last pool)))))
	  (set! serial (1+ serial)))))
				
    (when (not front) (set! front last))

    (let ((state (cons-flexpool filename prefix flexbase flexcap partsize
				opts basemap pools
				front start-pool last))
	  (flex-opts `#[adjunct #t 
			register #t
			allocfn ,flexpool-alloc
			getloadfn ,flexpool-load
			fetchfn ,flexpool-fetch
			source ,filename
			cachelevel 0]))
      (let ((pool (make-procpool 
		   prefix flexbase flexcap (cons flex-opts opts) state
		   (getopt opts 'load 0))))
	(lognotice |Flexpool|
	  "Using " ($count (choice-size pools) "partition" "partitions") 
	  " of " ($num partsize) " OIDs"
	  " for " (write (pool-id pool)) 
	  "\n    " pool)
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

;;; Getting the 'next' flexpool (creates a new partition)

(define (flexpool-next-inner fp)
  (let* ((base (oid-plus (pool-base (flexpool-front fp))
			 (flexpool-partsize fp)))
	 (flexbase (flexpool-base fp))
	 (partsize (flexpool-partsize fp))
	 (prefix (flexpool-prefix fp))
	 (padlen (flexpool/padlen (flexpool-capacity fp) partsize))
	 (serial (quotient (oid-offset base flexbase) partsize))
	 (relpath (glom prefix "." (padnum serial padlen 16) ".pool"))
	 (opts `#[base ,base load 0 capacity ,partsize
		  metadata
		  ,(make-partition-metadata
		    relpath (flexpool-opts fp) base (flexpool-capacity fp)
		    partsize serial)
		  label ,(glom (basename prefix) "." (padnum serial padlen 16))])
	 (path (mkpath (dirname (flexpool-filename fp)) relpath)))
    (let ((new (if (file-exists? path)
		   (use-pool path (cons opts (flexpool-opts fp)))
		   (make-pool path (cons opts (flexpool-opts fp))))))
      (when (exists? (poolctl new 'metadata 'adjuncts)) 
	(adjuncts/setup! new))
      (set-flexpool-partitions! fp (choice new (flexpool-partitions fp)))
      (set-flexpool-front! fp new)
      new)))
(define-init flexpool-next
  (slambda (fp) (flexpool-next-inner fp)))

;;; Creating flexpool adjuncts

(define adjpool-suffix
  `(GREEDY {#("." (isxdigit+) ".pool" (eos)) #(".pool" (eos)) #(".flexpool" (eos))}))

(define (flexpool/adjunct! flexpool slotid flex-spec)
  (do-choices (partition (flexpool/partitions flexpool))
    (let ((spec (deep-copy flex-spec))
	  (prefix (flexpool-prefix (flexpool/record flexpool)))
	  (decls (poolctl partition 'metadata 'adjuncts)))
      (cond ((test decls slotid)
	     (logwarn |ExistingAdjunct| 
	       "Not overriding existing adjunct definition for " partition 
	       ": " (get decls slotid)))
	    ((test spec 'index) 
	     (adjuncts/add! partition slotid spec))
	    ((and (test spec 'pool)
		  (file-exists? (mkpath (dirname (pool-source partition))
					(get spec 'pool)))) 
	     (adjuncts/add! partition slotid spec))
	    ((and (test spec 'pool) 
		  (textsearch partition-suffix (pool-source partition)))
	     (store! spec 'pool
		     (glom (basename prefix) "."
		       (strip-suffix (get spec 'pool) ".flexpool")
		       (gather partition-suffix (pool-source partition))))
	     (adjuncts/add! partition slotid spec))
	    ((test spec 'pool)
	     (adjuncts/add! partition slotid spec))
	    (else (logwarn |BadAdjunctSpec| spec))))))

;;; Getting info

(define (flexpool/partitions fp)
  (let ((info (get flexdata fp)))
    (if (and (exists? info) (flexpool? info))
	(flexpool-partitions info)
	(irritant fp |UnknownFlexPool| flexpool/partitions))))
(define (flexpool/partcount fp)
  (let ((info (if (flexpool? fp) fp (get flexdata fp))))
    (if (and (exists? info) (flexpool? info))
	(choice-size (flexpool-partitions info))
	(irritant fp |UnknownFlexPool| flexpool/partitions))))
(define (flex/load flexpool (front))
  (set! front (flexpool-front flexpool))
  (if front
      (oid-offset (oid-plus (pool-base front) (pool-load front))
		  (flexpool-base flexpool))
      0))

;;; Deleting flexpools

(define (flexpool/delete! file (opts #f))
  (cond ((flexpool? file)
	 (set! file (abspath (flexpool-filename file))))
	((test flexdata file)
	 (set! file (abspath (flexpool-filename (get flexdata file)))))
	((exists? (flexpool/find file))
	 (set! file (abspath (flexpool-filename (flexpool/find file)))))
	((string? file) (set! file (abspath file)))
	(else (irritant file |FlexpoolRef|)))
  (cond ((file-exists? file) (set! file (realpath file)))
	((file-exists? (glom file ".flexpool"))
	 (set! file (realpath (glom file ".flexpool"))))
	(else (irritant file |NoFlexpool|)))
  (let* ((info (file->dtype file))
	 (prefix (getopt info 'prefix 
			 (mkpath (basename file) 
				 (strip-suffix (basename file) ".flexpool"))))
	 (partition-suffix
	  (append #(".")
		  (make-vector (flexpool/padlen (get info 'capacity) (get info 'partsize))
			       '(isxdigit))
		  #(".pool")))
	 (metadata (get info 'metadata))
	 (adjuncts (get metadata 'adjuncts))
	 (patterns `#(,(abspath prefix (dirname file)) ,partition-suffix))
	 (filedir (dirname file))
	 (topdir (dirname (abspath prefix filedir)))
	 (dirs topdir))
    (do-choices (adjslot (getkeys adjuncts))
      (let* ((adjinfo (get adjuncts adjslot))
	     (ref (if (string? adjinfo) 
		      adjinfo
		      (try (get adjinfo 'pool)
			   (get adjinfo 'flexpool)
			   (get adjinfo 'source))))
	     (info (tryif (table? adjinfo) adjinfo)))
	(if (or (has-suffix ref {".pool" ".flexpool"})
		(test info 'pool)
		(test info 'flexpool)
		(test info 'type '{pool flexpool}))
	    (cond ((has-prefix ref "/") (set+! dirs (dirname ref)))
		  (else
		   (when (position #\/ ref)
		     (set+! dirs (dirname (mkpath topdir (dirname ref)))))
		   (set+! patterns `#(,(glom (abspath prefix filedir) "." ref)
				      ,partition-suffix))))
	    (if (test adjinfo 'dedicated)
		(unless (getopt opts 'dryrun #f) (remove-file! ref))
		(logwarn |NotDeleted| 
		  "Not deleting the adjunct " (write ref) 
		  " might contain data for other pools.")))))
    (debug%watch "FLEXPOOL/DELETE!" 
      file prefix partition-suffix 
      "\nDIRS" dirs "\nPATTERNS" patterns)
    (let* ((files (getfiles dirs))
	   (matches (pick files string-matches? patterns)))
      (logwarn |FlexpoolDelete| 
	"Deleting " file " and\n "
	(do-choices (match matches i)
	  (printout (if (> i 0) (if (zero? (remainder i 2)) "\n " " "))
	    match)))
      (unless (getopt opts 'dryrun #f)
	(remove-file! file)
	(remove-file! matches)))))

;;; Resetting flexpools

(define (flexpool/reset! file (opts #f))
  (cond ((flexpool? file)
	 (set! file (abspath (flexpool-filename file))))
	((test flexdata file)
	 (set! file (abspath (flexpool-filename (get flexdata file)))))
	((exists? (flexpool/find file))
	 (set! file (abspath (flexpool-filename (flexpool/find file)))))
	((string? file) (set! file (abspath file)))
	(else (irritant file |FlexpoolRef|)))
  (cond ((file-exists? file) (set! file (realpath file)))
	((file-exists? (glom file ".flexpool"))
	 (set! file (realpath (glom file ".flexpool"))))
	(else (irritant file |NoFlexpool|)))
  (let* ((info (file->dtype file))
	 (prefix (getopt info 'prefix 
			 (mkpath (basename file) 
				 (strip-suffix (basename file) ".flexpool"))))
	 (partition-suffix
	  `#("." ,(make-vector (flexpool/padlen cap partsize) '(isxdigit)) ".pool"))
	 (metadata (get info 'metadata))
	 (adjuncts (get metadata 'adjuncts))
	 (patterns `#(,(abspath prefix (dirname file)) ,partition-suffix))
	 (filedir (dirname file))
	 (topdir (dirname (abspath prefix filedir)))
	 (dirs topdir))
    (do-choices (adjslot (getkeys adjuncts))
      (let* ((adjinfo (get adjuncts adjslot))
	     (ref (if (string? adjinfo) 
		      adjinfo
		      (try (get adjinfo 'pool)
			   (get adjinfo 'flexpool)
			   (get adjinfo 'source))))
	     (info (tryif (table? adjinfo) adjinfo)))
	(if (or (has-suffix ref {".pool" ".flexpool"})
		(test info 'pool)
		(test info 'flexpool)
		(test info 'type '{pool flexpool}))
	    (cond ((has-prefix ref "/") (set+! dirs (dirname ref)))
		  (else
		   (when (position #\/ ref)
		     (set+! dirs (dirname (mkpath topdir (dirname ref)))))
		   (set+! patterns `#(,(glom (abspath prefix filedir) "." ref)
				      ,partition-suffix))))
	    (if (test adjinfo 'dedicated)
		(unless (getopt opts 'dryrun #f) (remove-file! ref))
		(logwarn |NotReset| 
		  "Not resetting the adjunct " (write ref) ", "
		  "which might contain data from other pools.")))))
    (debug%watch "FLEXPOOL/RESET!" file prefix dirs patterns)
    (let* ((files (getfiles dirs))
	   (matches (pick files string-matches? patterns)))
      (logwarn |FlexpoolReset| 
	"Resetting " ($num (choice-size matches)) " partitions:\n"
	(do-choices (match matches i)
	  (printout (if (> i 0) (if (zero? (remainder i 2)) "\n " " "))
	    match)))
      (unless (getopt opts 'dryrun #f) (reset-pool matches)))))

(define (reset-pool! poolfile)
  (logwarn |NYI| "Pool resets aren't implemented yet"))

;;; Flexpool adjuncts

;;; Support functions

(define (flexpool/padlen cap chunk)
  (let ((n-chunks (quotient (or cap 0x100000000) chunk))
	(digits 1)
	(n 16))
    (while (<= n n-chunks)
      (set! digits (1+ digits))
      (set! n (* n 16)))
    digits))

(define (make-partition-metadata filename opts flexbase flexcap partsize serial)
  (let* ((metadata (deep-copy (getopt opts 'metadata #[])))
	 (prefix (get-partition-prefix filename))
	 (padlen (flexpool/padlen flexcap partsize))
	 (adjuncts (get metadata 'adjuncts))
	 (adjslots (getkeys adjuncts)))
    (when (test metadata 'type 'flexpool) (drop! metadata 'type))
    (store! metadata 'base (oid-plus flexbase (* serial partsize)))
    (store! metadata 'capacity partsize)
    (store! metadata 'flexbase flexbase)
    (store! metadata 'flexcap flexcap)
    (store! metadata 'partsize partsize)
    (store! metadata 'flexopts (deep-copy opts))
    (store! metadata 'prefix prefix)
    (store! metadata 'serial serial)
    (when (getopt opts 'adjunct)
      (store! metadata 'adjunct (getopt opts 'adjunct)))
    (drop! metadata '{source poolid cachelevel})
    (when (exists? adjslots)
      (let ((converted (frame-create #f)))
	(do-choices (slot adjslots)
	  (let ((spec (get adjuncts slot)))
	    (if (or (and (string? spec) (has-suffix spec {".pool" ".flexpool"}))
		    (and (table? spec) (test spec 'pool) (string? (get spec 'pool)))
		    (and (table? spec) (test spec 'flexpool) 
			 (string? (get spec 'flexpool)))
		    (and (table? spec) (test spec 'pool) (string? (get spec 'pool))
			 (test spec 'prefix)))
		(let* ((adjspec (deep-copy spec))
		       (specname (if (string? spec) spec 
				     (try (get spec 'flexpool) 
					  (get spec 'pool)
					  (get spec 'source))))
		       (speclabel (if (string? spec) #f
				      (getopt spec 'label #f)))
		       (name (getopt spec 'prefix
				     (strip-suffix specname {".pool" ".flexpool"})))
		       (label (glom (or speclabel (getopt opts 'label (basename name)))
				"." (padnum serial padlen 16)))
		       (adjbase (glom name "." (padnum serial padlen 16) ".pool"))
		       (adjpath (glom (basename prefix) "." name
				  "." (padnum serial padlen 16) ".pool")))
		  (if (string? adjspec)
		      (set! adjspec `#[pool ,adjpath])
		      (store! adjspec 'pool adjpath))
		  (store! adjspec 'base (oid-plus flexbase (* serial partsize)))
		  (store! adjspec 'capacity partsize)
		  (store! adjspec 'flexbase flexbase)
		  (store! adjspec 'flexcap flexcap)
		  (store! adjspec 'label label)
		  (store! adjspec 'adjunct slot)
		  (store! adjspec 'metadata `#[adjunct ,slot label ,label])
		  (store! adjspec 'base (oid-plus flexbase (* serial partsize)))
		  (store! adjspec 'capacity partsize)
		  (debug%watch "MAKE-PARTITION-METADATA/adjunct" 
		    filename prefix flexbase flexcap
		    metadata spec adjspec)
		  (store! converted slot adjspec))
		(store! converted slot spec))))
	(store! metadata 'adjuncts converted)))
    (debug%watch "MAKE-PARTITION-METADATA" filename prefix metadata)
    metadata))

;;; Splitting existing pools into smaller flexpools

(define (flexpool/split original (opts #f) (partsize) (output) (rootdir) (prefix))
  (default! partsize (getopt opts 'partsize (config 'PARTSIZE default-partsize)))
  (default! output
    (abspath
     (getopt opts 'output 
	     (config 'OUTPUT 
		     (glom (strip-suffix (abspath original) ".pool")
		       ".flexpool")))))
  (default! rootdir 
    (getopt opts 'rootdir (config 'ROOTDIR (dirname output))))
  (default! prefix 
    (getopt opts 'prefix (config 'PREFIX (get-partition-prefix original))))
  (let* ((input (open-pool original (cons #[adjunct #t] opts)))
	 (flexbase (pool-base input))
	 (flexcap (getopt opts 'newcap (CONFIG 'NEWCAP (pool-capacity input))))
	 (flexload (getopt opts 'newload (config 'NEWLOAD (pool-load input))))
	 (metadata (poolctl input 'metadata))
	 (label (config 'LABEL (pool-label input)))
	 (source (pool-source input))
	 (padlen (flexpool/padlen flexcap partsize)))

    (let* ((opts+ `#[base ,flexbase 
		     prefix ,prefix
		     capacity ,flexcap
		     load ,flexload
		     rootdir ,rootdir
		     metadata ,(poolctl input 'metadata)
		     partsize ,partsize
		     build ,(timestamp)])
	   (batchsize (getopt opts 'batchsize default-batchsize))
	   (nthreads (getopt opts 'nthreads (rusage 'ncpus)))
	   (flexpool (flexpool/make output (cons opts+ opts))))
      (when (getopt opts 'copy #t)
	(let* ((partitions (flexpool/partitions flexpool))
	       (fifo (fifo/make (choice->vector (flexpool/partitions flexpool))
				`#[fillfn ,fifo/exhausted!])))
	  ;; Now, copy all of the OIDs, trying to be efficient
	  (let ((count 0)
		(start (elapsed-time))
		(total-time 0.0))
	    (let ((logcopy (slambda (subpool n total time)
			     (loginfo |RightNow|
			       "Took " (secs->string time) " to copy "
			       ($num n) " (of " ($num total) ") OIDs to " subpool)
			     (set! total-time (+ total-time time))
			     (set! count (+ n count))
			     (lognotice |Overall|
			       "Copied " ($num count) " OIDs (" (show% count flexload) ") "
			       " in " (secs->string (elapsed-time start) 1) ", "
			       ($num (->exact (/~ count (elapsed-time start)))) " OIDs/second or "
			       ($num (->exact (/~ count total-time))) " OIDs/second/thread"))))
	      (let ((threads {}))
		(cond ((and nthreads (> nthreads 1))
		       (dotimes (i nthreads)
			 (set+! threads (thread/call copy-subpool input fifo batchsize logcopy))
			 (when thread-spacing (sleep thread-spacing)))
		       (thread/wait threads))
		      (else (copy-subpool input fifo batchsize logcopy))))))))
      flexpool)))

;;; Copying OIDs into a subpool

(define (copy-subpool from fifo batchsize (logcopy #f))
  (let ((to (fifo/pop fifo)))
    (while (and (exists? to) to)
      (pool/copy from to #f batchsize logcopy)
      (set! to (fifo/pop fifo)))))

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

