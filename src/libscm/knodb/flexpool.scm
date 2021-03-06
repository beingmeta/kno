;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/flexpool)

(use-module '{binio db/drivers texttools kno/statefiles})
(use-module '{ezrecords text/stringfmts logger varconfig fifo})
(use-module '{knodb/adjuncts knodb/filenames})
(use-module '{knodb})

(module-export! '{flexpool/open flexpool/make flexpool?
		  flexpool/ref flexpool/record 
		  flexpool/zero flexpool/front flexpool/last
		  flexpool/info flexpool/partition
		  flexpool/partitions flexpool/partcount
		  flexpool/delete!})

(module-export! '{flexpool-def flexpool-opts flexpool-partitions 
		  flexpool-partsize flexpool-front
		  flexpool-filename flexpool-prefix flexpool-base flexpool-capacity
		  flexpool-suffix})

(define-init %loglevel %notify%)

(define-init default-partsize 0x100000)
(varconfig! flexpool:partsize default-partsize)

;;; Utility functions

(define partition-suffix
  #("." (isxdigit+) ".pool" (eos)))
(define pool-suffix
  {#(".flexpool" (eos)) #("." (opt #((isxdigit+) ".")) "pool" (eos))})

(define (get-partition-prefix filename (opts #f) (given))
  (set! given (getopt opts 'prefix (config 'prefix)))
  (if given
      (if (has-suffix given "/")
	  (mkpath given (basename filename #t))
	  given)
      (let ((stripped (textsubst (basename filename) (qc pool-suffix) "")))
	(if (getopt opts 'partdir #f)
	    (mkpath (getopt opts 'partdir #f) stripped)
	    stripped))))

(define (get-partition-type opts)
  (getopt opts 'partition-type 
	  (if (getopt opts 'partopts)
	      (getopt (getopt opts 'partopts) 'type 'kpool)
	      'kpool)))

;;; Flexpool records

(define (flexpool->string f)
  (stringout "#<FLEXPOOL " (flexpool-filename f) " " 
    (oid->string (flexpool-base f)) "+" 
    (flexpool/partcount f) "*" (flexpool-partsize f) " "
    (flexpool-capacity f) ">"))

(defrecord (flexpool mutable opaque 
		     #[predicate isflexpool?] 
		     `(stringfn . flexpool->string))
  filename prefix base capacity def
  (partsize default-partsize) (partopts `#[partsize default-partsize])
  (opts) (basemap (make-hashtable)) (partitions {})
  (front #f) (zero #f) (last #f)
  (lock (make-condvar)))

(define-init flexdata (make-hashtable))
(define-init flexpools (make-hashtable))

(define (flexpool? arg)
  (or (isflexpool? arg) 
      (and (test flexdata arg)
	   (exists isflexpool? (get flexdata arg)))))

(define (flexpool/zero fp)
  (cond ((isflexpool? fp) (flexpool-zero fp))
	((test flexdata fp)
	 (flexpool-zero (get flexdata fp)))
	(else #f)))
(define (flexpool/zero fp) (flexpool/zero fp))

(define (flexpool/front fp)
  (cond ((isflexpool? fp) (flexpool-front fp))
	((test flexdata fp)
	 (flexpool-front (get flexdata fp)))
	(else #f)))

(define (flexpool/last fp)
  (cond ((isflexpool? fp) 
	 (or (flexpool-last fp) (flexpool-front fp)
	     (flexpool-zero fp)))
	((test flexdata fp)
	 (flexpool/last (get flexdata fp)))
	(else #f)))

(define (flexpool/record fp)
  (cond ((isflexpool? fp) fp)
	((test flexdata fp) (get flexdata fp))
	(else #f)))

(define (flexpool/info fp)
  (let* ((record (flexpool/record fp))
	 (front (flexpool-front record)))
    `#[base ,(flexpool-base record)
       capacity ,(flexpool-capacity record)
       load ,(flexpool/load fp)
       partsize ,(flexpool-partsize record)
       partcount ,(choice-size (flexpool-partitions record))
       front ,(and front (pool-source front))
       last ,(and (flexpool-last record)
		  (pool-source (flexpool-last record)))]))

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
	 (let* ((def (statefile/read filename))
		(opts (if opts (cons opts def) def)))
	   (let* ((prefix (getopt def 'prefix (get-partition-prefix filename opts)))
		  (base (getopt def 'base))
		  (partsize (getopt def 'partsize (getopt def 'step default-partsize)))
		  (partopts (get def 'partopts))
		  (cap (getopt def 'capacity)))
	     (debug%watch "FLEXPOOL/OPEN" 
	       filename prefix base partsize cap
	       "\nDEF" def "\nOPTS" opts)
	     (try (tryif (and (exists? prefix) prefix)
		    (get flexpools (mkpath (dirname (abspath filename)) prefix))
		    (get flexpools (mkpath (dirname (realpath filename)) prefix)))
		  (if (and (satisfied? prefix) (satisfied? base) (satisfied? cap)
			   (satisfied? partsize) (satisfied? partopts))
		      (let ((pool (unique-flexpool filename prefix base cap def
						   partsize partopts
						   opts)))
			(dotimes (i (getopt opts 'reserve 0))
			  (loginfo |ReservePartition| 
			    "Reserved partition " (flexpool/partition pool i opts 'create)))
			pool)
		      (irritant opts |BadFlexpoolData|))))))))

(define partopts #f)

(define (make-flexpool filename opts)
  (let ((base (getopt opts 'base))
	(cap (getopt opts 'capacity))
	(prefix (get-partition-prefix filename opts))
	(partsize (getopt opts 'partsize (getopt opts 'step default-partsize)))
	(pooltype (getopt opts 'parttype (getopt opts 'pooltype 'kpool)))
	;; These adjuncts apply to the partitions of the flexpool
	(adjuncts (getopt opts 'adjuncts {})))
    (debug%watch "MAKE-FLEXPOOL" 
      filename prefix base partsize cap "\nOPTS" opts)
    (if (and (satisfied? prefix) (satisfied? base) (satisfied? cap) (satisfied? partsize))
	(let* ((metadata (getopt opts 'metadata #[]))
	       (absprefix (mkpath (dirname (abspath filename)) prefix))
	       (partopts (frame-create #f
			   'capacity partsize
			   'load (tryif (getopt opts 'prealloc #f) partsize)
			   'prealloc (getopt opts 'prealloc {})
			   'pooltype pooltype
			   'metadata 
			   (frame-create #f
			     'adjuncts adjuncts
			     'flexinfo `#[base ,base cap ,cap partsize ,partsize
					  pooltype ,pooltype prefix ,prefix])
			   'prefix prefix))
	       (flexdef  (frame-create #f
			   'created (timestamp)
			   'init (config 'sessionid)
			   'base base 'capacity cap 'partsize partsize
			   'prefix prefix
			   'prealloc (getopt opts 'prealloc {})
			   'partitions (getopt opts 'partitions {})
			   'partopts partopts
			   'metadata metadata)))
	  (debug%watch "MAKE-FLEXPOOL"
	    filename absprefix "\n" partopts "\n" metadata "\n" flexdef)
	  (drop! metadata '{cachelevel poolid source cached locked registered load})
	  (drop! metadata '{%slotids readonly opts props})
	  (store! metadata 'flags
	    (intersection (get metadata 'flags) '{isadjunct adjunct sparse}))
	  (store! metadata 'partopts partopts)
	  (unless (and (file-exists? (dirname absprefix))
		       (file-directory? (dirname absprefix)))
	    (logwarn |FlexpoolDir| 
	      "Creating a partitions directory " (write (dirname absprefix))
	      " for " filename)
	    (mkdirs (dirname absprefix)))
	  (statefile/save! flexdef #[useformat xtype] filename)
	  (logdebug |MakeFlexpool|
	    "Initialized " filename " with definition:\n"
	    (listdata flexdef))
	  (let ((pool (unique-flexpool (abspath filename) 
				       prefix base cap flexdef
				       partsize partopts
				       opts))
		(reserve (getopt opts 'reserve 1)))
	    (when reserve
	      (dotimes (i (if (number? reserve) reserve 1))
		(logwarn |ReservePartition| 
		  "Reserved partition " (flexpool/partition pool i opts 'create))))
	    pool))
	(irritant opts |IncompleteFlexpoolDef|))))

(define (flexpool/make filename opts)
  (unless (has-prefix filename "/")
    (set! filename (abspath filename)))
  (let ((existing (flexpool/find filename)))
    (if (exists? existing)
	(irritant filename |AlreadyExists|)
	(make-flexpool filename (cons #[make #t] opts)))))

(define (flexpool/ref filename opts)
  (unless (has-prefix filename "/")
    (set! filename (abspath filename)))
  (try (flexpool/open filename opts)
       (make-flexpool filename
		      (if (getopt opts 'make #f) opts (cons #[make #t] opts)))))

(define unique-flexpool
  ;; Use define-init to avoid duplicate slambdas/locks when reloading
  (slambda (filename prefix flexbase flexcap def partsize partopts (open-opts #f))
    (try (flexpool/find filename prefix)
	 (init-flexpool filename prefix flexbase flexcap def partsize partopts open-opts))))

(define (init-flexpool filename file-prefix flexbase flexcap def
		       partsize partopts 
		       (open-opts #f))
  (unless (has-prefix filename "/") (set! filename (abspath filename)))
  (let* ((prefix (textsubst file-prefix pool-suffix ""))
	 (padlen (get-padlen flexcap partsize))
	 (flexdir (dirname filename))
	 (suffix-pat `#({"/" (bos)}
			,(basename prefix)
			"." ,(make-vector padlen '(isxdigit)) ".pool"))
	 (matching-files (pick (getfiles (dirname (mkpath flexdir prefix)))
			   string-ends-with? suffix-pat))
	 (basemap (make-hashtable))
	 (pools {})
	 (front #f)
	 (last #f)
	 (zero #f))

    (debug%watch "INIT-FLEXPOOL" 
      filename file-prefix "\n" open-opts )

    (do-choices (other matching-files)
      (let ((pool (knodb/pool
		   (if (getopt open-opts 'adjunct)
		       (open-pool other (opt+ open-opts partopts))
		       (use-pool other (opt+ open-opts partopts)))
		   open-opts)))
	(loginfo |PoolPartition| "+ " (pool-source pool))
	(poolctl pool 'props 'flexbase flexbase)
	(store! basemap (pool-base pool) pool)
	(set+! pools pool)
	;; *front* is the first pool with any space
	(when (and (not front) (< (pool-load pool) (pool-capacity pool)))
	  (set! front pool))
	;; Zero is the first pool
	(when (= (oid-addr (pool-base pool)) (oid-addr flexbase))
	  (set! zero pool))
	;; *last* is the numerically largest pool in the flex pool
	(when (or (not last)
		  (> (oid-addr (pool-base pool)) (oid-addr (pool-base last))))
	  (set! last pool))))
    
    (when (not front) (set! front last))

    (let* ((state (cons-flexpool filename prefix flexbase flexcap def
				 partsize partopts open-opts
				 basemap pools
				 front zero last))
	   (flex-opts `#[adjunct #t virtual #t
			 register (getopt opts 'register)
			 type flexpool
			 source ,filename
			 cachelevel 0])
	   (zero-pool (flexpool-partition state 0 open-opts 'create)))
      (let ((pool (make-procpool
		   prefix flexbase flexcap (cons flex-opts open-opts) state
		   (getopt open-opts 'load 0))))
	(lognotice |Flexpool|
	  "Using " ($count (choice-size pools) "partition" "partitions") 
	  " of " ($num partsize) " OIDs"
	  " for" (if (getopt open-opts 'adjunct) " adjunct" )" flexpool " (write (pool-id pool)) 
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

(define (flexpool-partition fp serial (open-opts #f) (action #f))
  (let ((base (oid-plus (flexpool-base fp) (* serial (flexpool-partsize fp)))))
    (try (pick (flexpool-partitions fp) pool-base base)
	 (let* ((padlen (get-padlen (flexpool-capacity fp) (flexpool-partsize fp)))
		(relpath (glom (flexpool-prefix fp) "." (padnum serial padlen 16) ".pool"))
		(path (mkpath (dirname (flexpool-filename fp)) relpath))
		(partopts (deep-copy (flexpool-partopts fp)))
		(metadata (get partopts 'metadata)))
	   (store! partopts 'base base)
	   (store! (get metadata 'flexinfo) 'serial serial)
	   (store! (get metadata 'flexinfo) 'base base)
	   (debug%watch "FLEXPOOL-PARTITION" 
	     fp serial base padlen path 
	     "\n" metadata "\n" partopts
	     "\nFLEXINFO"  (get metadata 'flexinfo))
	   (cond ((file-exists? path)
		  (knodb/pool
		   (if (or (getopt open-opts 'adjunct)
			   (getopt partopts 'adjunct))
		       (open-pool path (cons open-opts partopts))
		       (use-pool path (cons open-opts partopts)))))
		 ((eq? action 'err)
		  (irritant path |BadPartition| flexpool-partition))
		 ((or (eq? action 'create) (eq? action #t))
		  (let ((made (knodb/pool (make-pool path partopts)
					  (cons #[create #t] open-opts))))
		    (lognotice |NewPartition| made " for " fp)
		    (unless (satisfied? (flexpool-front fp))
		      (set-flexpool-front! fp made))
		    (unless (satisfied? (flexpool-last fp))
		      (set-flexpool-last! fp made))
		    (when (and (satisfied? (flexpool-zero fp)) 
			       (eq? (pool-base made) (flexpool-base fp)))
		      (set-flexpool-zero! fp made))
		    (set-flexpool-partitions! fp (choice made (flexpool-partitions fp)))
		    made)))))))

(define (flexpool/partition fp serial (opts #f) (action))
  (default! action (if (eq? opts #t) 'create (getopt opts 'action)))
  (if (eq? opts #t) (set! opts #f))
  (cond ((isflexpool? fp) (flexpool-partition fp serial opts action))
	((test flexdata fp) (flexpool-partition (get flexdata fp) serial opts action))
	(else #f)))

;;; Getting the 'next' flexpool (creates a new partition)

(define (flexpool-next-inner fp)
  (let* ((front (flexpool-front fp))
	 (serial (get (poolctl front 'metadata 'flexinfo) 'serial))
	 (next (flexpool-partition fp (1+ serial) (flexpool-opts fp) 'create)))
    (loginfo |FlexpoolNext| "Created new pool " next " in " fp)
    (set-flexpool-front! fp next)
    next))
(define-init flexpool-next
  (slambda (fp) (flexpool-next-inner fp)))

;;; Creating flexpool adjuncts

#|
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
|#

;;; Getting info

(define (flexpool/partitions fp)
  (let ((info (get flexdata fp)))
    (if (and (exists? info) (isflexpool? info))
	(flexpool-partitions info)
	(irritant fp |UnknownFlexPool| flexpool/partitions))))
(define (flexpool/partcount fp)
  (let ((info (if (isflexpool? fp) fp (get flexdata fp))))
    (if (and (exists? info) (isflexpool? info))
	(choice-size (flexpool-partitions info))
	(irritant fp |UnknownFlexPool| flexpool/partitions))))
(define (flexpool/load flexpool (front))
  (if (getopt (flexpool-def flexpool) 'prealloc #f)
      (if (flexpool-front flexpool)
	  (let ((front (flexpool-front flexpool)))
	    (oid-offset (oid-plus (pool-base front) (pool-load front))
			(flexpool-base flexpool)))
	  0)))
(define flex/load flexpool/load)

;;; Deleting flexpools

(define (flexpool/delete! file (opts #f))
  (cond ((isflexpool? file)
	 (set! file (abspath (flexpool-filename file))))
	((test flexdata file)
	 (set! file (abspath (flexpool-filename (get flexdata file)))))
	((exists? (flexpool/find file))
	 (set! file (abspath (flexpool-filename (get flexpools (flexpool/find file))))))
	((string? file) (set! file (abspath file)))
	(else (irritant file |FlexpoolRef|)))
  (cond ((file-exists? file) (set! file (realpath file)))
	((file-exists? (glom file ".flexpool"))
	 (set! file (realpath (glom file ".flexpool"))))
	(else (irritant file |NoFlexpool|)))
  (let* ((info (statefile/read file))
	 (prefix (getopt info 'prefix 
			 (mkpath (basename file) 
				 (strip-suffix (basename file) ".flexpool"))))
	 (partition-suffix
	  (append #(".")
		  (make-vector (get-padlen (get info 'capacity) (get info 'partsize))
			       '(isxdigit))
		  #(".pool" {"" ".commit" ".rollback"})))
	 (metadata (get info 'metadata))
	 (adjuncts (get metadata 'adjuncts))
	 (patterns `#(,(abspath prefix (dirname file)) (opt #("_" (isalpha+))) ,partition-suffix))
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
  (cond ((isflexpool? file)
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
  (let* ((info (statefile/read file))
	 (cap (get info 'flexcap))
	 (partsize (get info 'partsize))
	 (prefix (getopt info 'prefix 
			 (mkpath (basename file) 
				 (strip-suffix (basename file) ".flexpool"))))
	 (partition-suffix
	  `#("." ,(make-vector (get-padlen cap partsize) '(isxdigit)) ".pool"))
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
      (unless (getopt opts 'dryrun #f) (reset-pool! matches)))))

(define (reset-pool! poolfile)
  (logwarn |NYI| "Pool resets aren't implemented yet"))

;;; Support functions

(define (get-padlen cap chunk)
  (let ((n-chunks (quotient (or cap 0x100000000) chunk))
	(digits 1)
	(n 16))
    (while (<= n n-chunks)
      (set! digits (1+ digits))
      (set! n (* n 16)))
    digits))

;; (define (flexpool-partition-load flexload partsize serial (load))
;;   (default! load (- flexload (* partsize serial)))
;;   (if (<= load 0) 0 (if (> load partsize) partsize load)))

(define (flexpool/partition-opts file flexbase flexload flexcap partsize serial opts)
  (let* ((base (oid-plus flexbase (* serial partsize)))
	 (padlen (get-padlen flexcap partsize))
	 ;; (load (flexpool-partition-load flexload partsize serial))
	 )
    (let ((metadata (make-partition-metadata file opts flexbase flexcap partsize serial)))
      (modify-frame
	  `#[base ,base
	     ;;maybe delete me
	     ;;load ,(if (> load partsize) partsize load)
	     capacity ,partsize
	     adjunct ,(getopt opts 'adjunct (getopt metadata 'adjunct))
	     type ,(get-partition-type opts)
	     metadata ,metadata
	     label ,(glom (getopt opts 'prefix) "." (padnum serial padlen 16))]
	'compression (getopt opts 'compression {})))))

(define (make-partition-metadata filename opts flexbase flexcap partsize serial)
  (let* ((flex-metadata (getopt opts 'metadata #[]))
	 (metadata (frame-create #f
		     'adjunct (getopt flex-metadata 'adjunct {})))
	 (prefix (get-partition-prefix filename))
	 (padlen (get-padlen flexcap partsize))
	 (adjuncts (get flex-metadata 'adjuncts))
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
		       (adjpath (glom name ;; (basename prefix) "."
				  "." (padnum serial padlen 16) ".pool")))
		  (if (string? adjspec)
		      (set! adjspec `#[pool ,adjpath])
		      (store! adjspec 'pool adjpath))
		  (store! adjspec 'type (get-partition-type opts))
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

;;; Handlers

(define (flexpool-alloc pool flexpool (n 1))
  (with-lock (flexpool-lock flexpool)
    (let ((front (flexpool-front flexpool)))
      (unless (satisfied? front)
	(set! front (flexpool-partition flexpool 0)))
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
	 "Can't store values directly in the flexpool " pool))
(define (flexpool-load pool flexpool (front))
  (if (getopt (flexpool-partopts flexpool) 'prealloc)
      (* (flexpool-partsize flexpool) (|| (flexpool-partitions flexpool)))
      (begin
	(set! front (flexpool-front flexpool))
	(if front
	    (oid-offset (oid-plus (pool-base front) (pool-load front))
			(pool-base pool))
	    0))))

(define (flexpool-ctl pool flexpool op . args)
  (cond ((and (eq? op 'partitions) (null? args))
	 (flexpool-partitions flexpool))
	(else (apply poolctl/default pool op args))))

(defpooltype 'flexpool
  `#[open ,flexpool/open
     create ,flexpool/make
     alloc ,flexpool-alloc
     getload ,flexpool-load
     fetch ,flexpool-fetch
     poolctl ,flexpool-ctl])
(defpooltype 'knodb/flexpool
  `#[open ,flexpool/open
     create ,flexpool/make
     alloc ,flexpool-alloc
     getload ,flexpool-load
     fetch ,flexpool-fetch
     poolctl ,flexpool-ctl])

