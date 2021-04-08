;;; -*- Mode: Scheme -*-

(in-module 'knodb/actions/packpool)

(module-export! '{main packpool})

(use-module '{kno/mttools varconfig binio engine text/stringfmts optimize logger})
(use-module '{knodb/countrefs})

(define %loglevel (config 'loglevel %notice%))
(define %optimize '{knodb/actions/packpool knodb knodb/countrefs})

(define isadjunct #f)
(varconfig! isadjunct isadjunct config:boolean)
(varconfig! adjunct isadjunct config:boolean)

(define (getflags)
  (choice->list
   (choice ;; (tryif (config 'dtypev2 #f) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64)
	   (tryif (config 'ZLIB #f) 'ZLIB)
	   (tryif (config 'ZLIB9 #f) 'ZLIB9)
	   (tryif (config 'ZSTD #f) 'ZSTD)
	   (tryif (config 'ZSTD9 #f) 'ZSTD9)
	   (tryif (config 'ZSTD19 #f) 'ZSTD19)
	   (tryif (config 'SNAPPY #f) 'SNAPPY))))

(define (domove from to)
  (onerror (move-file from to)
      (lambda (ex) (system "mv " from " " to))))

(unless (bound? symbolize)
  (define (symbolize s)
    (if (or (symbol? s)  (number? s)) s
	(if (string? s) (string->symbol (downcase s))
	    (irritant s |NotStringOrSymbol|)))))

(define compression-type-map
  #[bigpool snappy oidpool zlib kpool zstd9 filepool #f])

(define (make-new-pool filename old (opts #f) (type) (adjslot))
  (default! type (getopt opts 'pooltype (symbolize (config 'pooltype 'kpool))))
  (default! adjslot (getopt opts 'adjslot (CONFIG 'ADJSLOT (CONFIG 'ADJUNCTSLOT #f))))
  (let ((metadata (or (poolctl old 'metadata) #[]))
	(base-metadata (or (poolctl old '%metadata) #[]))
	(xrefs (and (eq? type 'kpool) (get-xrefs old opts))))
    (when (eq? type 'kpool)
      (if xrefs
	  (lognotice |XRefs| "Identified " (length xrefs) " xrefs")
	  (lognotice |XRefs| "Disabled"))
      (when (config 'savexrefs)
	(if xrefs
	    (fileout (config 'savexrefs) (doseq (xref xrefs) (lineout xref)))
	    (when (file-exists? (config 'savexrefs)) (remove-file (config 'savexrefs))))))
    (when adjslot (store! base-metadata 'adjunct adjslot))
    (when (or adjslot (CONFIG 'ISADJUNCT))
      (add! base-metadata 'format 'adjunct))
    (drop! base-metadata 'makeopts)
    (make-pool filename
      (modify-frame 
	  `#[type ,type
	     base ,(pool-base old)
	     capacity ,(config 'newcap (pool-capacity old))
	     load ,(config 'newload (pool-load old))
	     label ,(config 'label (pool-label old))]
	'slotids (get-slotids metadata type)
	'xrefs (tryif xrefs xrefs)
	'oidrefs
	(tryif (eq? type 'bigpool)
	  (if (not xrefs) #f 
	      (tryif (not (config 'oidrefs #t config:boolean)) #f)))
	'symrefs 
	(tryif (eq? type 'bigpool)
	  (if (not xrefs) #f 
	      (tryif (not (config 'symrefs #t config:boolean)) #f)))
	'maxrefs (config 'maxrefs #{})
	'compression (get-compression metadata type)
	'isadjunct
	(tryif (or adjslot
		   (config 'ISADJUNCT 
			   (or (test metadata 'format 'adjunct)
			       (test metadata 'flags 'isadjunct))
			   config:boolean))
	  'adjunct)
	'metadata base-metadata
	'register #t))))

(define code-slots #default)
(varconfig! codeslots code-slots config:boolean)

(define (get-slotids metadata type (current) (added (or (config 'slotids) {})))
  (set! current (getopt metadata 'slotids #f))
  (and (if (eq? code-slots #default)
	   (or (vector? current) (exists? added))
	   code-slots)
       (let ((cur (or current #())))
	 (append cur (choice->vector (difference added (elts cur)))))))
(define (get-compression metadata type)
  (or (and (config 'compression) (symbolize (config 'compression)))
      (and (test metadata 'compression) (get metadata 'compression))
      (and type (test compression-type-map type)
	   (symbolize (get compression-type-map type)))))

(define (get-batchsize n)
  (cond ((and (config 'batchsize) (< (config 'batchsize) 1))
	 (* n (config 'batchsize)))
	((and (config 'batchsize) (> (config 'batchsize) 1))
	 (config 'batchsize))
	((config 'ncycles) (/ n (config 'ncycles)))
	(else 100000)))

(define (copy-block queuefn old new (xopts #f) (report #f))
  (let ((oids (queuefn))
	(started (elapsed-time)))
    (while (and (exists? oids) oids)
      (let* ((oidvec (choice->vector oids))
	     (valvec (pool/fetchn old oidvec))
	     (storevec (if xopts
			   (forseq (val valvec) (raw-xtype val xopts))
			   valvec)))
	(pool/storen! new oidvec storevec))
      (when report (report (choice-size oids) started))
      (set! oids (queuefn))
      (set! started (elapsed-time)))))

(define (copy-oids old new (opts #f))
  (loginfo |CopyOIDS|
      "Copying OIDs" (if (pool-label old)
			 (append " for " (pool-label old)))
      " from " (or (pool-source old) old)
      "\n into " (or (pool-source new) new))
  (let* ((started (elapsed-time))
	 (newload (pool-load new))
	 (alloids (or (poolctl old 'keys) (pool-elts old)))
	 (oids (if (= (pool-load old) newload)
		   alloids
		   (pick alloids oid-offset < newload)))
	 (threadcount (mt/threadcount (config 'nthreads 10)))
	 (loadsize (config 'loadsize 100000))
	 (blocksize (config 'blocksize (quotient loadsize threadcount)))
	 (noids (choice-size oids))
	 (compression (dbctl new 'compression))
	 (nblocks (1+ (quotient noids blocksize)))
	 (blocks-done 0)
	 (oids-done 0)
	 (queue '()))
    (dotimes (i nblocks)
      (set! queue (cons (qc (pick-n oids blocksize (* i blocksize)))
			queue)))
    (set! queue (reverse queue))
    (lognotice |CopyOIDS|
      "Copying " ($num (choice-size oids)) " OIDs"
      (if (pool-label old) (append " for " (pool-label old)))
      " from " (or (pool-source old) old)
      "\n into " (or (pool-source new) new)
      " in " ($num (length queue)) " blocks"
      " using " (or threadcount "no") " threads")
    (let ((pop-queue
	   (slambda ()
	     (tryif (pair? queue)
	       (prog1 (car queue) (set! queue (cdr queue))))))
	  (report-progress
	   (slambda (count start)
	     (set! blocks-done (1+ blocks-done))
	     (set! oids-done (+ oids-done count))
	     (loginfo |FinishedOIDs|
	       "Finished a block of " ($num count) " OIDs "
	       "in " (secs->string (elapsed-time start)))
	     (lognotice |CopyOIDs|
	       "Copied " ($num blocks-done) "/" ($num nblocks) " blocks, "
	       ($num oids-done) " OIDs (" (show% oids-done noids) ") "
	       "after " (secs->string (elapsed-time started)))))
	  (xopts (and (eq? (poolctl new 'type) 'kpool)
		      [xrefs (poolctl new '%xrefs) 
		       compression (getopt opts 'compresion compression)])))
      (if threadcount
	  (let ((threads {}))
	    (dotimes (i threadcount)
	      (set+! threads 
		(thread/call copy-block pop-queue old new xopts report-progress)))
	    (thread/wait threads))
	  (copy-block pop-queue old new xopts report-progress)))))

(define (existing-xrefs pool (opts #f))
  (let ((refs (poolctl pool 'xrefs))
	(maxlen (getopt opts 'maxrefs)))
    (when (and (not refs) (testopt opts 'compute))
      (logwarn |NoExistingXRefs| "Computing xrefs from contents of " pool)
      (set! refs (count-xrefs pool opts)))
    (and refs (vector? refs)
	 (if (and maxlen (> (length refs) maxlen))
	     (slice refs 0 maxlen)
	     refs))))

(define (get-xrefs pool (opts #f))
  (let ((xrefs-source (getopt opts 'xrefs (config 'xrefs #f))))
    (cond ((and (string? xrefs-source) (file-exists? xrefs-source))
	   (read-xrefs xrefs-source opts))
	  ((or (symbol? xrefs-source) (string? xrefs-source))
	   (let ((opt (downcase xrefs-source)))
	     (cond ((overlaps? opt {"no" "none" "false" "off" "reset" "fresh"}) #f)
		   ((overlaps? opt {"keep" "retain"}) (poolctl pool 'xrefs))
		   ((overlaps? opt {"auto" "compute" "count"}) (count-xrefs pool opts))
		   (else (logwarn |BadXrefsOption| xrefs-source)))))
	  (else (existing-xrefs pool (opt+ opts 'compute #t))))))

(define (get-base-oids pool)
  (let ((base (pool-base pool))
	(cap (pool-capacity pool))
	(base-oids {}))
    (dotimes (i (+ (quotient cap #mib)
		   (if (zero? (remainder cap #mib)) 0 1)))
      (set+! base-oids (oid-plus base (* i #mib))))
    base-oids))

(define (read-xrefs file (opts #f))
  (cond ((has-suffix file ".pool")
	 (poolctl (open-pool file [register #f adjunct #t]) 'xrefs))
	((has-suffix file ".index")
	 (poolctl (open-index file [register #f adjunct #t]) 'xrefs))
	(else (let* ((elts '())
		     (in (open-input-file file))
		     (item (read in)))
		(while (or (symbol? item) (oid? item))
		  (set! elts (cons item elts))
		  (set! item (read in)))
		(reverse (->vector elts))))))

(define (count-xrefs pool opts)
  (let* ((freqs (countrefs/pool pool opts)))
    (when (config 'savefreqs) (write-xtype freqs (config 'savefreqs)))
    (rsorted (getkeys freqs) freqs)))

(define (writable? file)
  (if (file-exists? file)
      (file-writable? file)
      (and (file-directory? (dirname file)) 
	   (file-writable? (dirname file)))))

(define (repack-pool from to (opts #f))
  (let* ((base (basename from))
	 (inplace (equal? from to))
	 (tmpfile (config 'TMPFILE (CONFIG 'TEMPFILE (glom to ".part"))))
	 (bakfile (config 'BAKFILE (CONFIG 'BACKUP (glom from ".bak")))))
    (config! 'appid (glom "pack(" (basename to) ")"))
    (cond ((and (not (writable? to)))
	   (logcrit |NotWritable| "Can't write output file " (write to))
	   (exit))
	  ((not (writable? tmpfile))
	   (logcrit |NotWritable| "Can't write temporary output file " (write tmpfile))
	   (exit))
	  ((and (file-exists? tmpfile) (not (config 'RESTART)))
	   (logwarn |ExistingTMP| 
	     "The temporary file " (write tmpfile) " already exists. "
	     "Remove it or specify the config RESTART=yes to ignore")
	   (exit))
	  ((file-exists? tmpfile)
	   (remove-file tmpfile))
	  (else))
    (let ((old (open-pool from `#[register #t adjunct #t cachelevel 2
				  repair ,(config 'repair #f config:boolean)])))
      (lognotice |Repack| 
	"Repacking " ($num (pool-load old)) "/" ($num (pool-capacity old)) " "
	"OIDs " (if (pool-label old) (glom "for " (pool-label old)))
	(when inplace
	  (printout " with backup saved as " bakfile)))
      (let ((new (make-new-pool tmpfile old)))
	(copy-oids old new opts)
	(commit new)))
    (when inplace (domove from bakfile))
    (domove tmpfile to)))

(define (copy-pool from to (opts #f))
  (let* ((base (basename from)))
    (config! 'appid (glom "copy(" (basename to) ")"))
    (when (not (writable? to))
      (logcrit |NotWritable| "Can't write output file " (write to))
      (exit))
    (let ((old (open-pool from `#[register #t adjunct #t cachelevel 2
				  repair ,(config 'repair #f config:boolean)]))
	  (new (open-pool to #[register #t adjunct #t cachelevel 2])))
      (lognotice |Copy| 
	"Copying " ($num (pool-load old)) "/" ($num (pool-capacity old)) " "
	"OIDs " (if (pool-label old) (glom "for " (pool-label old)))
	" to " to)
      (copy-oids old new opts)
      (commit new))))

;;; Exports

(define (packpool (from #f) (to) (overwrite) (restart))
  (default! to from)
  (default-configs)
  (default! overwrite (config 'overwrite #f config:boolean))
  (default! restart (config 'restart #f config:boolean))
  (when (and to (file-exists? to) (not (equal? from to)) (not overwrite))
    (logerr |FileExists|
      "The output file " (write to) " already exists.\n  "
      "Specify OVERWRITE=yes to remove.")
    (exit))
  (when (and to (file-exists? (glom to ".part")))
    (cond (restart 
	   (logwarn |Restarting| "Removing " (write (glom to ".part")))
	   (remove-file (glom to ".part")))
	  (else (logerr |InProgress|
		  "The temporary output file " (write (glom to ".part")) " exists.\n  "
		  "Specify RESTART=yes to remove.")
		(exit))))
  (cond ((or (not (bound? from)) (not from)) (usage))
	((and (file-exists? to) (not (equal? from to)) (config 'COPY #f))
	 (config! 'appid (glom "copy(" (basename from) ")"))
	 (logwarn |Copying| "Copying OIDs to existing pool " (write to))
	 (copy-pool from to #f))
	((and (file-exists? to) (not (equal? from to)) (not overwrite))
	 (logwarn |FileExists| "Not overwriting " (write to))
	 (exit))
	((not (file-exists? from))
	 (logwarn |MissingInput| "Can't locate source " (write from))
	 (exit))
	(else (config! 'appid (glom "repack(" (basename from) ")"))
	      (repack-pool (abspath from) (abspath to) #f))))

(define (default-configs)
  (config! 'cachelevel 2)
  (config! 'optlevel 4)
  (config! 'logprocinfo #t)
  (config! 'logthreadinfo #t)
  (config! 'thread:logexit #f))

(define (main (from #f) (to))
  (default! to from)
  (when (config 'optimized #t) (optimize-module! %optimize))
  (if (and from (file-exists? from))
      (packpool from to)
      (usage)))

(define (usage)
  (lineout "Usage: pack-pool <from> [to]\n"
    ($indented 4
	       "Repacks the file pool stored in <from> either in place or into [to]."
	       "Common options include (first value is default) : \n"
	       ($indented 4
			  "POOLTYPE=keep|knopool|filepool\n"
			  "COMPRESSION=none|zlib9|snappy|zstd9\n"
			  "CODESLOTS=yes|no\n"
			  "OVERWRITE=no|yes\n")
	       "If specified, [to] must not exist unless OVERWRITE=yes")))

