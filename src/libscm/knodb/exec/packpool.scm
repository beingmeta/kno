;;; -*- Mode: Scheme -*-

(in-module 'knodb/exec/packpool)

(module-export! 'main)

(use-module '{optimize kno/mttools varconfig engine text/stringfmts logger})
(use-module '{knodb/countrefs})

(define %loglevel (config 'loglevel %notice%))

(define dtypev2 #f)
(varconfig! dtypev2 dtypev2 config:boolean)
(define isadjunct #f)
(varconfig! isadjunct isadjunct config:boolean)
(varconfig! adjunct isadjunct config:boolean)

(define (getflags)
  (choice->list
   (choice (tryif (config 'dtypev2 #f) 'dtypev2)
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

(define (make-new-pool filename old 
		       (type (symbolize (config 'pooltype 'kpool)))
		       (adjslot (CONFIG 'ADJSLOT (CONFIG 'ADJUNCTSLOT #f))))
  (let ((metadata (or (poolctl old '%metadata) #[]))
	(xrefs (and (eq? type 'kpool) (compute-xrefs old))))
    (when (eq? type 'kpool)
      (if xrefs
	  (lognotice |XRefs| "Identified " (length xrefs) " xrefs")
	  (lognotice |XRefs| "Disabled"))
      (when (config 'savexrefs)
	(if xrefs
	    (fileout (config 'savexrefs) (doseq (xref xrefs) (lineout xref)))
	    (when (file-exists? (config 'savexrefs)) (remove-file (config 'savexrefs))))))
    (when adjslot (store! metadata 'adjunct adjslot))
    (when (or adjslot (CONFIG 'ISADJUNCT))
      (add! metadata 'format 'adjunct))
    (drop! metadata 'makeopts)
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
	'dtypev2 (tryif (or dtypev2 (test metadata 'flags 'dtypev2)) 'dtypev2)
	'isadjunct
	(tryif (or adjslot
		   (config 'ISADJUNCT 
			   (or (test metadata 'format 'adjunct)
			       (test metadata 'flags 'isadjunct))
			   config:boolean))
	  'adjunct)
	'metadata metadata
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
      (and type (test compression-type-map type)
	   (symbolize (get compression-type-map type)))))

(define (get-batchsize n)
  (cond ((and (config 'batchsize) (< (config 'batchsize) 1))
	 (* n (config 'batchsize)))
	((and (config 'batchsize) (> (config 'batchsize) 1))
	 (config 'batchsize))
	((config 'ncycles) (/ n (config 'ncycles)))
	(else 100000)))

(define (copy-block queuefn old new (msg #f))
  (let ((oids (queuefn))
	(started (elapsed-time)))
    (while (and (exists? oids) oids)
      (let* ((oidvec (choice->vector oids))
	     (valvec (pool/fetchn old oidvec)))
	(pool/storen! new oidvec valvec))
      (when msg (msg (choice-size oids) started))
      (set! oids (queuefn))
      (set! started (elapsed-time)))))

(define (copy-oids old new)
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
	 (blocksize (quotient loadsize threadcount))
	 (noids (choice-size oids))
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
	       "after " (secs->string (elapsed-time started))))))
      (if threadcount
	  (let ((threads {}))
	    (dotimes (i threadcount)
	      (set+! threads 
		(thread/call copy-block pop-queue old new report-progress)))
	    (thread/wait threads))
	  (copy-block pop-queue old new report-progress)))))

(define (compute-xrefs pool)
  (let ((xrefs-config (config 'xrefs)))
    (cond ((not xrefs-config) (count-xrefs pool))
	  ((and (string? xrefs-config) (file-exists? xrefs-config))
	   (read-xrefs xrefs-config))
	  ((and (or (symbol? xrefs-config) (string? xrefs-config))
		(overlaps? (downcase xrefs-config) 
			   {"no" "none" "false" "off"}))
	   #f)
	  (else (count-xrefs pool)))))

(define (get-base-oids pool)
  (let ((base (pool-base pool))
	(cap (pool-capacity pool))
	(base-oids {}))
    (dotimes (i (+ (quotient cap #mib)
		   (if (zero? (remainder cap #mib)) 0 1)))
      (set+! base-oids (oid-plus base (* i #mib))))
    base-oids))

(define (read-xrefs file)
  (let* ((elts '())
	 (in (open-input-file file))
	 (item (read in)))
    (while (or (symbol? item) (oid? item))
      (set! elts (cons item elts))
      (set! item (read in)))
    (reverse (->vector elts))))

(define (count-xrefs pool)
  (let* ((freqs (countrefs/pool pool)))
    (rsorted (getkeys freqs) freqs)))

(define (main (from) (to))
  (default! to from)
  (config! 'cachelevel 2)
  (config! 'optlevel 4)
  (config! 'logprocinfo #t)
  (config! 'logthreadinfo #t)
  (config! 'thread:logexit #f)
  (cond ((not (bound? from)) (usage))
	((and (file-exists? to) (not (equal? from to)) (config 'COPY #f))
	 (config! 'appid (glom "copy(" (basename from) ")"))
	 (logwarn |Copying| "Copying OIDs to existing pool " (write to))
	 (copy-pool from to))
	((and (file-exists? to) 
	      (not (equal? from to))
	      (not (config 'OVERWRITE #f)))
	 (logwarn |FileExists| "Not overwriting " (write to))
	 (exit))
	((not (file-exists? from))
	 (logwarn |MissingInput| "Can't locate source " (write from))
	 (exit))
	(else (config! 'appid (glom "repack(" (basename from) ")"))
	      (repack-pool (abspath from) (abspath to)))))

(define (writable? file)
  (if (file-exists? file)
      (file-writable? file)
      (and (file-directory? (dirname file)) 
	   (file-writable? (dirname file)))))

(define (repack-pool from to)
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
	(copy-oids old new)
	(commit new)))
    (when inplace (domove from bakfile))
    (domove tmpfile to)))

(define (copy-pool from to)
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
      (copy-oids old new)
      (commit new))))

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

(when (config 'optimized #t)
  (optimize! '{knodb knodb/countrefs})
  (optimize!))

