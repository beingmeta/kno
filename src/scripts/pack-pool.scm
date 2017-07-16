;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module '{optimize mttools varconfig stringfmts logger})

(define %loglevel %info%)

(define (getflags)
  (choice->list
   (choice (tryif (config 'dtypev2 #f) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64)
	   (tryif (config 'ZLIB #f) 'ZLIB)
	   (tryif (config 'SNAPPY #f) 'SNAPPY))))

(define default-schematize #t)

(define (get-schemas old)
  (and (or (and default-schematize (not (config 'NOSCHEMAS #f)))
	   (config 'SCHEMAS #f) (config 'SCHEMATIZE #f))
       (if (and (config 'schemafile #f)
		(string? (config 'schemafile #f))
		(file-exists? (config 'schemafile #f)))
	   (begin
	     (message "Using existing schemas from " (config 'schemafile #f))
	     (file->dtype (config 'schemafile #f)))
	   (let ((table (make-hashtable)))
	     (message "Identifying schemas from " (pool-load old) " OIDs "
	       "in " (or (pool-source old) old))
	     (do-choices-mt (f (pool-elts old) 
			       (mt/threadcount (config 'nthreads 0.7))
			       (lambda (oids done)
				 (when done (clearcaches))
				 (unless done (prefetch-oids! old oids)))
			       (config 'blocksize (quotient (pool-load old) 10))
			       (mt/custom-progress "Identifying schemas"))
	       (hashtable-increment! table (sorted (getkeys (get old f)))))
	     (let* ((default-threshold (max (quotient (table-maxval table) 4) 2))
		    (threshold (config 'schemathresh default-threshold))
		    (schemas (getkeys table))
		    (picked (table-skim table threshold))
		    (sum (reduce-choice + picked 0 table)))
	       (message "Identified " (choice-size picked) 
		 " repeated schemas covering " sum " objects out of "
		 (choice-size schemas) " overall")
	       (if (config 'schemafile #t)
		   (let ((schemas (rsorted (getkeys table) table)))
		     (when (and (config 'schemafile #f)
				(string? (config 'schemafile #f)))
		       (message "Writing schemas to " (config 'schemafile #f))
		       (dtype->file schemas (config 'schemafile #t)))
		     schemas)
		   (rsorted picked table)))))))

(define (symbolize s)
  (if (or (symbol? s)  (number? s)) s
      (if (string? s) (string->symbol (upcase s))
	  (irritant s |NotStringOrSymbol|))))

(define ztype-map
  #[bigpool zlib oidpool zlib filepool #f])

(define (make-new-pool filename old 
		       (type (symbolize (config 'pooltype 'bigpool))))
  (let ((metadata (or (poolctl old 'metadata) #[])))
    (make-pool filename `#[type ,type
			   base ,(pool-base old)
			   capacity ,(config 'newcap (pool-capacity old))
			   load ,(config 'newload (pool-load old))
			   label ,(config 'label (pool-label old))
			   compression 
			   ,(symbolize (config 'compression
					       (try (get ztype-map type) #f)))
			   dtypev2 ,(config 'dtypev2 (test metadata 'flags 'dtypev2))
			   isadjunct ,(config 'ISADJUNCT (test metadata 'flags 'isadjunct))
			   register #t])))

(define (get-batchsize n)
  (cond ((and (config 'batchsize) (< (config 'batchsize) 1))
	 (* n (config 'batchsize)))
	((and (config 'batchsize) (> (config 'batchsize) 1))
	 (config 'batchsize))
	((config 'ncycles) (/ n (config 'ncycles)))
	(else 100000)))

(define last-save (elapsed-time))
(define save-interval 30)
(define saving #f)

(defslambda (getsavelock)
  (cond (saving #f)
	(else (set! saving (timestamp))
	      #t)))

(define (dosave old new)
  (commit new)
  (swapout old)
  (swapout new)
  (set! last-save (elapsed-time))
  (set! saving #f))

(define (copy-block queuefn old new)
  (let ((oids (queuefn)))
    (while (exists? oids)
      (pool-prefetch! old oids)
      (do-choices (oid oids)
	(set-oid-value! oid (get old oid)))
      (commit new)
      (swapout old oids)
      (set! oids (queuefn)))))

(define (copy-oids old new)
  (loginfo |CopyOIDS|
      "Copying OIDs" (if (pool-label old)
			 (append " for " (pool-label old)))
      " from " (or (pool-source old) old)
      " into " (or (pool-source new) new))
  (let* ((started (elapsed-time))
	 (newload (pool-load new))
	 (alloids (or (poolctl old 'keys) (pool-elts old)))
	 (oids (if (= (pool-load old) newload) alloids
		   (reject alloids < (oid-plus (pool-base old) newload))))
	 (blocksize (config 'blocksize 100000))
	 (nblocks (1+ (quotient (choice-size oids) blocksize)))
	 (queue '()))
    (dotimes (i nblocks)
      (set! queue (cons (qc (pick-n oids blocksize (* i blocksize)))
			queue)))
    (let ((pop-queue
	   (slambda ()
	     (tryif (pair? queue)
	       (prog1 (car queue) (set! queue (cdr queue))))))
	  (threads {}))
      (dotimes (i (mt/threadcount))
	(set+! threads (thread/call copy-block pop-queue old new)))
      (thread/wait threads))))

(define (main (from) (to #f))
  (cond ((not (bound? from)) (usage))
	((not to) (repack-pool from))
	((and (file-exists? to) (not (config 'OVERWRITE #f)))
	 (message "Not overwriting " to))
	((not (file-exists? from))
	 (message "Can't locate source " (write from)))
	(else
	 (when (file-exists? to) (remove-file to))
	 (let* ((old (open-pool from #[register #f cachelevel 2]))
		(new (make-new-pool to old)))
	   (copy-oids old new)
	   (commit new)))))

(define (repack-pool from)
  (let* ((base (basename from))
	 (tmpfile (or (config 'TMPFILE #f)
		      (and (config 'TMPDIR #f)
			   (mkpath (config 'TMPDIR)
				   (string-append base ".tmp")))
		      (string-append from ".tmp")))
	 (bakfile (or (config 'BAKFILE #f)
		      (string-append from ".bak"))))
    (when (file-exists? tmpfile)
      (if (config 'RESTART)
	  (remove-file tmpfile)
	  (begin
	    (logwarn |ExistingTMP| 
	      "The temporary file " (write tmpfile) " already exists. "
	      "Remove it or specify the config RESTART=yes to ignore")
	    (exit))))
    (let* ((old (open-pool from #[register #f cachelevel 2]))
	   (new (make-new-pool tmpfile old)))
      (copy-oids old new)
      (commit new))
    (onerror (move-file from bakfile)
	     (lambda (ex) (system "mv " from " " bakfile)))
    (onerror (move-file tmpfile from)
	     (lambda (ex) (system "mv " tmpfile " " from)))))

(define (usage)
  (lineout "Usage: pack-pool <from> [to]")
  (lineout "    Repacks the file pool stored in <from>.  The new file ")
  (lineout "    pool is either replace <from> or is written into [to].")
  (lineout "    [to] if specified must not exist unless OVERWRITE=yes.")
  (lineout "    POOLTYPE=bigpool|oidpool|filepool"))

(optimize!)

