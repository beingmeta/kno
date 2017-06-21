;;; -*- Mode: Scheme -*-

(config! 'cachelevel 2)
(use-module '{optimize mttools varconfig logger})

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
  (if (symbol? s) s
      (if (string? s) (string->symbol (upcase s))
	  (irritant s |NotStringOrSymbol|))))

(define ztype-map
  #[bigpool snappy oidpool zlib filepool #f])

(define (make-new-pool filename old 
		       (type (symbolize (config 'pooltype 'bigpool))))
  (make-pool filename `#[type ,type
			 base ,(pool-base old)
			 capacity ,(config 'newcap (pool-capacity old))
			 load ,(config 'newload (pool-load old))
			 label ,(config 'label (pool-label old))
			 compression 
			 ,(symbolize (config 'compression (try (get ztype-map type) #f)))
			 dtypev2 ,(config 'dtypev2 #f)
			 register #t]))

(define (copy-oids old new)
  (message "Copying OIDs" (if (pool-label old)
			      (append " for " (pool-label old)))
	   " from " (or (pool-source old) old)
	   " into " (or (pool-source new) new))
  (let ((prefetcher (lambda (oids done)
		      (when done (commit) (clearcaches))
		      (unless done
			(pool-prefetch! old oids)
			(lock-oids! oids))))
	(progress-label 
	 (if (pool-label old)
	     (string-append "Copying " (pool-label old))
	     "Copying OIDs")))
    (do-choices-mt (f (pool-elts old) (config 'nthreads 4)
		      prefetcher (config 'blocksize 50000)
		      (mt/custom-progress progress-label))
      (set-oid-value! f (get old f)))))

(define (main (from) (to #f))
  (cond ((not (bound? from)) (usage))
	((not to) (repack-pool from))
	((and (file-exists? to) (not (config 'OVERWRITE #f)))
	 (message "Not overwriting " to))
	((not (file-exists? from))
	 (message "Can't locate source " from))
	(else
	 (when (file-exists? to) (remove-file to))
	 (let* ((old (open-pool from #[register #f cachelevel 3]))
		(new (make-new-pool to old)))
	   (copy-oids old new)))))

(define (repack-pool from)
  (let* ((base (basename from))
	 (tmpfile (or (config 'TMPFILE #f)
		      (and (config 'TMPDIR #f)
			   (mkpath (config 'TMPDIR)
				   (string-append base ".tmp")))
		      (string-append from ".tmp")))
	 (bakfile (or (config 'BAKFILE #f)
		      (string-append from ".bak"))))
    (let* ((old (open-pool from #[register #f cachelevel 3]))
	   (new (make-new-pool tmpfile old)))
      (copy-oids old new))
    (onerror (move-file from bakfile)
	     (lambda (ex) (system "mv " from " " bakfile)))
    (onerror (move-file tmpfile from)
	     (lambda (ex) (system "mv " tmpfile " " from)))))

(define (usage)
  (lineout "Usage: pack-pool <from> [to]")
  (lineout "    Repacks the file pool stored in <from>.  The new file ")
  (lineout "    pool is either replace <from> or is written into [to].")
  (lineout "    [to] if specified must not exist unless OVERWRITE=yes")
  (lineout "    OLDPOOL=yes generates a standard file pool")
  (lineout "    OIDPOOL=yes generates a new model OID pool"))

(optimize! copy-oids)

