;;; -*- Mode: Scheme -*-

(use-module '{optimize mttools varconfig stringfmts logger 
	      storage/flexpools})

(define %loglevel %notice%)
(config! 'cachelevel 2)
(config! 'optlevel 4)
(config! 'logthreadinfo #t)
(config! 'thread:logexit #f)

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
  #[bigpool snappy oidpool zlib filepool #f])

(define (make-new-pool filename old 
		       (type (symbolize (config 'pooltype 'bigpool))))
  (let ((metadata (or (poolctl old 'metadata) #[])))
    (make-pool filename `#[type ,type
			   base ,(pool-base old)
			   capacity ,(config 'newcap (pool-capacity old))
			   load ,(config 'newload (pool-load old))
			   label ,(config 'label (pool-label old))
			   slotids ,(get-slotids metadata type)
			   compression ,(get-compression metadata type)
			   dtypev2 ,(config 'dtypev2 (test metadata 'flags 'dtypev2))
			   isadjunct ,(config 'ISADJUNCT (test metadata 'flags 'isadjunct))
			   register #t])))

(define (get-slotids metadata type (current) (added (config 'slotids {})))
  (set! current (getopt metadata 'slotids #()))
  (append current (choice->vector (difference added (elts current)))))
(define (get-compression metadata type)
  (symbolize (config 'compression (try (get ztype-map type) #f))))

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

(define (copy-block queuefn old new (msg #f))
  (let ((oids (queuefn))
	(started (elapsed-time)))
    (while (exists? oids)
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
      " into " (or (pool-source new) new))
  (let* ((started (elapsed-time))
	 (newload (pool-load new))
	 (alloids (or (poolctl old 'keys) (pool-elts old)))
	 (oids (if (= (pool-load old) newload) alloids
		   (reject alloids < (oid-plus (pool-base old) newload))))
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
      " into " (or (pool-source new) new)
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

(define (main (from) (to #f) (capacity #f))
  (when (number? to)
    (if (string? capacity)
	(let ((usecap to))
	  (set! to capacity)
	  (set! capacity usecap))))
  (cond ((not (bound? from)) (usage))
	((not (file-exists? from))
	 (logcrit |MissingInput| "The file " (write from) " does not exist"))
	((and to (file-exists? to) (not (config 'OVERWRITE #f)))
	 (logwarn |NotOverwriting| "Existing file " to))
	((and (not to) (file-exists? (glom from ".bak")))
	 (logcrit |BackupFile|
	   "I don't want to delete the backup file " (write (glom from ".bak"))))
	((not to) 
	 (move-file from (glom from ".bak"))
	 (flexpool/split from (or capacity (* 1024 1024)) from))
	(else (flexpool/split from (or capacity (* 1024 1024)) to))))

(define (usage)
  (message "split-pool *source-pool* *output* [capacity]"))

(optimize! 'storage/flexpools)
(optimize!)

