;;; -*- Mode: Scheme -*-

(use-module '{optimize mttools logger stringfmts fifo varconfig})

(config! 'cachelevel 2)
(config! 'optlevel 4)
(config! 'logthreadinfo #t)
(config! 'thread:logexit #f)

(define (getflags)
  (choice->vector
   (choice (tryif (config 'dtypev2 #t) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64)
	   )))

(config! 'logprocinfo #t)
(config! 'logthreadinfo #t)

(define validate-oids #f)
(varconfig! VALIDATE validate-oids config:boolean)

(define prefetch-keys #t)
(varconfig! PREFECTCH prefetch-keys)

(define %loglevel %notice%)

(define last-log (elapsed-time))
(define log-freq 30)
(defslambda (dolog?)
  (cond ((< (elapsed-time last-log) log-freq) #f)
	(else (set! last-log (elapsed-time)) #t)))

;;; MT/MAP

(define (mt-iterfn fn vec args indexfn)
  (let ((vec-i (indexfn)))
    (while vec-i
      (if (null? args)
	  (fn (elt vec vec-i))
	  (apply fn (elt vec vec-i) args))
      (set! vec-i (indexfn)))))

(define (mt/iter threadcount fcn vec . args)
  (let ((i 0) (len (length vec)))
    (let ((getindex
	   (slambda ()
	     (and (< i len) (prog1 i (set! i (1+ i))))))
	  (nthreads (mt/threadcount threadcount))
	  (threads {}))
      (dotimes (i nthreads)
	(set+! threads (thread/call mt-iterfn fcn vec args getindex)))
      (thread/wait threads))))

;;; Computing baseoids 

(define (get-baseoids-from-pool pool)
  (get-baseoids-from-base (pool-base pool) (pool-capacity pool)))

(define (get-baseoids-from-base base capacity)
  (choice base
	  (tryif (> capacity 0)
		 (get-baseoids-from-base (oid-plus base capacity)
					 (- capacity (* 1024 1024))))))

(define baseoids {})

(config-def! 'baseoid (slambda (var val)
			(if (bound? val)
			    (set+! baseoids val)
			    baseoids)))
(config-def! 'pool (slambda (var val)
		     (if (bound? val)
			 (set+! baseoids (get-baseoids-from-pool val))
			 (fail))))

;;; Computing slotids

(define slotids #f)

(config-def! 'slotid
	     (slambda (var val)
	       (if (bound? val)
		   (if slotids (set! slotids (append slotids (list val)))
		       (set! slotids (list val)))
		   slotids)))

(define (count-slotid key table)
  (when (pair? key) (hashtable-increment! table (car key))))

(define (compute-slotids keyvec)
  (let ((slotids-config (config 'slotids #f)))
    (if (and (string? slotids-config) (file-exists? slotids-config))
	(append (if slotids (->vector slotids) #())
		(file->dtype slotids-config))
	(let ((table (make-hashtable))
	      (start (elapsed-time)))
	  (loginfo |PackIndex/slotids|
	    "Computing slotids based on " ($num (length keyvec)) " keys")
	  (doseq (key keyvec)
	    (when (pair? key) (hashtable-increment! table (car key))))
	  ;; (mt/iter 3 count-slotid keyvec table)
	  (lognotice |PackIndex/slotids|
	    "Found " ($num (choice-size (getkeys table)))
	    " slotids across " ($num (length keyvec)) " keys in "
	    (secs->string (elapsed-time start)))
	  (if slotids (drop! table (elts slotids)))
	  (append (if slotids (->vector slotids) #())
		  (rsorted (getkeys table) table))))))
  
;;; Other features

(define (symbolize s)
  (if (symbol? s) s
      (if (string? s) (string->symbol (upcase s))
	  (irritant s |NotStringOrSymbol|))))

(define (get-new-size nkeys)
  (config 'newsize (inexact->exact (* (config 'loadfactor 2) nkeys))))

(define (get-metadata base)
  (and (config 'metadata #f) (file->dtype (config 'metadata #f))))

(define (get-metadata)
  (and (config 'metadata #f) (file->dtype (config 'metadata #f))))

(define (make-new-index filename old keyvec (size) (type))
  (default! type (symbolize (config 'type 'hashindex)))
  (default! size (get-new-size (length keyvec)))
  (lognotice |NewIndex|
    "Constructing new index " (write filename) " based on "
    (index-source old))
  (if (eq? type 'stdindex)
      (make-index filename #[type fileindex size ,size])
      (make-index filename 
		  `#[type hashindex size ,size
		     slotids ,(compute-slotids keyvec)
		     baseoids ,(sorted baseoids)
		     metadata ,(get-metadata)]))
  (open-index filename))

(defambda (copy-all-keys keyv old new (opts #f) (start (elapsed-time)))
  (let ((batchsize #f)
	(n-batches #f)
	(n (length keyv))
	(modfn (getopt opts 'modfn #f))
	(changed 0)
	(removed 0)
	(count 0))

    (cond ((getopt opts 'batchsize) 
	   (set! batchsize (getopt opts 'batchsize)))
	  ((getopt opts 'nbatches) 
	   (set! n-batches (getopt opts 'nbatches)))
	  ((config 'batchsize)
	   (set! batchsize (config 'batchsize)))
	  ((config 'nbatches)
	   (set! n-batches (config 'nbatches)))
	  (else (set! batchsize 100000)))

    (cond (batchsize
	   (set! n-batches 
	     (+ (quotient n batchsize)
		(if (zero? (remainder n batchsize)) 0 1))))
	  (n-batches
	   (set! batchsize
	     (+ (quotient n n-batches)
		(if (zero? (remainder n n-batches)) 0 1)))))

    (let ((batches (make-vector n-batches #f))
	  (default-nthreads
	    (CONFIG 'NTHREADS (max 1 (min (rusage 'ncpus) (quotient n-batches 3)))))
	  (counter (slambda (delta (modified 0) (dropped 0))
		     (set! count (+ count delta))
		     (set! changed (+ changed modified))
		     (set! removed (+ removed dropped))
		     `#[count ,count changed ,changed removed ,removed
			nkeys ,n time ,(elapsed-time start)])))

      (dotimes (i n-batches)
	(vector-set! batches i 
		     (qc (elts keyv (* i batchsize) 
			       (min (* (1+ i) batchsize) n)))))

      (let ((fifo (fifo/make batches `#[fillfn ,fifo/exhausted!]))
	    (n-threads (mt/threadcount (getopt opts 'nthreads default-nthreads))))

	(lognotice |BatchCopy|
	  "Copying " ($num n-batches) " batches of "
	  "~" ($num batchsize) " keys using "
	  (or n-threads "no") " threads")
	    
	(if (and (number? n-threads) (> n-threads 1))
	    (let ((threads {}))
	      (dotimes (i n-threads)
		(set+! threads (thread/call copy-batches old new fifo modfn counter)))
	      (thread/wait threads))
	    (copy-batches old new fifo modfn counter))))))

(define (copy-batches from to fifo modfn counter)
  (let ((batch (fifo/pop fifo))
	(batch-start (elapsed-time))
	(modified 0)
	(dropped 0))
    (while (exists? batch)
      (let ((table (make-hashtable (* 2 (choice-size batch)))))
	(when prefetch-keys (prefetch-keys! from batch))
	(do-choices (key batch) 
	  (let ((v (if validate-oids
		       (pick (get from key) valid-oid?)
		       (get from key))))
	    (cond ((fail? v) (set! dropped (1+ dropped)))
		  ((not modfn) (store! table key (get from key)))
		  (else (let ((newv (modfn key (qc v))))
			  (cond ((identical? v newv))
				((exists? newv)
				 (set! modified (1+ modified))
				 (store! table key newv))
				(else (set! dropped (1+ dropped)))))))))
	(merge+save! to table)
	(swapout from batch))
      (let ((status (counter (choice-size batch) modified dropped)))
	(loginfo |Batch|
	  "Copied " ($num (- (choice-size batch) dropped)) " keys "
	  "in " (secs->string (elapsed-time batch-start))
	  (unless (zero? dropped)
	    (printout ", dropping " ($num dropped) " keys"))
	  (unless (zero? modified)
	    (printout ", modifying " ($num modified) " keys")))
	(when (and (> (elapsed-time last-log) log-freq) (dolog?))
	  (let ((count (get status 'count))
		(dropped (try (get status 'dropped) 0))
		(modified (try (get status 'modified) 0))
		(time (get status 'time))
		(n (get status 'nkeys)))
	    (lognotice |Overall|
	      "Processed " ($num count) " keys" " (" (show% count n) ") "
	      " in " (secs->string (get status 'time))
	      (unless (zero? (- count dropped modified))
		(printout ", copying " ($num (- count dropped modified)) " keys"))
	      (unless (zero? dropped)
		(printout ", dropping " ($num dropped) " keys"))))
	  (unless (zero? modified)
	    (printout ", modifying " ($count modified) " keys")))
	(set! batch (fifo/pop fifo))
	(set! batch-start (elapsed-time))
	(set! modified 0)
	(set! dropped 0)))))

(defslambda (merge+save! index table (start (elapsed-time)))
  (logdebug |Merge+Save|
    "Merging " (table-size table) " into " index)
  (index-merge! index table)
  (logdebug |Merge+Save|
    "Saving " (table-size table) " merged keys in " index)
  (commit index)
  (swapout index)
  (loginfo |Merge+Save|
    "Merged and saved " (table-size table) " for " index
    " in " (secs->string (elapsed-time start))))

(define (main from (to #f))
  (cond ((not to) (repack-index from))
	((not (file-exists? from))
	 (logcrit |PackIndex| "Can't locate source " from))
	((and (file-exists? to) (not (config 'OVERWRITE #f)))
	 (logcrit |PackIndex| "Not overwriting " to))
	(else
	 (when (file-exists? to) (remove-file to))
	 (lognotice |PackIndex| "Copying index " from " into " to)
	 (let* ((old (open-index from))
		(keyv (index-keysvec old))
		(new (make-new-index to old keyv)))
	   (copy-all-keys keyv old new #f)
	   (commit new)
	   new))))

(define (repack-index from)
  (let* ((base (basename from))
	 (tmpfile (or (config 'TMPFILE #f)
		      (and (config 'TMPDIR #f)
			   (mkpath (config 'TMPDIR) (glom base ".part")))
		      (glom base ".part")))
	 (bakfile (or (config 'BAKFILE #f)
		      (string-append base ".bak"))))
    (lognotice |PackIndex| 
      "Repacking the index " from " using " tmpfile 
      " and saving original in " bakfile)
    (let* ((old (open-index from))
	   (keyv (index-keysvec old))
	   (new (make-new-index tmpfile old keyv)))
      (copy-all-keys keyv old new)
      (commit new))
    (onerror (move-file from bakfile)
	     (lambda (ex) (system "mv " from " " bakfile)))
    (onerror (move-file tmpfile from)
	     (lambda (ex) (system "mv " tmpfile " " from)))))

(optimize!)

