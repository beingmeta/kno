;;; -*- Mode: Scheme -*-

(use-module '{optimize mttools logger stringfmts fifo varconfig})

(config! 'cachelevel 2)
(config! 'optlevel 4)
(config! 'logprocinfo #t)

(define (getflags)
  (choice->vector
   (choice (tryif (config 'dtypev2 #t) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64)
	   )))

(config! 'logprocinfo #t)
(config! 'logthreadinfo #t)

(define %loglevel %notice%)

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

(defambda (copy-all-keys keys old new (opts #f) (start (elapsed-time)))
  (let ((batchsize (getopt opts 'batchsize (config 'batchsize #f)))
	(n-batches (getopt opts 'nbatches (config 'nbatches #f)))
	(n-threads (mt/threadcount (getopt opts 'nthreads (config 'nthreads #default))))
	(n (choice-size keys))
	(count 0))
    (cond (batchsize 
	   (set! n-batches 
	     (+ (quotient n batchsize)
		(if (zero? (remainder n batchsize)) 0 1))))
	  (n-batches 
	   (set! batchsize
	     (if (zero? (remainder n n-batches))
		 (quotient n n-batches)
		 (1+ (quotient n n-batches)))))
	  (else (set! batchsize 100000)))
    (let ((batches (make-vector n-batches))
	  (counter (slambda (n)
		     (set! count (+ count n))
		     count)))
      (dotimes (i n-batches)
	(vector-set! batches i (qc (pick-n keys batchsize (* i batchsize)))))
      (let ((fifo (fifo/make batches `#[fillfn ,fifo/exhausted!])))
	(if (and (number? n-threads) (> n-threads 1))
	    (let ((threads {}))
	      (dotimes (i n-threads)
		(thread/call copy-batches old new fifo counter))
	      (thread/wait threads))
	    (copy-batches old new fifo counter))))))

(defslambda (merge+save! index table)
  (index-merge! index table)
  (commit))

(define (copy-batches from to fifo counter)
  (let ((batch (fifo/pop fifo))
	(batch-start (elapsed-time)))
    (while (exists? batch)
      (let ((table (make-hashtable (* 2 (choice-size batch)))))
	(prefetch-keys! from batch)
	(do-choices (key batch) 
	  (store! table key (get from key)))
	(merge+save! to table)
	(swapout from batch))
      (let ((count.time (counter (choice-size batch))))
	(lognotice |Batch|
	  "Copied " ($num (choice-size batch)) " in " (secs->string (elapsed-time batch-start)))
	(lognotice |Overall|
	  "Copied " ($num (car count.time)) 
	  " in " (secs->string (elapsed-time (cdr count.time))))
	(set! batch (fifo/pop fifo))
	(set! batch-start (elapsed-time))))))

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
	   (copy-all-keys keyv old new)))))

(define (repack-index from)
  (let* ((base (basename from))
	 (tmpfile (or (config 'TMPFILE #f)
		      (and (config 'TMPDIR #f)
			   (mkpath (config 'TMPDIR)
				   (string-append base ".tmp")))
		      (string-append base ".tmp")))
	 (bakfile (or (config 'BAKFILE #f)
		      (string-append base ".bak"))))
    (lognotice |PackIndex| 
      "Repacking the index " from " using " tmpfile 
      " and saving original in " bakfile)
    (let* ((old (open-index from))
	   (keyv (index-keysvec old))
	   (new (make-new-index tmpfile old keyv)))
      (copy-all-keys keyv old new))
    (onerror (move-file from bakfile)
	     (lambda (ex) (system "mv " from " " bakfile)))
    (onerror (move-file tmpfile from)
	     (lambda (ex) (system "mv " tmpfile " " from)))))

(optimize!)
