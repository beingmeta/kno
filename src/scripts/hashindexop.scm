;;; -*- Mode: Scheme -*-

(use-module '{optimize kno/mttools logger text/stringfmts fifo varconfig})

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

(define get-keyinfo 
  (ffi/proc "kno_hashindex_keyinfo" #f 'lisp 'lisp 'lisp 'lisp))

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

(defambda (compute-slotids keyinfo)
  (let ((slotids-config (config 'slotids #f)))
    (if (and (string? slotids-config) (file-exists? slotids-config))
	(append (if slotids (->vector slotids) #())
		(file->dtype slotids-config))
	(let ((table (make-hashtable))
	      (start (elapsed-time)))
	  (loginfo |PackIndex:slotids|
	    "Computing slotids based on " ($num (choice-size keyinfo)) " keys")
	  (do-choices (info keyinfo)
	    (when (pair? (get info 'key))
	      (hashtable-increment! table (car (get info 'key)) (get info 'count))))
	  (lognotice |PackIndex:slotids|
	    "Found " ($num (choice-size (getkeys table)))
	    " slotids across " ($num (choice-size keyinfo)) " keys in "
	    (secs->string (elapsed-time start)))
	  (if slotids (drop! table (elts slotids)))
	  (append (if slotids (->vector slotids) #())
		  (rsorted (getkeys table) table))))))
  
;;; Other features

(unless (bound? symbolize)
  (define (symbolize s)
    (if (symbol? s) s
	(if (string? s) (string->symbol (upcase s))
	    (irritant s |NotStringOrSymbol|)))))

(define (get-new-size nkeys)
  (config 'newsize (inexact->exact (* (config 'loadfactor 2) nkeys))))

(define (get-metadata base)
  (and (config 'metadata #f) (file->dtype (config 'metadata #f))))

(define (get-metadata)
  (and (config 'metadata #f) (file->dtype (config 'metadata #f))))

(define (make-new-index filename old keyinfo (size) (type))
  (default! type (symbolize (config 'type 'hashindex)))
  (default! size (get-new-size (choice-size keyinfo)))
  (lognotice |NewIndex|
    "Constructing new index " (write filename) " based on "
    ($num (choice-size keyinfo)) " keys from " (index-source old))
  (if (eq? type 'stdindex)
      (make-index filename #[type fileindex size ,size])
      (make-index filename 
		  `#[type hashindex size ,size
		     slotcodes ,(compute-slotids keyinfo)
		     oidcodes ,(sorted baseoids)
		     metadata ,(get-metadata)]))
  (lognotice |NewIndex| 
    "Created new index with " ($num size) " buckets in " filename)
  (open-index filename))

(defambda (copy-all-keys keyinfo old new (opts #f))
  (let ((batchsize (getopt opts 'batchsize (config 'batchsize 100000)))
	(default-nthreads (CONFIG 'NTHREADS (max 1 (rusage 'ncpus))))
	(bucketmap (make-hashtable (choice-size keyinfo)))
	(total (choice-size keyinfo))
	(new-size (indexctl new 'hash))
	(batches '()))

    (lognotice |BatchCopy|
      "Organizing " ($num (choice-size keyinfo)) " keys into "
      "batches of > " ($num batchsize) " values")

    (do-choices (info keyinfo)
      (add! bucketmap (remainder (get info 'hash) new-size)
	    info))

    (let ((batch {})
	  (size 0))
      (do-choices (bucket (getkeys bucketmap))
	(let* ((keys (get bucketmap bucket))
	       (n-values (reduce-choice + keys 0 'count)))
	  (cond ((zero? size)
		 (set! batch (get keys 'key))
		 (set! size n-values))
		((> (+ size n-values) batchsize)
		 (set! batches (cons (qc batch) batches))
		 (set! batch (get keys 'key))
		 (set! size n-values))
		(else
		 (set+! batch (get keys 'key))
		 (set! size (+ size n-values))))))
      (when (exists? batch)
	(set+! batch (cons (qc batch) batches)))
      (set! batches (->vector batches)))

    (let ((fifo (fifo/make batches))
	  (n-threads (mt/threadcount (getopt opts 'nthreads default-nthreads)))
	  (modfn (getopt opts 'modfn #f))
	  (changed 0)
	  (removed 0)
	  (count 0)
	  (start (elapsed-time)))
      (let ((counter (slambda (delta (modified 0) (dropped 0))
		       (set! count (+ count delta))
		       (set! changed (+ changed modified))
		       (set! removed (+ removed dropped))
		       `#[count ,count changed ,changed removed ,removed
			  nkeys ,total time ,(elapsed-time start)])))
	(lognotice |BatchCopy|
	  "Copying " ($num (length batches)) " batches of "
	  "~" ($num batchsize) " values using "
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
	      (unless (zero? dropped)
		(printout ", dropping " ($num dropped) " keys"))
	      (unless (zero? modified)
		(printout ", modifying " ($count modified) " keys")))))
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

(define (writable? file)
  (if (file-exists? file)
      (file-writable? file)
      (file-writable? (dirname (realpath (abspath file))))))

(define (main from (to))
  (default! to from)
  (cond ((not (file-exists? from))
	 (logcrit |PackIndex| "Can't locate source " from))
	((and (file-exists? to) (not (equal? to from))
	      (not (config 'OVERWRITE #f)))
	 (logcrit |PackIndex| "Not overwriting " to))
	((not (writable? to))
	 (logcrit |PackIndex| "Can't write output file " to))
	(else (repack-index from to))))

(define (file-mover src dest)
  (onerror (move-file src dest)
      (lambda (err1)
	(logwarn |FirstMoveFailed| "Couldn't MOVE " src " to " dest)
	(onerror (system "mv " (abspath src) " " (abspath dest))
	    (lambda (err2)
	      (logwarn |SecondMoveFailed| "Couldn't just 'mv' " src " to " dest)
	      (reraise err1))))))
				 
(define (repack-index from to)
  (let* ((tmpfile (or (config 'TMPFILE #f) (glom to ".part")))
	 (repack (equal? (realpath from) (realpath to)))
	 (bakfile (config 'BAKFILE (and repack (glom from ".bak"))))
	 (started (elapsed-time)))
    (lognotice |PackIndex| 
      (if repack "Repacking " "Copying ") from 
      (unless repack (printout " into " to))
      " via " tmpfile)
    (when (file-exists? tmpfile) (remove-file tmpfile))
    (pack-index from tmpfile)
    (lognotice |PackIndex|
      "Finished " (if repack "repacking " "copying ")
      from
      (unless repack (printout " into " tmpfile))
      " in " (secs->string (elapsed-time started)))
    (when bakfile 
      (lognotice |PackIndex| 
	"Saving original " (write from) " into " (write bakfile))
      (file-mover from bakfile))
    (file-mover tmpfile to)))

(define (pack-index from tofile)
  (let* ((cur (open-index from))
	 (keyinfo 
	  (begin (lognotice |GatherKeyInfo| "From " cur)
	    (get-keyinfo cur (config 'MINCOUNT) (config 'MAXCOUNT))))
	 (new (make-new-index tofile cur (qc keyinfo))))
    (copy-all-keys keyinfo cur new #f)
    (commit new)
    new))

(optimize!)

