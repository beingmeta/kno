;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'knodb/splitpool)

(use-module '{kno/reflect texttools regex varconfig
	      logger logctl fifo
	     kno/mttools text/stringfmts opts})
(use-module '{knodb knodb/flexpool knodb/adjuncts})

(module-export! 'flexpool/split)

(define maxmem (quotient (rusage 'physical-memory) 2))

(define %loglevel %info%)

(define default-batchsize 0x40000)
(varconfig! BATCHSIZE default-batchsize)

(define (get-padlen cap chunk)
  (let ((n-chunks (quotient (or cap 0x100000000) chunk))
	(digits 1)
	(n 16))
    (while (<= n n-chunks)
      (set! digits (1+ digits))
      (set! n (* n 16)))
    digits))

;;; Splitting existing pools into smaller flexpools

(define (flexpool/split original (opts #f) (step) (output) (rootdir) (prefix))
  (default! step (getopt opts 'step (config 'FLEXSTEP 0x100000)))
  (default! output 
    (abspath
     (getopt opts 'output 
	     (config 'OUTPUT 
		     (glom (strip-suffix (abspath original) ".pool")
		       ".flexpool")))))
  (default! rootdir 
    (getopt opts 'rootdir (config 'ROOTDIR (dirname output))))
  (default! prefix 
    (getopt opts 'prefix (config 'PREFIX (textsubst (basename output) #("." (isalpha+) (eos)) ""))))
  (let* ((input (open-pool original (cons #[adjunct #t] opts)))
	 (flexbase (pool-base input))
	 (flexcap (getopt opts 'newcap (CONFIG 'NEWCAP (pool-capacity input))))
	 (flexload (getopt opts 'newload (config 'NEWLOAD (pool-load input))))
	 (metadata (poolctl input 'metadata))
	 (label (config 'LABEL (pool-label input)))
	 (source (pool-source input))
	 (padlen (get-padlen flexcap step)))

    (let* ((opts+ `#[base ,flexbase 
		     prefix ,prefix
		     capacity ,flexcap
		     load ,flexload
		     rootdir ,rootdir
		     metadata ,(poolctl input 'metadata)
		     step ,step
		     build ,(timestamp)])
	   (batchsize (getopt opts 'batchsize default-batchsize))
	   (nthreads (getopt opts 'nthreads (rusage 'ncpus)))
	   (flexpool (flexpool/make output (cons opts+ opts))))
      (let ((count 0)
	    (start (elapsed-time))
	    (total-time 0.0))
	(let ((logcopy (slambda (subpool n total time)
			 (loginfo |Copied|
			   "Took " (secs->string time) " to copy "
			   ($num n) " (of " ($num total) ") OIDs to " subpool)
			 (set! total-time (+ total-time time))
			 (set! count (+ n count))
			 (lognotice |Progress|
			   "Overall, copied " ($num count) 
			   " (" (show% count flexload) " of " ($num flexload) ") OIDs "
			   " in " (secs->string (elapsed-time start)) 
			   " (aggregate " total-time ") or "
			   ($num (/~ count (/~ (elapsed-time start) 60)) 1) " OIDs/minute"))))
	      (let ((threads {})
		    (fifo (fifo/make (choice->vector (flexpool/partitions flexpool))
				     `#[fillfn ,fifo/exhausted!])))
		(cond ((and nthreads (> nthreads 1))
		       (dotimes (i nthreads)
			 (set+! threads (thread/call copy-subpool input fifo batchsize logcopy)))
		       (thread/wait threads))
		      (else (copy-subpool input fifo batchsize logcopy)))))))
    #f))

(define (copy-subpool from fifo batchsize (logcopy #f))
  (let ((to (fifo/pop fifo)))
    (while (and (exists? to) to)
      (let* ((oids (pool-vector to))
	     (n (length oids))
	     (n-batches (1+ (quotient n batchsize)))
	     (subpool-start (elapsed-time)))
	(lognotice |Subpool/Start| 
	  "Copying " ($num n) " OIDs in "
	  ($num n-batches) " batches of up to "  ($num batchsize)
	  " OIDs into " to)
	(dotimes (i n-batches)
	  (let* ((batch-started (elapsed-time))
		 (oidvec (slice oids (* i batchsize)
				(min (* (1+ i) batchsize) (length oids))))
		 (valvec (pool/fetchn from oidvec)))
	    (pool/storen! to oidvec valvec)
	    (when logcopy (logcopy to (length oidvec) n (elapsed-time batch-started)))))
	(commit to)
	(swapout to)
	(poolctl to 'cachelevel 0)
	(lognotice |Subpool/Done| 
	  "Copied " ($num n) " OIDs into " to " in " (secs->string (elapsed-time subpool-start))
	  " (" ($num (/~ n (/~ (elapsed-time subpool-start) 60)) 2) " OIDs/minute)"))
      (set! to (fifo/pop fifo)))))

(defambda (copy-oids from fifo nthreads (copy-start (elapsed-time)))
  (lognotice |CopyOIDs| 
    "Copying " (fifo/load fifo) " batches of OIDs from " from
    " using " (or nthreads "no") " threads")
  (let* ((counter 0)
	 (progress (make-hashtable))
	 (countup (slambda (to n)
		    (set! counter (+ counter n))
		    (hashtable-increment! progress to n)
		    (when (>= (get progress to) (pool-load to))
		      (lognotice |PartitionFinished| to)
		      (commit to)
		      (poolctl to 'cachelevel 1)
		      (swapout to))
		    (cons counter (elapsed-time copy-start)))))
    (if (and nthreads (> nthreads 1))
	(let ((threads {})
	      (threadcount (min nthreads (fifo/load fifo))))
	  (dotimes (i threadcount)
	    (set+! threads (thread/call copier fifo from countup)))
	  (thread/wait threads))
	(copier fifo from countup))))

(defambda (make-batches subpools poolwidth batchsize (start 0) (end))
  (let ((batchlist '()))
    (do-choices (subpool subpools)
      (let* ((oidvec (pool-vector subpool))
	     (n (length oidvec))
	     (start 0))
	(while (< start n)
	  (set! batchlist
	    (cons (cons subpool (slice oidvec start 
				       (min (+ start batchsize)
					    (length oidvec))))
		  batchlist))
	  (set! start (+ start batchsize)))))
    (reverse (->vector batchlist))))

(define (copier fifo from countup)
  (let* ((cycle-start (elapsed-time))
	 (item (fifo/pop fifo))
	 (to (car item))
	 (oidvec (cdr item)))
    (while (exists? item)
      (logdebug |CopyThread/Batch| "Copying " (length oidvec) " OIDs")
      
      (let ((progress (countup to (length oidvec))))
	(loginfo |CopyThread|
	  "Copied " (length oidvec) "/" (car progress) " OIDs to " to 
	  " in " (secs->string (elapsed-time cycle-start)) "/" (secs->string (cdr progress)) 
	  " seconds"))
      (set! item (fifo/pop fifo))
      (set! to (car item))
      (set! oidvec (cdr item))
      (when (> (memusage) maxmem) (pause-copy! fifo from))
      (set! cycle-start (elapsed-time)))))

(defslambda (pause-copy! fifo from)
  (when (> (memusage) maxmem)
    (logwarn |PauseCopy| "Saving out any cached state")
    (fifo/pause! fifo 'read)
    (commit) (swapout)
    (poolctl from 'cachelevel 1)
    (poolctl from 'cachelevel 2)
    (set! maxmem (+ (memusage) (quotient maxmem 4)))
    (fifo/pause! fifo #f)))


