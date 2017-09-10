;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/splitpool)

(use-module '{reflection texttools regex varconfig
	      logger logctl fifo
	      mttools stringfmts opts})
(use-module '{storage/flex storage/adjuncts})

(module-export! 'flexpool/split)

(define maxmem (quotient (rusage 'physical-memory) 2))

(define %loglevel %notice%)

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
	   (flexpool (flexpool/make output (cons opts+ opts)))
	   (subpools (flex/pools flexpool))
	   (batches (make-batches subpools batchsize))
	   (fifo (fifo/make (make-batches subpools batchsize)
			    `#[fillfn ,fifo/exhausted!])))
      (copy-oids input fifo nthreads))
    #f))

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

(defambda (make-batches subpools batchsize (start 0) (end))
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
      (let ((valvec (pool/fetchn from oidvec)))
	(pool/storen! to oidvec valvec))
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


