;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/splitpool)

(use-module '{reflection texttools regex varconfig
	      logger logctl fifo
	      mttools stringfmts opts})
(use-module '{storage/flex storage/adjuncts})

(module-export! 'flexpool/split)

(define %loglevel %notice%)

(define default-batchsize
  (if (> (rusage 'physical-memory) 16000000000)
      0x100000
      0x40000))
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
	 (flexcap (getopt opts 'usecap (CONFIG 'NEWCAP (pool-capacity input))))
	 (flexload (getopt opts 'useload (config 'NEWLOAD (pool-load input))))
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
	   (subpools (flex/pools flexpool)))
      (do-choices (subpool subpools)
	(copy-oids input subpool (pool-vector subpool)
		   batchsize nthreads)))
    #f))

(defambda (copy-oids from to oidvec batchsize nthreads
		     (copy-start (elapsed-time)))
  (let* ((fifo (fifo/make (make-batches oidvec batchsize)
			  `#[fillfn ,fifo/exhausted!]))
	 (counter 0)
	 (countup (slambda (n)
		    (set! counter (+ counter n))
		    (cons counter (elapsed-time copy-start)))))
    (loginfo |CopyOIDs|
      "Copying " ($num (length oidvec)) " OIDs in batches of "
      ($num batchsize) " OIDs using " nthreads " threads.")
    (if (and nthreads (> nthreads 1))
	(let ((threads {})
	      (threadcount (min nthreads (fifo/load fifo))))
	  (dotimes (i threadcount)
	    (set+! threads (thread/call copier fifo from to countup)))
	  (thread/wait threads))
	(copier fifo from to countup))
    (swapout from)
    (commit to)
    (swapout to)))

(define (make-batches oidvec batchsize (start 0) (end))
  (default! end (length oidvec))
  (let* ((range (- end start))
	 (n-batches 
	  (+ (quotient range batchsize)
	     (if (zero? (remainder range batchsize)) 0 1)))
	 (batches (make-vector n-batches #f)))
    (loginfo |CopyBatches| 
      "Created " n-batches " of at least " ($num batchsize) " OIDs "
      "from " (oid->string (elt oidvec start)) " "
      "to " (oid->string (elt oidvec (-1+ end))))
    (dotimes (i n-batches)
      (vector-set! batches i 
		   (qc (elts oidvec
			     (+ start (* i batchsize))
			     (min (+ start (* (1+ i) batchsize))
				  end)))))
    batches))

(define (copier fifo from to countup)
  (loginfo |CopyThread/Init| 
    "Copying OIDs from " (write (pool-source from)) 
    " to " (write (pool-source to))
    " reading batches from " fifo)
  (let ((batch (fifo/pop fifo))
	(cycle-start (elapsed-time)))
    (while (exists? batch)
      (logdebug |CopyThread/Batch| "Copying " (choice-size batch) " OIDs")
      (pool-prefetch! from batch)
      (do-choices (oid batch)
	(store! to oid (get from oid)))
      (logdebug |CopyThread/Swapout|
	"Swapping out " (choice-size batch) " OIDs from " from)
      (swapout from batch)
      (logdebug |CopyThread/Commit| "Committing changes to " to)
      (commit to)
      (let ((progress (countup (choice-size batch))))
	(loginfo |CopyThread|
	  "Copied " (choice-size batch) "/" (car progress) " OIDs to " to 
	  " in " (->exact (elapsed-time cycle-start)) "/" (->exact (cdr progress)) 
	  " seconds"))
      (set! batch (fifo/pop fifo))
      (set! cycle-start (elapsed-time)))))


