;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'knodb/flexpool/split)

(use-module '{binio db/drivers texttools})
(use-module '{text/stringfmts logger varconfig fifo})
(use-module '{knodb/adjuncts knodb/filenames knodb/flexpool})
(use-module '{knodb})

(module-export! 'flexpool/split)

(define %loglevel %notice%)

(define-init maxmem (quotient (rusage 'physical-memory) 2))
(varconfig! splitpool:maxmem maxmem)

(define-init thread-spacing 2)
(varconfig! splitpool:spacing thread-spacing config:boolean+)

(define-init default-batchsize 0x40000)
(varconfig! splitpool:batchsize default-batchsize)

(defimport get-partition-prefix 'knodb/flexpool)
(defimport get-padlen 'knodb/flexpool)
(defimport default-partsize 'knodb/flexpool)

;;; Splitting existing pools into smaller flexpools

(define (flexpool/split original (opts #f) (partsize) (output) (rootdir) (prefix))
  (default! partsize (getopt opts 'partsize (config 'PARTSIZE default-partsize)))
  (default! output
    (abspath
     (getopt opts 'output 
	     (config 'OUTPUT 
		     (glom (strip-suffix (abspath original) ".pool")
		       ".flexpool")))))
  (default! rootdir 
    (getopt opts 'rootdir (config 'ROOTDIR (dirname output))))
  (default! prefix (get-partition-prefix original opts))
  (let* ((input (open-pool original (cons #[adjunct #t] opts)))
	 (flexbase (pool-base input))
	 (flexcap (getopt opts 'newcap (CONFIG 'NEWCAP (pool-capacity input))))
	 (flexload (getopt opts 'newload (config 'NEWLOAD (pool-load input))))
	 (metadata (poolctl input 'metadata))
	 (label (config 'LABEL (pool-label input)))
	 (source (pool-source input))
	 (padlen (get-padlen flexcap partsize)))

    (let* ((opts+ `#[base ,flexbase 
		     prefix ,prefix
		     capacity ,flexcap
		     load ,flexload
		     rootdir ,rootdir
		     metadata ,(poolctl input 'metadata)
		     partsize ,partsize
		     build ,(timestamp)])
	   (batchsize (getopt opts 'batchsize default-batchsize))
	   (nthreads (getopt opts 'nthreads (rusage 'ncpus)))
	   (flexpool (flexpool/make output (cons opts+ opts))))
      (when (getopt opts 'copy #t)
	(let* ((partitions (flexpool/partitions flexpool))
	       (fifo (fifo/make (choice->vector (flexpool/partitions flexpool))
				`#[fillfn ,fifo/exhausted!])))
	  ;; Now, copy all of the OIDs, trying to be efficient
	  (let ((count 0)
		(start (elapsed-time))
		(total-time 0.0))
	    (let ((logcopy (slambda (subpool n total time)
			     (loginfo |RightNow|
			       "Took " (secs->string time) " to copy "
			       ($num n) " (of " ($num total) ") OIDs to " subpool)
			     (set! total-time (+ total-time time))
			     (set! count (+ n count))
			     (lognotice |Overall|
			       "Copied " ($num count) " OIDs (" (show% count flexload) ") "
			       " in " (secs->string (elapsed-time start) 1) ", "
			       ($num (->exact (/~ count (elapsed-time start)))) " OIDs/second or "
			       ($num (->exact (/~ count total-time))) " OIDs/second/thread"))))
	      (let ((threads {}))
		(cond ((and nthreads (> nthreads 1))
		       (dotimes (i nthreads)
			 (set+! threads (thread/call copy-subpool input fifo batchsize logcopy))
			 (when thread-spacing (sleep thread-spacing)))
		       (thread/wait threads))
		      (else (copy-subpool input fifo batchsize logcopy))))))))
      flexpool)))

;;; Copying OIDs into a subpool

(define (copy-subpool from fifo batchsize (logcopy #f))
  (let ((to (fifo/pop fifo)))
    (while (and (exists? to) to)
      (pool/copy from to #f batchsize logcopy)
      (set! to (fifo/pop fifo)))))

