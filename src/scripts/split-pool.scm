;;; -*- Mode: Scheme -*-

(use-module '{fdweb optimize mttools varconfig 
	      stringfmts logger fifo 
	      flexdb storage/flexpools})

(define %loglevel %notice%)
(config! 'cachelevel 2)
(config! 'optlevel 4)
(config! 'logthreadinfo #t)
(config! 'logprocinfo #t)
(config! 'thread:logexit #f)

(define (getflags)
  (choice->list
   (choice (tryif (config 'dtypev2 #f) 'dtypev2)
	   (tryif (config 'B32 #f) 'B32)
	   (tryif (config 'B40 #f) 'B40)
	   (tryif (config 'B64 #f) 'B64)
	   (tryif (config 'ZLIB #f) 'ZLIB)
	   (tryif (config 'SNAPPY #f) 'SNAPPY))))

(define (main (from #f) (to #f) (capacity #f))
  (unless from (usage) (exit))
  (when (number? to)
    (if (string? capacity)
	(let ((usecap to))
	  (set! to usecap)
	  (set! capacity usecap))))
  (unless to (set! to  (glom (strip-suffix from ".pool") ".flexpool")))
  (cond ((not (bound? from)) (usage) (exit))
	((not (file-exists? from))
	 (logcrit |MissingInput| "The file " (write from) " does not exist"))
	((not to) (usage) (exit))
	((and to (file-exists? to) (has-suffix to ".pool"))
	 (config! 'appid (glom "split(" (basename to) ")"))
	 (let ((in (open-pool from #[adjunct #t readonly #t cachelevel 2]))
	       (out (open-pool to #[adjunct #t cachelevel 2])))
	   (pool/copy in out `#[batchsize ,(config 'BATCHSIZE 0x40000)])
	   (commit out)
	   (swapout out)
	   (swapout in)))
	((and to (file-exists? to) (not (config 'OVERWRITE #f)))
	 (logwarn |NotOverwriting| "Existing file " to))
	((and (not to) (file-exists? (glom from ".bak")))
	 (logcrit |BackupFile|
	   "I don't want to delete the backup file " (write (glom from ".bak"))))
	((not (or (config 'FORK #f) (config 'NPROCS #f)))
	 (config! 'appid (glom "split(" from ")"))
	 (flexpool/split from 
			 (frame-create #f
			   'make #t 'output to
			   'partsize (config 'partsize (tryif capacity capacity))
			   'prefix (or (config 'PREFIX) {})
			   'newload (or (config 'NEWLOAD) {})
			   'newcap (or (config 'NEWCAP) {})
			   'nthreads (tryif (config 'nthreads) (config 'nthreads)))
			 (or capacity #default)))
	(else
	 (config! 'appid (glom "split(" from ")"))
	 (let* ((split-opts
		 (frame-create #f
		   'make #t 'output to 'copy #f
		   'partsize (config 'partsize (tryif capacity capacity))
		   'batchsize (config 'BATCHSIZE 0x40000)
		   'prefix (or (config 'PREFIX) {})
		   'newload (or (config 'NEWLOAD) {})
		   'newcap (or (config 'NEWCAP) {})
		   'nthreads (tryif (config 'nthreads) (config 'nthreads))))
		(flexpool (flexpool/split from split-opts (or capacity #default)))
		(partitions (flexpool/partitions flexpool))
		(fifo (fifo/make (choice->vector (flexpool/partitions flexpool))
				`#[fillfn ,fifo/exhausted!]))
		(nprocs (config 'nprocs
				(max 1 (min (choice-size partitions)
					    (quotient (rusage 'ncpus) 2)))))
		(procthreads {}))
	   (lognotice |CopyStarted|
	     "Starting to copy OIDs from " (write from) " to partitions")
	   (if (and nprocs (> nprocs 1))
	       (begin (dotimes (i nprocs)
			(set+! procthreads 
			  (thread/call fork-copy from fifo split-opts))
			;; Sleep to reduce conflicts between threads/processes 
			(sleep (config 'THREADSPREAD 5)))
		 (thread/wait procthreads))
	       (fork-copy from fifo opts))
	   (lognotice |CopyDone|
	     "Finished copying OIDs from " (write from) " to partitions")
	   #f))))

(define (fork-copy from fifo opts)
  (let ((subpool (fifo/pop fifo)))
    (while (and (exists? subpool) subpool)
      (fork/cmd/wait (config 'EXE) (config 'SOURCE)
		     from (pool-source subpool)
		     (glom "BATCHSIZE=" (getopt opts 'batchsize 0x40000)))
      (set! subpool (fifo/pop fifo)))))

(define (usage)
  (message "split-pool source-pool *output* [capacity]")
  (message " OVERWRITE=yes|no")
  (message " PARTSIZE=n          ; split into files covering n OIDs  (e.g. 0x100000)")
  (message " PREFIX=data         ; partition filenames have form prefix.XXX.pool")
  (message " NEWCAP=1000000000   ; the new flexpool should have capacity of 1000000000 OIDs")
  (message " NEWLOAD=1000000     ; the new flexpool should an initial load of 1000 OIDs")
  (message "                     ; component pools are created to cover this many OIDs")
  (message "                     ; and this can also restrict how many OIDs are copied")
  (message " LABEL=kilroy        ; The flexpool is labelled 'kilroy' and subpools 'kilroy.XXX"))

(optimize! '{storage/flexpools storage/filenames storage/adjuncts})
(optimize!)

