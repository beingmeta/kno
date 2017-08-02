;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{fileio logger reflection varconfig})

(config! 'CHECKDTSIZE #t)

(define poolfile "test.pool")
(define pooltype 'bigpool)
(define compression #f)
(varconfig! poolfile poolfile)
(varconfig! pooltype pooltype #t)
(varconfig! compression compression #t)

(define testpool #f)

(define (get-pool (poolfile poolfile) (opts #f))
  (cond (testpool testpool)
	((file-exists? poolfile) (use-pool poolfile opts))
	(else
	 (make-pool poolfile
		    (cons `#[type ,pooltype
			     compression ,compression
			     base @b001/0 
			     capacity 100000
			     metadata #[status fresh]
			     offtype ,(config 'offtype 'B40)]
			  opts)))))

(define (make-random-frame pool (interrupt #f))
  (let* ((seed (1+ (random 1000)))
	 (frame (frame-create pool
		  '%id (stringout seed)
		  'type 'test
		  'seed seed))
	 (n-slots (random 43))
	 (rslots {}))
    (store! frame (intern (glom "SEED" seed)) frame)
    (dotimes (i n-slots)
      (let* ((r (random 200))
	     (rslot (intern (stringout "SLOT" (* r seed)))))
	(store! frame rslot (* (random 4000000) seed))
	(add! frame 'rslots rslot)))
    (store! frame 'n-slots (1+ (choice-size (getkeys frame))))
    frame))

(define (test-frame frame (interrupt #f))
  (debug%watch "Testing" frame)
  (and (exists number? (get frame 'seed))
       (exists number? (get frame 'n-slots))
       (= (get frame 'n-slots) (choice-size (getkeys frame)))
       (let ((seed (get frame 'seed))
	     (failed #f))
	 (do-choices (rslot (get frame 'rslots))
	   (unless failed
	     (unless (and (test frame rslot)
			  (singleton? (get frame rslot))
			  (number? (get frame rslot))
			  (zero? (remainder (get frame rslot) seed)))
	       (set! failed #t))))
	 (not failed))))

(define (get-rthreads)
  (config 'RTHREADS 
	  (and (CONFIG 'NTHREADS) 
	       (* 3 (CONFIG 'NTHREADS #f)))))
(define (get-wthreads) (config 'WTHREADS (CONFIG 'NTHREADS #f)))

(define (make-n-frames count pool)
  (dotimes (i count) (make-random-frame pool)))
(define (test-n-frames count pool)
  (dotimes (i count) 
    (applytest #t test-frame (random-oid pool))))
(define (test-n-frames rthreads count pool)
  (if rthreads
      (let ((n-per-thread (1+ (quotient testcount rthreads)))
	    (threads {}))
	(dotimes (i wthreads)
	  (set+! threads (threadcall test-n-frames n-per-thread pool)))
	(threadjoin threads))
      (test-n-frames count pool)))

(define (main (testcount 250) (poolfile poolfile) 
	      (reset (config 'RESET #f))
	      (rthreads (get-rthreads))
	      (wthreads (get-wthreads)))
  (set! testcount (floor testcount))
  (when (and reset (file-exists? poolfile))
    (remove-file poolfile))
  (let* ((init (not (file-exists? poolfile)))
	 (pool (get-pool poolfile `#[readonly ,(not init)])))
    (set! testpool pool)
    (if init
	(logwarn |FreshPool| pool)
	(logwarn |ExistingPool| pool))
    (when init
      (if wthreads
	  (let ((n-per-thread (1+ (quotient (* testcount 4) wthreads)))
		(threads {}))
	    (dotimes (i wthreads)
	      (set+! threads 
		(threadcall make-n-frames n-per-thread pool)))
	    (threadjoin threads))
	  (dotimes (i (* testcount 4))
	    (make-random-frame pool))))
    (when (exists? (poolctl pool 'metadata 'status))
      (if init
	  (applytest 'fresh (poolctl pool 'metadata 'status))
	  (applytest 'existing (poolctl pool 'metadata 'status)))
      (poolctl pool 'metadata 'status 'existing))
    (dotimes (i (quotient testcount 4)) 
      (applytest #t test-frame (random-oid pool)))
    (logwarn |PoolTests1| "Passed some tests on " pool)
    (commit pool)
    (dotimes (i (quotient testcount 4)) 
      (applytest #t test-frame (random-oid pool)))
    (logwarn |PoolTests2| "Passed some more tests on " pool)
    (swapout)
    (dotimes (i (quotient testcount 4)) 
      (applytest #t test-frame (random-oid pool)))
    (logwarn |PoolTests3| "Passed still some more tests on " pool)))
