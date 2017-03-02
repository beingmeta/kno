;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{fileio logger reflection varconfig})

(config! 'CHECKDTSIZE #t)

(define poolfile (get-component "test.pool"))
(define pooltype 'bigpool)
(varconfig! poolfile poolfile)
(varconfig! pooltype pooltype)

(define testpool #f)

(define (open-pool (fresh #t))
  (cond (testpool testpool)
	((and fresh (file-exists? poolfile))
	 (remove-file poolfile)
	 (open-pool poolfile))
	((file-exists? poolfile)
	 (use-pool poolfile))
	(else
	 (set! testpool
	       (make-pool poolfile
			  `#[type ,pooltype
			     base @b001/0 
			     capacity 100000
			     offtype ,(config 'offtype 'B40)]))
	 testpool)))

(define (make-random-frame pool (interrupt #f))
  (let* ((seed (random 1000))
	 (frame (frame-create pool
		  '%id (stringout seed)
		  'type 'test
		  'seed seed))
	 (n-slots (random 43))
	 (rslots {}))
    (store! frame (intern (glom "SEED" seed)) frame)
    (store! frame 'n-slots (+ 4 n-slots))
    (dotimes (i (random 50))
      (let* ((r (random 200))
	     (rslot (intern (stringout "SLOT" (* r seed)))))
	(store! frame rslot (* (random 4000000) seed))
	(add! frame 'rslots rslot)))
    frame))

(define (test-random-frame frame (interrupt #f))
  (debug%watch "Testing" frame)
  (and (exists number? (get frame 'seed))
       (exists number? (get frame 'n-slots))
       (= (get frame 'n-slots) (choice-size (getkeys frame)))
       (= (choice-size (get frame 'rslots)) 
	  (+ (get frame 'n-slots) 4))
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

(define (main)
  (let ((pool (open-pool #t)))
    (dotimes (i 1000) (make-random-frame pool))
    (commit pool)))
