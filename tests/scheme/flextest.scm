(use-module '{storage/flex texttools varconfig})

(define poolfile "flex.pool")
(define pooltype 'bigpool)
(define compression #f)
(varconfig! poolfile poolfile)
(varconfig! pooltype pooltype #t)
(varconfig! compression compression #t)

(define testpool #f)

(define (main (poolfile poolfile) (reset (config 'reset #f config:boolean)))
  (when (and reset (file-exists? poolfile))
    (flexpool/delete! poolfile))
  (let* ((existing (file-exists? poolfile))
	 (opts `#[type ,pooltype compression ,compression])
	 (fp (if existing 
		 (flexpool/ref poolfile #f)
		 (flexpool/start poolfile #f @99/0 1024 16))))
    (set! testpool fp)
    (unless existing
      (dotimes (i 9) 
	(frame-create fp
	  '%id i 'num i 'generation 1
	  'sq (* i i)))
      (dotimes (i 9) 
	(frame-create fp
	  '%id (glom "2." i) 'num i 'generation 2
	  'sq (* i i)))
      (applytest 2 flex/poolcount fp)
      (commit (flex/pools fp)))
    (applytest 2 flex/poolcount fp)))



