(use-module '{storage/flex texttools varconfig logger})

(define poolfile "flex.flexpool")
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
		 (flexpool/open poolfile #f)
		 (flexpool/make poolfile 
				#[prefix "flex" base @99/0 capacity 1024 step 16]))))
    (set! testpool fp)
    (unless existing
      (logwarn |Initializing| testpool)
      (dotimes (i 9) 
	(frame-create fp
	  '%id i 'num i 'generation 1
	  'sq (* i i)))
      (applytest 1 flex/poolcount fp)
      (dotimes (i 9) 
	(frame-create fp
	  '%id (glom "2." i) 'num i 'generation 2
	  'sq (* i i)))
      (applytest 2 flex/poolcount fp)
      (commit (flex/pools fp)))
    (applytest 2 flex/poolcount fp)))




