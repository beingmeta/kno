(use-module '{storage/flex storage/flexpools texttools varconfig logger})

(define poolfile "flex.flexpool")
(define pooltype 'bigpool)
(define compression #f)
(varconfig! poolfile poolfile)
(varconfig! pooltype pooltype #t)
(varconfig! compression compression #t)

(define testpool #f)

(define (main (poolfile poolfile) (reset (config 'reset #f config:boolean)))
  (%watch "MAIN" poolfile reset)
  (when (and reset (file-exists? poolfile))
    (flexpool/delete! poolfile))
  (let* ((existing (file-exists? poolfile))
	 (opts `#[type ,pooltype compression ,compression])
	 (fp (if existing 
		 (flexpool/open poolfile #f)
		 (flexpool/make poolfile 
				`#[prefix ,(strip-suffix poolfile ".flexpool")
				   base @99/0
				   capacity 1024
				   partsize 16]))))
    (set! testpool fp)
    (unless existing
      (logwarn |Initializing| testpool)
      (dotimes (i 9) 
	(frame-create fp
	  '%id i 'num i 'generation 1
	  'sq (* i i)))
      (applytest 1 flexpool/partcount fp)
      (dotimes (i 9) 
	(frame-create fp
	  '%id (glom "2." i) 'num i 'generation 2
	  'sq (* i i)))
      (applytest 2 flexpool/partcount fp)
      (commit (flexpool/partitions fp)))
    (applytest 2 flexpool/partcount fp)))




