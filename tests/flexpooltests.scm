(load-component "common.scm")

(use-module '{flexdb flexdb/flexpool texttools varconfig logger logctl})

(logctl! 'flexdb/flexpool %debug%)
(logctl! 'flexdb/adjuncts %debug%)

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
	 (opts `#[base @99/0 capacity 1024 partsize 16 type flexpool
		  prefix ,(strip-suffix poolfile ".flexpool")
		  adjuncts #[to3rd "to3rd"]
		  compression ,compression
		  partition-type bigpool])
	 (fp ((if existing open-pool make-pool) poolfile opts)))
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




