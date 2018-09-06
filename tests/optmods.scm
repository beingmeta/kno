(use-module '{logger optimize})
(load-component "common.scm")
(config! 'optalltest #t)
(config! 'traceload #t)
;(config! 'optimize:checkusage #f)
(config! 'mysql:lazyprocs #f)
(define %loglevel %info%)

(define check-modules
  (macro expr
    `(begin
       (use-module ,(cadr expr))
       (do-choices (mod ,(cadr expr))
	 (loginfo |Load All| "Optimizing module " mod)
	 (optimize-module! mod)))))

(optimize-module! 'optimize)

(load-component "extmods.scm")
