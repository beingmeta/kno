(use-module '{logger optimize})
(load-component "common.scm")
(config! 'optalltest #t)
(config! 'traceload #t)
;(config! 'optimize:checkusage #f)
(config! 'optimize:err #t)
(config! 'mysql:lazyprocs #f)
(define %loglevel %info%)
(define trouble #f)

(define check-modules
  (macro expr
    `(onerror
	 (begin
	   (use-module ,(cadr expr))
	   (do-choices (mod ,(cadr expr))
	     (loginfo |Load All| "Optimizing module " mod)
	     (optimize-module! mod)))
	 (lambda (ex) (set! trouble ex) (reraise ex)))))

(when (config 'optimize2) (optimize-module! 'optimize))

(define started (elapsed-time))
(load-component "libscm.scm")
(load-component "stdlib.scm")
(load-component "brico.scm")
(logwarn |Done| "After " (elapsed-time started) " seconds")

(when (> (optimize/count-warnings) 0)
  (logwarn |OptimizerWarnings| 
    "There were " ($count (optimize/count-warnings) "warning"))
  (optimize/list-warnings)
  (set! trouble #t)
  (error |OptimizerWarnings| optmods.scm #f
    "There were " ($count (optimize/count-warnings) "warning")))

(when trouble (exit 1))
