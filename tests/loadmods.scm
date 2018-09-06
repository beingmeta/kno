(use-module '{logger optimize})
(load-component "common.scm")
(config! 'optalltest #t)
(config! 'traceload #t)
(config! 'mysql:lazyprocs #f)
(define %loglevel %info%)

(define check-modules
  (macro expr `(begin (use-module ,(cadr expr)))))

(load-component "extmods.scm")
