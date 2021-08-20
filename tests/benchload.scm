(use-module 'logger)
(define %loglevel %info%)
(define trouble #f)
;;(config! 'traceload #t)

(define check-modules
  (macro expr
    `(onerror
	 (use-module ,(cadr expr))
	 (lambda (ex) (set! trouble ex) (reraise ex)))))

(define start (elapsed-time))
(define elapsed #f)
(load-component "libscm.scm")
(load-component "stdlib.scm")
;;(load-component "brico.scm")
(set! elapsed (elapsed-time start))
(logwarn |LoadTime| "Loaded in " elapsed " secs")



