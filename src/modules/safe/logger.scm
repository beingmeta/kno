(in-module 'logger)

(module-export! '{%loglevel logger logdebug loginfo lognotice %debug})

(define %nosubst '%loglevel)

(define %loglevel 4)

(define logger
  (macro expr
    `(logif (>= %loglevel ,(car expr)) ,(car expr) ,@(cdr expr))))

(define logdebug
  (macro expr `(logif+ (>= %loglevel 7) 7 ,@(cdr expr))))
(define loginfo
  (macro expr `(logif+ (>= %loglevel 6) 6 ,@(cdr expr))))
(define lognotice
  (macro expr `(logif+ (>= %loglevel 5) 5 ,@(cdr expr))))
(define %debug
  (macro expr `(logif+ (>= %loglevel 5) 5 ,@(cdr expr))))

