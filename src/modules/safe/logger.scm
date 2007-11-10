(in-module 'logger)

(module-export! '{%loglevel logger logdebug loginfo lognotice %debug})
(module-export! '{%emergency! %alert! %critical! %error! %warning! %notice! %info! %debug!})

(define %nosubst '%loglevel)

(define %emergency! 0)
(define %alert! 1)
(define %critical! 2)
(define %error! 3)
(define %warning! 4)
(define %notice! 5)
(define %info! 6)
(define %debug! 7)

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
  (macro expr `(logif+ (>= %loglevel 7) 7 ,@(cdr expr))))

