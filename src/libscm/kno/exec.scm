;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'kno/exec)

(use-module '{procprims})

(module-export! '{knox/fork/wait
		  fork/cmd/wait
		  fork/wait
		  knox/fork
		  fork/cmd
		  fork
		  knox
		  exec/cmd
		  exec})

;;; direct calls, no options

(define (fork/cmd prog . args)
  (apply proc/open prog #[fork #t lookup #t] args))
(define (fork/cmd/wait prog . args)
  (apply proc/open prog #[fork #t lookup #t wait #t] args))

(define (fork prog . args)
  (apply proc/open prog #[fork #t] args))
(define (fork/wait prog . args)
  (apply proc/open prog #[fork #t wait #t] args))

(define (knox prog . args)
  (apply proc/open prog #[fork #f knox #t] args))
(define (knox/fork prog . args)
  (apply proc/open prog #[fork #t] args))
(define (knox/fork/wait prog . args)
  (apply proc/open prog #[fork #t knox #t wait #t] args))

(define (exec/cmd prog . args)
  (apply proc/open prog #[fork #f lookup #t] args))
(define (exec prog . args)
  (apply proc/open prog #[fork #f] args))
