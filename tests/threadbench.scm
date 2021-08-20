;;; -*- Mode: Scheme; -*-

(use-module '{optimize bench bench/threads bench/miscfns
	      varconfig logger kno/mttools})

(define optimized #t)
(varconfig! optimized optimized config:bool)
(define runtime 10)
(varconfig! time runtime config:interval)

(define (fibloop) (fibi 200))

(define (main (nthreads (mt/threadcount)) (limit runtime))
  (tbench nthreads limit fibloop #f))

(when optimized
  (optimize! '{bench bench/threads bench/miscfns})
  (optimize-locals!))
