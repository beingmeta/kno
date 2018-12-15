;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2016-2018 beingmeta, inc.  All rights reserved.

(in-module 'bench)

(use-module '{optimize varconfig logger})

(define default-repeat 100)
(varconfig! repeat default-repeat)

(define (benchn repeat benchmark-name fn . args)
  (when (applicable? benchmark-name)
    (set! args (cons fn args))
    (set! fn benchmark-name)
    (set! benchmark-name 
      (or (procedure-name benchmark-name) benchmark-name)))
  (let ((start (timestamp)))
    (dotimes (i repeat) (apply fn args))
    (let ((time (difftime (timestamp) start)))
      (logwarn |Benchmark| benchmark-name 
	" (" repeat " cycles) took " time " seconds"))))

(define (bench benchmark-name . args)
  (if (number? benchmark-name)
      (apply benchn benchmark-name args)
      (apply benchn default-repeat benchmark-name args)))

(module-export! '{bench benchn})

