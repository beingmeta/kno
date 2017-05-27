;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2016-2017 beingmeta, inc.  All rights reserved.

(in-module 'bench)

(use-module '{optimize varconfig logger})

(define default-repeat 100)
(varconfig! repeat default-repeat)

(define (benchn repeat benchmark-name fn . args)
  (let ((start (timestamp)))
    (dotimes (i repeat) (apply fn args))
    (let ((time (difftime (timestamp) start)))
      (logwarn |Benchmark| benchmark-name 
	" (" repeat " cycles) took " time " seconds"))))

(define (bench benchmark-name fn . args)
  (let ((repeat (config 'repeat 5))
	(start (timestamp)))
    (dotimes (i default-repeat) (apply fn args))
    (let ((time (difftime (timestamp) start)))
      (logwarn |Benchmark| benchmark-name 
	" (" repeat " cycles) took " time " seconds"))))

(module-export! '{bench benchn})

