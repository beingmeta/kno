#!/usr/bin/env fdexec

(use-module 'optimize)

(define (run-benchmark benchmark-name benchmark-thunk)
  (let ((start (timestamp)))
    (dotimes (i 5) (benchmark-thunk))
    (let ((time (difftime (timestamp) start)))
      (lineout "Benchmark " benchmark-name
	       " took " time " seconds"))))

(define (main file)
  (load file))

