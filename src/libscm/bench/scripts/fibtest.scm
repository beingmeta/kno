#!/usr/bin/env fdexec
;;; -*- Mode: Scheme -*-

(use-module '{bench/miscfns bench/threads optimize})
(optimize! '{bench/miscfns bench/threads})

(config! 'thread:logexit #f)

(define (main (nthreads 1.0) (interval 15) (fibrange 50))
  (tbench nthreads interval (lambda (data) (fibi (random fibrange))) #f))

