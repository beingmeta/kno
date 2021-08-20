#!../dbg/knox
;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'reflection)

(define (simplify-arg arg)
  (if (and (string? arg) (position #\/ arg)
	   (file-exists? arg))
      (basename arg)
      arg))

(define (simpconfig name (val))
  (default! val (config name))
  (and (exists? val) val
       (map simplify-arg (config name))))

(define (identity x) x)

(define (main arg1 arg2 arg3 arg4 arg5 arg6 arg7 (arg8 #f) (arg9 #f))
  (applytest "a" identity arg1)
  (applytest "b b" identity arg2)
  (applytest "c" identity arg3)
  (applytest 4 identity arg4)
  (applytest 1/3 identity arg5)
  (applytest 3 identity arg6)
  (applytest 5.9 identity arg7)
  (applytest 'x identity arg8)
  (applytest 
   #("knox" 
     "exectest.scm" "a" "b b" "c" 
     "foobar=8" "quux=1/2" "4" 
     "1/3" "9/3" "5.9" ":x")
   simpconfig 'ARGV)
  (applytest
   #("knox" "exectest.scm" "a" "b b" "c" 
     "foobar=8" "quux=1/2" "4" 
     "1/3" "9/3" "5.9" ":x")
   simpconfig 'RAWARGS)

  (applytest #("a" "b b" "c" 4 1/3 3 5.9 X)
	     CONFIG 'cmdargs)
  (applytest #("a" "b b" "c" 4 1/3 3 5.9 X)
	     CONFIG 'args)
  (applytest #("a" "b b" "c" "4" "1/3" "9/3" "5.9" ":x")
	     CONFIG 'stringargs))

#|
(define (main arg1 arg2 arg3 arg4 arg5 arg6 arg7 (arg8 #f) (arg9 #f))
  (%watch (config 'argv) (config 'args) (config 'cmdargs) arg1 arg2))
|#

