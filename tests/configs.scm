;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(define-init identity (lambda (x) x))

(applytest #f config 'z)
(applytest 3 config 'z 3)
(config! 'z 9)
(applytest 9 config 'z)
(applytest 9 config 'z 3)

(config! 'z:two-words 9)
(applytest 9 config 'z:two-words)
(applytest 9 config 'z:two_words)

(applytest fixnum? config 'pid)
(applytest fixnum? config 'ppid)
(applytest string? config 'hostname)

(define test-val 17)
(define test-val-mods 0)
(define test-val-readonly #f)
(define-tester (config-test-val var (val))
  (cond ((unbound? val) test-val)
	(test-val-readonly 
	 (error |ReadOnlyConfig| config-test-val))
	(else (set! test-val val)
	      (set! test-val-mods (1+ test-val-mods)))))
(config-def! 'TEST_VAL config-test-val
	     "Gets or sets the value of test-val")
(applytest 17 config 'TEST-VAL)
(config! 'TEST_VAL 42)
(applytest 42 config 'TEST-VAL)
(config! 'TEST-VAL 99)
(applytest 99 config 'TEST-VAL)
(applytest 2 identity test-val-mods)

(set! test-val-readonly #t)
(errtest (config! 'TEST_VAL 42))

;;; Check for config setting

(define configured-value #f)
(varconfig! confvar configured-value #t)
(evaltest 88 configured-value)

(evaltest "quux" (config 'foo))
(evaltest "baz" (config 'xconf))

(applytest "quux" config "foo")
(applytest "quux" config "FOO")
(applytest 'err config 88)
(applytest "quux" config 'foo 88)
(applytest 'quux config 'foo 88 #t)

(config! 'badexpr "(+ 2")
(applytest 9 config 'badexpr 9 #t)

(define (bad-valfn x) (error 'just-because))

(applytest 9 config 'badexpr 9 bad-valfn)

(applytest 89 config 'confvar 0 1+)

(applytest 89 1+ #:config:confvar)

(config! "BAZ" 89.7)
(applytest 89.7 config 'baz)
(applytest 89.7 config "baz")
(applytest 'err config 89.7)

(config-default! 'confvar 99)
(applytest 88 config "confvar" 9)

(applytest overlaps? '{|PID| |PPID|} find-configs "pid")
