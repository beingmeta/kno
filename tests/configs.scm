;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'varconfig)

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
(applytest #f #:config:novar)
(applytest #f #:config"novar")

(config! "BAZ" 89.7)
(applytest 89.7 config 'baz)
(applytest 89.7 config "baz")
(applytest 'err config 89.7)

(config-default! 'confvar 99)
(applytest 88 config "confvar" 9)

;;; load-config

(define config-root (get-component "webfiles/root"))
(varconfig! 'useconfigroot config-root)

(load-config (mkpath config-root "sample.cfg"))
(applytest "xxx" config 'xval)
(applytest #f config 'yval)
(applytest "zzz" config 'zval)
(load-default-config (mkpath config-root "default.cfg"))
(applytest "xxx" config 'xval)
(applytest "YYY" config 'yval)

(config! 'itis9 9)

(errtest (load-config))
(errtest (load-config 'nosuchref))
(errtest (load-config 'itis9))
(errtest (load-config #"foo"))
(errtest (load-config '(foo)))
(errtest (load-config "data/nosuch.cfg"))
(errtest (load-default-config))
(errtest (load-default-config 'nosuchref))
(errtest (load-default-config 'itis9))
(errtest (load-default-config '(foo)))
(errtest (load-default-config #"foo"))
(errtest (load-default-config "data/nosuch.cfg"))


(applytest overlaps? '{|PID| |PPID|} find-configs "pid")
(applytest overlaps? '{|PID| |PPID|} find-configs #/p+id/i)

(with-sourcebase 
 #f (load-config (get-component "webfiles/root/sample.cfg")))
(with-sourcebase 
 #f (load-default-config (get-component "webfiles/root/sample.cfg")))

(define (list-contains? l val)
  (member val l))

(config! 'config (abspath (get-component "data/load.cfg")))
(applytest timestamp? config 'test.load.cfg)
(applytest has-prefix (config 'cwd) config 'test.load.cfg.path)
(applytest list-contains? (abspath (get-component "data/load.cfg")) config 'config)
;; Redundant
(config! 'config (abspath (get-component "data/load.cfg")))

(config! 'defaults (get-component "webfiles/root/default.cfg"))

(config! 'config-config (get-component "webfiles/root/default.cfg"))
(load-config 'config-config)
(load-default-config 'config-config)
(errtest (load-config 'config-notconfig))
(errtest (load-default-config 'config-notconfig))
(config! 'config-wrongtype-config '(a b))
(errtest (load-config 'config-wrongtype-config))
(errtest (load-default-config 'config-wrongtype-config))

;;; Errors

(applytest 'err config! 33 88)
(applytest 'err config-default! 33 88)
(applytest 'err config! #"foo" 88)
(applytest 'err config-default! #"foo" 88)

;;; Errors in config-def handlers

(define badconfig-var #f)
(config! 'badconfig "(3 4")
(errtest
 (config-def! 'badconfig
   (lambda (var (val))
     (cond ((bound? val)
	    (set! badconfig-var 
	      (if (string? val)
		  (string->lisp val)
		  val)))
	   (else badconfig-var)))))

(config-def! 'badconfig2
  (lambda (var (val))
    (cond ((bound? val)
	   (set! badconfig-var 
	     (if (string? val)
		 (string->lisp val)
		 val)))
	  (else badconfig-var))))
(errtest (config! 'badconfig2 "(3 4"))

;;; With promises

(config! 'foobar5 (delay (+ 2 3)))
(applytest 5 config 'foobar5)
(applytest 7 config 'foobar7 (delay (+ 5 2)))

(config-default! 'foobar9 (delay (+ 2 3)))
(applytest 5 config 'foobar9)
(config-default! 'foobar5 (delay (+ 2 3 2)))
(applytest 5 config 'foobar5)


;;; Various configs

;;; Just exercising, not checking if they work.

(define (test-config name (newv))
  (default! newv (config name))
  (let ((v (config name)))
    (config! name newv)
    (config! name v)))

(test-config 'utf8err #t)
(test-config 'utf8warn #t)
(test-config 'sessionid)
(test-config 'appid)
(test-config 'runbase)
(test-config 'randomseed)

(applytest fixnum? config 'runuser)
(applytest fixnum? config 'rungroup)
(applytest fixnum? config 'maxnproc)
(applytest fixnum? config 'maxfiles)

(test-config 'rungroup)
(test-config 'runuser)
(test-config 'maxfiles)

(errtest (config! 'runuser 0))
(errtest (config! 'rungroup 0))

(applytest flonum? config 'elapsed)
(applytest string? config 'directory)
(applytest string? config 'cwd)

(applytest pair? config 'loadpath)

(dbg #t)
(do-choices (setting '{LOCAL_MODULES INSTALLED_MODULES STDLIB_MODULES})
  (applytest string? config setting))

;;; This exercises the really long string case in config_intern
(config! "ZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXWZYXW" 3)

;;;; Optconfigs

(config! 'optconfig (abspath (get-component "data/xoptional.cfg")))
(applytest #f config 'optional.cfg)
(config! 'optconfig (abspath (get-component "data/optional.cfg")))
(applytest string? config 'optional.cfg)

;;; READ-CONFIG

(read-config (filestring (get-component "webfiles/root/sample.cfg")))

;;; Test passing to MAIN

(define (main)
  (applytest 'testsym config 'testsym)
  (applytest ":COLON" config 'teststring))

