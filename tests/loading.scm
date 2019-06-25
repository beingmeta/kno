;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(config! 'loadpath (get-component "data"))

(use-module '{stringfmts testcapi})

(errtest (load-component "data/parsefail.scm"))
(errtest (load-component "data/nosuchfile.scm"))

(errtest (load (get-component "data/evalfail.scm")))
(errtest (load (get-component "data/nosuchfile.scm")))

(use-module 'reloadmod)

(lognotice |LoadPath| (config 'loadpath))

(define (test-reloading)
  (let ((base (get-load-count)))
    (applytest base get-load-count)
    (reload-module 'reloadmod)
    (applytest (1+ base) get-load-count)
    (update-modules)
    (applytest (1+ base) get-load-count)
    (set-file-modtime! (get-source 'reloadmod) (timestamp))
    (sleep 1)
    (update-module 'reloadmod)
    (applytest (+ 2 base) get-load-count)
    (set-file-modtime! (get-source 'reloadmod) (timestamp))
    (sleep 1)
    (update-modules)
    (applytest (+ 3 base) get-load-count)))

(test-reloading)
(sleep 2)
(test-reloading)

(load-component "data/loadok.scm")

(applytest 1 get-load-ok-count)

(load (get-component  "data/loadok.scm"))

(applytest 2 get-load-ok-count)

(load->env (get-component "data/loadok.scm") (%env))

(applytest 3 get-load-ok-count)

(begin (load-latest (get-component "data/loadok.scm"))
  (applytest 4 get-load-ok-count))
(begin (load-latest (get-component "data/loadok.scm"))
  (applytest 4 get-load-ok-count))

(begin (set-file-modtime! (get-component "data/loadok.scm") (timestamp))
  (load-latest (get-component "data/loadok.scm"))
  (applytest 5 get-load-ok-count))

(evaltest 1
	  (withenv #f 
	    (load->env (get-component "data/loadok.scm") (%env))
	    load-ok-count))

(evaltest 1
	  (withenv #f 
	    (load->env (get-component "data/loadok.scm") (%env))
	    (get-load-ok-count)))

;;; Module tests

(get-module 'stringfmts)
(get-module 'testcapi)

(errtest (use-module 'badmod))

(let ((mod (get-module 'stringfmts_alias)))
  (overlaps? 'get% (get-exports mod)))

(evaltest #t (applicable? (within-module 'stringfmts get%)))
(evaltest #t (applicable? (within-module 'stringfmts quotient~)))
(errtest (within-module 'stringfmts (quotient~)))

(evaltest 5 (within-module 'stringfmts (quotient~ 17 3)))
(errtest (within-module 'stringfmts (quotient~ 17 "three")))
;;(errtest (within-module 'stringfmts (quotient~ 17 0)))

(errtest (accessing-module 'testcapi (quotient~ zval 3)))

(define zval 17)
(errtest (within-module 'stringfmts (quotient~ zval 3)))
(evaltest 5 (accessing-module 'stringfmts (quotient~ zval 3)))

(evaltest #t (overlaps? (get-exports (get-module 'fileio)) 'open-output-file))
(evaltest #t (overlaps? (get-exports (get-module 'stringfmts))
			'get%))

(modules/testcapi)
