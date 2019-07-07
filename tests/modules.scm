;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(config! 'loadpath (get-component "data"))

(unless (has-suffix (config 'exe) "/knoc") (config! 'logloaderrs #f))

(use-module '{stringfmts testcapi})

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
(errtest (within-module 'stringfmts (->exact (quotient~ 17 0))))

(errtest (accessing-module 'testcapi (quotient~ zval 3)))

(define zval 17)
(errtest (within-module 'stringfmts (quotient~ zval 3)))
(evaltest 5 (accessing-module 'stringfmts (quotient~ zval 3)))

(evaltest #t (overlaps? (get-exports (get-module 'fileio)) 'open-output-file))
(evaltest #t (overlaps? (get-exports (get-module 'stringfmts))
			'get%))

(modules/testcapi)

;;; Update modules

(config! 'updatemodules 15)
(config! 'updatemodules #f)

;;; Reload testing

(use-module 'reloadmod)
(errtest (reload-module 'nosuchmod))
(errtest (reload-module 33))
(errtest (reload-module '(kno tests)))
(errtest (use-module (get-component "data/nosuchmod.scm")))

(lognotice |LoadPath| (config 'loadpath))

(define-tester (test-reloading)
  (let ((base (get-load-count)))
    (applytest base get-load-count)
    (reload-module 'reloadmod)
    (applytest (1+ base) get-load-count)
    (update-modules)
    (applytest (1+ base) get-load-count)
    (set-file-modtime! (get-source 'reloadmod) (timestamp))
    (sleep 1)
    (update-module 'reloadmod)
    (errtest (update-module 'nosuchmod))
    (errtest (update-module))
    (applytest (+ 2 base) get-load-count)
    (set-file-modtime! (get-source 'reloadmod) (timestamp))
    (sleep 1)
    (update-modules)
    (applytest (+ 3 base) get-load-count)
    (update-module 'reloadmod #t)))

(test-reloading)
(sleep 2)
(test-reloading)

