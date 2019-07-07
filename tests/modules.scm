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
