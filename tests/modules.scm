;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(config! 'loadpath (get-component "data"))

(unless (has-suffix (config 'exe) "/knoc") (config! 'logloaderrs #f))

(use-module '{stringfmts testcapi})

(get-module 'stringfmts)
(get-module 'testcapi)

(errtest (in-module))
(errtest (in-module '(not a module spec)))
(errtest (module-export!))
(errtest (module-export! #"packet"))
(errtest (use-module))
(errtest (use-module . rest))
(errtest (use-module 'nomod))
(errtest (use-module 'badmod))

(applytest #f dynamic-load "data/nomod")

(applytest overlaps? 'show% get-exports 'stringfmts)
(applytest {} get-exports 'zyizx)

(applytest pair? config 'module)
(applytest 'void config! 'module 'stringfmts)

(let ((mod (get-module 'stringfmts_alias)))
  (overlaps? 'get% (get-exports mod)))

(evaltest #t (applicable? (within-module 'stringfmts get%)))
(evaltest #t (applicable? (within-module 'stringfmts quotient~)))
(errtest (within-module 'stringfmts (quotient~)))
(errtest (within-module))
(errtest (within-module 'qrxtm))

(evaltest 5 (within-module 'stringfmts (quotient~ 17 3)))
(errtest (within-module 'stringfmts (quotient~ 17 "three")))
(errtest (within-module 'stringfmts (->exact (quotient~ 17 0))))

(errtest (accessing-module 'testcapi (quotient~ zval 3)))
(errtest (accessing-module))
(errtest (accessing-module 'badmod (+ 3 4)))
(errtest (accessing-module '(not a module) (+ 3 4)))

(define zval 17)
(errtest (within-module 'stringfmts (quotient~ zval 3)))
(evaltest 5 (accessing-module 'stringfmts (quotient~ zval 3)))

(evaltest #t (overlaps? (get-exports (get-module 'fileio)) 'open-output-file))
(evaltest #t (overlaps? (get-exports (get-module 'stringfmts))
			'get%))

(modules/testcapi)

;; Tests recursive loading
(use-module 'loop2mod)

(applytest eq? $num get-binding 'stringfmts '$num)
(applytest eq? $num %get-binding 'stringfmts '$num)

(applytest 'err get-binding 'stringfmts '1+)
(applytest 'err %get-binding 'stringfmts '1+)
(applytest 'err get-binding (get-module 'stringfmts) '1+)
(applytest 'err %get-binding (get-module 'stringfmts) '1+)
(applytest 'err get-binding 'stringfmts 'rem~)
(applytest applicable? %get-binding 'stringfmts 'rem~)

(applytest 'err get-binding #"packet" '$num)
(applytest 'err get-binding 'zyxxyz '$num)
(applytest 'err %get-binding #"packet" '$num)
(applytest 'err %get-binding 'zyxxyz '$num)

;;; Update modules

(config! 'updatemodules 15)
(applytest 15.0 config 'updatemodules)
(config! 'updatemodules 13.0)
(applytest 13.0 config 'updatemodules)
(config! 'updatemodules #t)
(config! 'updatemodules #f)
(errtest (config! 'updatemodules 1/2))
(errtest (config! 'updatemodules #"packet"))

;;; Try some APPMODS cases

(errtest (config! 'appmods #"notamodule"))
(errtest (config! 'appmods '(stringfmts #"notamodule")))

;;; Reload testing

(define goodmod-file  (get-component "data/goodmod.scm"))

(evaltest #t (update-module goodmod-file))

(use-module 'reloadmod)
(errtest (reload-module 'nosuchmod))
(errtest (reload-module 33))
(errtest (reload-module '(kno tests)))
(errtest (use-module (get-component "data/nosuchmod.scm")))
(errtest (use-module (get-component "data/badmod.scm")))
(evaltest #t (ignore-errors 
	      (begin (use-module goodmod-file) #t)
	      #f))
(evaltest 'void (use-module (get-component "data/splitmod.scm")))
(config! 'splitmod:err #t)
(errtest (reload-module (get-component "data/splitmod.scm")))
(config! 'splitmod:err #f)

(evaltest #f (reload-module "data/nosuchmod.scm"))

(lognotice |LoadPath| (config 'loadpath))

(config! 'load:trace #t)

;; (fileout (get-component "data/xtestmod.scm")
;;   (pprint '(in-module 'xtestmod))
;;   (pprint '(define z (* 1000 1000 1000 1000 1000))))
;; (use-module 'xtestmod)
;; (fileout (get-component "data/xtestmod.scm")
;;   (pprint '(in-module 'xtestmod))
;;   (pprint '(define z (* 1000 1000 1000 1000 1000 "three"))))
;; (errtest (reload-module 'xtestmod))
;; (fileout (get-component "data/xtestmod.scm")
;;   (pprint '(in-module 'notxtestmod))
;;   (pprint '(define z (* 1000 1000 1000 1000 1000))))
;; (errtest (reload-module 'xtestmod))

(define-tester (test-reloading)
  (let ((base (get-load-count)))
    (applytest base get-load-count)
    (reload-module 'reloadmod)
    (applytest (1+ base) get-load-count)
    (update-modules)
    (applytest (1+ base) get-load-count)
    (set-file-modtime! (get-source 'reloadmod) (timestamp+ 1))
    (update-module 'reloadmod)
    (set-file-modtime! goodmod-file (timestamp+ 4))
    (update-module goodmod-file)
    (errtest (update-module 'nosuchmod))
    (errtest (update-module))
    (applytest (+ 2 base) get-load-count)
    (set-file-modtime! (get-source 'reloadmod) (timestamp+ 2))
    (update-modules)
    (applytest (+ 3 base) get-load-count)
    (set-file-modtime! (get-source 'reloadmod) (timestamp))
    (update-module 'reloadmod #t)))

(test-reloading)
(sleep 2)
(test-reloading)

(define (test-webmods (prefix "file:"))
  (withenv #f
    (use-module "https://s3.amazonaws.com/knomods.beingmeta.com/kno/tests/testmod.scm")
    (evaltest #t (symbol-bound? 'alt-minus))
    (update-module  "https://s3.amazonaws.com/knomods.beingmeta.com/kno/tests/testmod.scm")
    (update-module  "https://s3.amazonaws.com/knomods.beingmeta.com/kno/tests/testmod.scm" #t)))

(unless (or (config 'offline) (getenv "OFFLINE")) (test-webmods))

