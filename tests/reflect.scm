;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{reflection texttools ezrecords bench/miscfns})

(define swapf
  (macro expr 
    (let ((arg1 (get-arg expr 1))
	  (arg2 (get-arg expr 2)))
      `(let ((tmp ,arg1))
	 (set! ,arg1 ,arg2)
	 (set! ,arg2 tmp)))))

(applytester 'reflection procedure-module procedure-module)
(applytester 'scheme procedure-module *)
(applytester 'texttools procedure-module textsubst)
(applytester 'bench/miscfns procedure-module fibi)
(applytester 'scheme procedure-module if)
(applytester 'ezrecords procedure-module defrecord)

(define-tester (contains-string pat)
  (lambda (filename) (search pat filename)))

(applytester "factr" procedure-name factr)
(applytester 1 procedure-arity factr)
(applytester-pred (contains-string "miscfns.scm") procedure-filename factr)
(errtest (procedure-name 3))
(errtest (procedure-name "procedure"))

(applytester #f procedure-symbol (lambda (x) (1+ x)))
(applytester-pred procedure? procedure-id (lambda (x) (1+ x)))
(applytester '|CAR| procedure-id car)
(applytester 'contains-string procedure-id contains-string)
(applytester "expr" procedure-name swapf)
(applytester swapf procedure-id swapf)

(applytester-pred (contains-string "conditionals.c") procedure-filename if)
(applytester-pred (contains-string "miscfns.scm") module-source (get-module 'bench/miscfns))
;;; This doesn't return the correct value
(applytester-pred string? module-source (get-module 'reflection))
(errtest (module-source 5))
(applytester #f module-source #[])
(applytester #f module-source (make-hashtable))

(defslambda (syncit x y) (lineout "Syncing " x " with " y))
(defambda (nelts c) (choice-size c))

(applytester-pred (contains-string "engine.scm") get-source (get-module 'engine))
(applytester-pred (contains-string "sqlite.") get-source (get-module 'sqlite))
(applytester-pred (contains-string "engine.scm") get-source (get (get-module 'engine) 'engine/run))
(applytester-pred (contains-string "sqlite.") get-source (get (get-module 'sqlite) 'sqlite/open))
(applytester-pred (contains-string "iterators.c") get-source dolist)
(applytester-pred string? get-source swapf)

(applytester #t applicable? syncit)
(applytester #t applicable? contains-string)
(applytester #f applicable? if)
(applytester #f applicable? "if")

(applytester #t synchronized? syncit)
(applytester #f synchronized? contains-string)

(applytester #f non-deterministic? syncit)
(applytester #f non-deterministic? car)
(applytester #t non-deterministic? intersection)
(applytester #t non-deterministic? nelts)

(applytester #t module? (get-module 'bench/miscfns))
(applytester #f module? 'bench/miscfns)
(applytester #f module? "bench/miscfns")
(applytester #f module? #[x 3 y 4])
(applytester #f module? [x 3 y 4])
(applytester #f module? (make-hashtable))
(applytester #t module? #[%moduleid dont-trust-me])

(applytester-pred hashtable? module-table (get-module 'bench/miscfns))
(applytester-pred ambiguous? module-bindings (get-module 'bench/miscfns))
(applytester-pred hashtable? module-environment (get-module 'bench/miscfns))

(applytester-pred ambiguous? apropos "get")

(applytester-pred ambiguous? module-exports 'bench/miscfns)
(applytester-pred ambiguous? module-exports (get-module 'bench/miscfns))
(applytester-pred ambiguous? module-exports (get-module 'reflection))
(errtest (module-exports #f))
(applytester '{x y} module-exports #[x 3 y 4])
(errtest (module-exports "bench/miscfns"))

(evaltest #t (ambiguous? (getmodules)))

(applytester 'FACTR procedure-symbol factr)
(applytester 'if procedure-symbol if)
(applytester 'defrecord procedure-symbol defrecord)

(define miscmod (get-module 'bench/miscfns))

(evaltest miscmod  (wherefrom 'fibr))
(evaltest miscmod (wherefrom 'fibr (%env)))
(evaltest miscmod (wherefrom 'fibr (%env) #t))
(evaltest #t (not (eq? miscmod (wherefrom 'fibr (%env) #f))))
(errtest (wherefrom 'fibr (%env) szddi))
(errtest (wherefrom 'fibr szddienv szddi))
(errtest (wherefrom 'fibr "szddienv" szddi))

(define-local fibi)
(evaltest (%env) (wherefrom 'fibi))

(evaltest #t (number? (def+ qzzy 3)))
(evaltest 3 qzzy)
(errtest (def+))
(errtest (def+ foo))
(errtest (def+ 3))
(errtest (def+ foo (* 3 "nine")))
(errtest (def+ "foo" (* 3 "nine")))
(errtest (let ((x 3)) (def+ y 9)))

(errtest (define-init))
(errtest (define-init z))
(errtest (define-init 3))
(errtest (define-init 3 (+ 8 'x)))

(errtest (define-local))
(errtest (define-local 3))
(errtest (define-local rrfib))

(defimport fibr 'bench/miscfns)
(evaltest (%env) (wherefrom 'fibr))
(errtest (defimport))
(errtest (defimport "rrzy"))
(errtest (defimport rrzy))
(errtest (defimport rrzy (get-module 'reflection)))
(errtest (defimport rrzy (get-module 'bench/miscfns)))
(errtest (defimport rrzy 'nosuchmodule))
