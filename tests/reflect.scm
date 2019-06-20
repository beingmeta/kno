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

(applytest 'reflection procedure-module procedure-module)
(applytest 'scheme procedure-module *)
(applytest 'texttools procedure-module textsubst)
(applytest 'bench/miscfns procedure-module fibi)
(applytest 'scheme procedure-module if)
(applytest 'ezrecords procedure-module defrecord)

(define (contains-string pat)
  (lambda (filename) (search pat filename)))

(applytest "factr" procedure-name factr)
(applytest 1 procedure-arity factr)
(applytest-pred (contains-string "miscfns.scm") procedure-filename factr)
(errtest (procedure-name 3))
(errtest (procedure-name "procedure"))

(applytest #f procedure-symbol (lambda (x) (1+ x)))
(applytest-pred procedure? procedure-id (lambda (x) (1+ x)))
(applytest '|CAR| procedure-id car)
(applytest 'contains-string procedure-id contains-string)
(applytest "expr" procedure-name swapf)
(applytest swapf procedure-id swapf)

(applytest-pred (contains-string "conditionals.c") procedure-filename if)
(applytest-pred (contains-string "miscfns.scm") module-source (get-module 'bench/miscfns))
;;; This doesn't return the correct value
(applytest-pred string? module-source (get-module 'reflection))
(errtest (module-source 5))
(applytest #f module-source #[])
(applytest #f module-source (make-hashtable))

(defslambda (syncit x y) (lineout "Syncing " x " with " y))
(defambda (nelts c) (choice-size c))

(applytest-pred (contains-string "engine.scm") get-source (get-module 'engine))
(applytest-pred (contains-string "sqlite.") get-source (get-module 'sqlite))
(applytest-pred (contains-string "engine.scm") get-source (get (get-module 'engine) 'engine/run))
(applytest-pred (contains-string "sqlite.") get-source (get (get-module 'sqlite) 'sqlite/open))
(applytest-pred (contains-string "iterators.c") get-source dolist)
(applytest-pred string? get-source swapf)

(applytest #t synchronized? syncit)
(applytest #f synchronized? contains-string)

(applytest #f non-deterministic? syncit)
(applytest #f non-deterministic? car)
(applytest #t non-deterministic? intersection)
(applytest #t non-deterministic? nelts)

(applytest #t module? (get-module 'bench/miscfns))
(applytest #f module? 'bench/miscfns)
(applytest #f module? "bench/miscfns")
(applytest #f module? #[x 3 y 4])
(applytest #f module? [x 3 y 4])
(applytest #f module? (make-hashtable))
(applytest #t module? #[%moduleid dont-trust-me])

(applytest-pred hashtable? module-table (get-module 'bench/miscfns))
(applytest-pred ambiguous? module-bindings (get-module 'bench/miscfns))
(applytest-pred hashtable? module-environment (get-module 'bench/miscfns))

(applytest-pred ambiguous? apropos "get")

(applytest-pred ambiguous? module-exports 'bench/miscfns)
(applytest-pred ambiguous? module-exports (get-module 'bench/miscfns))
(applytest-pred ambiguous? module-exports (get-module 'reflection))
(errtest (module-exports #f))
(applytest '{x y} module-exports #[x 3 y 4])
(errtest (module-exports "bench/miscfns"))

(evaltest #t (ambiguous? (getmodules)))

(applytest 'FACTR procedure-symbol factr)
(applytest 'if procedure-symbol if)
(applytest 'defrecord procedure-symbol defrecord)

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
(errtest (defimport rrzy))
(errtest (defimport rrzy (get-module 'reflection)))
(errtest (defimport rrzy (get-module 'bench/miscfns)))
