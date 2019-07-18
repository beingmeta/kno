;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{reflection texttools ezrecords bench/miscfns optimize stringfmts})

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

(define (arity-test x (y 3)) (+ x y))
(define (arity-test2 x y (q 3) . z) (+ x y))

(define-tester (contains-string pat)
  (lambda (filename) (search pat filename)))

(applytester "factr" procedure-name factr)
(applytester 'err procedure-name #"packet")
(applytester 1 procedure-arity factr)
(applytester 1 procedure-min-arity factr)
(applytester 1 procedure-min-arity car)
(applytester 2 procedure-arity arity-test)
(applytester 1 procedure-min-arity arity-test)
(applytester #f procedure-arity arity-test2)
(applytester 2 procedure-min-arity arity-test2)

(errtest (procedure-min-arity "foo"))

(errtest (apropos 99))

(errtest (set-lambda-args! "foo" '(x (y 5))))
(errtest (set-lambda-body! "foo" '(x (y 5))))

(applytest '(x (y 3)) lambda-args arity-test)
(set-lambda-args! arity-test '(x (y 5)))
(applytest '(x (y 5)) lambda-args arity-test)

(errtest (procedure-arity if))
(errtest (procedure-arity defrecord))
(errtest (procedure-arity 99))
(errtest (procedure-arity "string"))

(applytester string? procedure-documentation arity-test)
(evaltest 'void (set-procedure-documentation! arity-test "testing arity"))
(applytester "testing arity" procedure-documentation arity-test)

(evaltest 'void (set-procedure-documentation! if "branch"))
(applytester "branch" procedure-documentation if)

(applytest #t procedure-tailable? arity-test)
(evaltest 'void (set-procedure-tailable! arity-test #f))
(applytest #f procedure-tailable? arity-test)
(evaltest 'void (set-procedure-tailable! arity-test #t))

(applytester pair? lambda-start arity-test2)
(errtest (procedure-tailable? if))
(errtest (procedure-tailable? "three"))

(applytest 'define car (lambda-source arity-test2))
(errtest (applytest 'define car lambda-source arity-test2))
(optimize! arity-test2)
(applytester pair? lambda-start arity-test2)

(applytest 'err lambda-args "#packet")
(applytest 'err lambda-env "#packet")
(applytest 'err lambda-body "#packet")
(applytest 'err set-lambda-args! "#packet" '(bytes))
(applytest 'err set-lambda-source! "#packet" (cons 'defslambda (cdr (lambda-source arity-test2))))

(set-lambda-source! arity-test2 (cons 'defslambda (cdr (lambda-source arity-test2))))
(applytest 'defslambda car (lambda-source arity-test2))

(define (add1 x) (1+ x))
(applytest 3 add1 2)
(set-lambda-body! add1 '((+ 2 x)))
(applytest 4 add1 2)
(optimize! add1)
(set-lambda-body! add1 '((+ 3 x)))
(applytest 5 add1 2)

(errtest (lambda-start car))
(errtest (lambda-start "string"))
(errtest (lambda-source car))
(errtest (lambda-source "string"))
(errtest (lambda-body car))
(errtest (lambda-body "string"))
(errtest (lambda-args car))
(errtest (lambda-args "string"))

(applytester (contains-string "miscfns.scm") procedure-filename factr)
(applytester (contains-string "ezrecords") procedure-filename defrecord)
(errtest (procedure-name 3))
(errtest (procedure-name "procedure"))

(define nameless
  (with-sourcebase #f
		   (list (lambda (x) (1+ x)) 
			 (macro expr `(+ 2 3)))))
(applytest #f procedure-filename (elts nameless))
(applytest 'err procedure-filename #"packet")
(applytest 'err procedure-module #"packet")
(applytest 'err reflect/add! #"packet" 'length 6)
(applytest 'err reflect/drop! #"packet" 'length 6)
(applytest 'err reflect/store! #"packet" 'length 6)

(applytester #f procedure-symbol (lambda (x) (1+ x)))
(applytester procedure? procedure-id (lambda (x) (1+ x)))
(applytester '|car| procedure-id car)
(applytester '|IF| procedure-id if)
(applytester 'contains-string procedure-id contains-string)
(applytester "expr" procedure-name swapf)
(applytester swapf procedure-id swapf)

(applytester #f procedure-cname factr)
(applytester 'err procedure-cname #"packet")
(applytester "car" procedure-cname car)
(applytester "open_output_file" procedure-cname open-output-file)
(applytester #("pair") procedure-typeinfo car)
(applytester #f procedure-typeinfo load)
(applytester #("string" #f #f) procedure-typeinfo load->env)
(applytester #(%void %void %void) procedure-defaults load->env)

(applytest has-prefix (procedure-filename car) procedure-fileinfo car)
(applytest has-prefix (procedure-filename show%) procedure-fileinfo show%)

(applytest #f reflect/get arity-test 'testprop)
(applytest 'void reflect/store! arity-test 'testprop "value")
(applytest "value" reflect/get arity-test 'testprop)
(applytest 'void reflect/add! arity-test 'testprop "more")
(applytest {"more" "value"} reflect/get arity-test 'testprop)

(applytest table? reflect/attribs arity-test)
(reflect/set-attribs! pair? #[documentation "is it a pair"])
(applytest "is it a pair" reflect/get pair? 'documentation)

(errtest (reflect/store! "foo" 'bar value))
(errtest (reflect/add! "foo" 'bar value))
(errtest (reflect/get "foo" 'bar))
(errtest (reflect/attribs "string"))

(applytester (contains-string "conditionals.c") procedure-filename if)
(applytester (contains-string "miscfns.scm") module-source (get-module 'bench/miscfns))
;;; This doesn't return the correct value
(applytester string? module-source (get-module 'reflection))
(errtest (module-source 5))
(applytester #f module-source #[])
(applytester #f module-source (make-hashtable))

(defslambda (syncit x y) (lineout "Syncing " x " with " y))
(defambda (nelts c) (choice-size c))

(applytester #t synchronized? syncit)
(applytester #f synchronized? car)
(applytester #f synchronized? contains-string)

(applytester (contains-string "engine.scm") get-source (get-module 'engine))
(applytester (contains-string "sqlite.") get-source (get-module 'sqlite))
(applytester (contains-string "engine.scm") get-source (get (get-module 'engine) 'engine/run))
(applytester (contains-string "sqlite.") get-source (get (get-module 'sqlite) 'sqlite/open))
(applytester (contains-string "iterators.c") get-source dolist)
(applytester string? get-source swapf)

(errtest (module-source))

(applytester #t applicable? syncit)
(applytester #t applicable? contains-string)
(applytester #f applicable? if)
(applytester #f applicable? "if")

(applytester #t synchronized? syncit)
(applytester #f synchronized? contains-string)

(errtest (synchronized? "s"))
(errtest (synchronized? 33))

(applytester #f non-deterministic? syncit)
(applytester #f non-deterministic? car)
(applytester #t non-deterministic? intersection)
(applytester #t non-deterministic? nelts)
(errtest (non-deterministic? "string"))
(errtest (non-deterministic? if))

(applytester #t module? (get-module 'bench/miscfns))
(applytester #f module? 'bench/miscfns)
(applytester #f module? "bench/miscfns")
(applytester #f module? #[x 3 y 4])
(applytester #f module? [x 3 y 4])
(applytester #f module? (make-hashtable))
(applytester #t module? #[%moduleid dont-trust-me])

(evaltest #t (hashtable? (wherefrom 'textmatch)))
(evaltest #t (module? (wherefrom '$size)))

(evaltest 5 (macroexpand defrecord 5))
(evaltest #t (pair? (macroexpand defrecord '(defrecord foo x y))))
(errtest (pair? (macroexpand car '(defrecord foo x y))))

(applytester ambiguous? module-bindings (get-module 'regex))
(applytester string? module-source (get-module 'regex))
(applytester hashtable? module-table (get-module 'regex))
(applytester #f module-environment (get-module 'regex))
(errtest (module-bindings "foo"))
(errtest (module-table "foo"))
(errtest (wherefrom "foo"))
(errtest (module-table))
(errtest (wherefrom))

(applytester string? module-source (get-module 'bench/miscfns))
(applytester hashtable? module-table (get-module 'bench/miscfns))
(applytester ambiguous? module-bindings (get-module 'bench/miscfns))
(applytester hashtable? module-environment (get-module 'bench/miscfns))
(errtest (module-source "foo"))

(applytest #t ambiguous? (getmodules (%env)))
(applytest #t ambiguous? (getmodules))
(evaltest {} (reject (getmodules) symbol?))
(evaltest {} (reject (getmodules (%env)) symbol?))
(errtest (getmodules "foo"))

(applytest ambiguous? all-modules)

(applytester ambiguous? apropos "get")

(applytester ambiguous? module-exports 'bench/miscfns)
(applytester ambiguous? module-exports (get-module 'bench/miscfns))
(applytester ambiguous? module-exports (get-module 'reflection))
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

;;;; Profiling

(applytest #f reflect/profiled? arity-test)
(evaltest #t (reflect/profile! arity-test))
(applytest #t reflect/profiled? arity-test)
(applytest #f reflect/profiled? car)
(errtest (reflect/profile! if))
(applytest #f reflect/profiled? if)

(define (ctest3 i) (1+ i))
(reflect/profile! ctest3)
(dotimes (i 10) (ctest3 i))
(let ((info (profile/getcalls ctest3)))
  [fcn (profile/fcn info)
   time (profile/time info)
   utime (profile/utime info)
   stime (profile/stime info)
   waits (profile/waits info)
   contests (profile/contests info)
   faults (profile/faults info)
   nsecs (profile/nsecs info)
   ncalls (profile/ncalls info)
   nitems (profile/nitems info)])

(errtest (profile/fcn #"packet"))
(errtest (profile/time #"packet"))
(errtest (profile/utime #"packet"))
(errtest (profile/stime #"packet"))
(errtest (profile/waits #"packet"))
(errtest (profile/contests #"packet"))
(errtest (profile/faults #"packet"))
(errtest (profile/nsecs #"packet"))
(errtest (profile/ncalls #"packet"))
(errtest (profile/nitems #"packet"))

(errtest (with-sourcebase))
(errtest (with-sourcebase "foo" . cdr))
(errtest (with-sourcebase #"foo"))

(profile/reset! ctest3)
(errtest (profile/reset! if))

(errtest (profile/getcalls if))

(let ((info (profile/getcalls ctest3)))
  [fcn (profile/fcn info)
   time (profile/time info)
   utime (profile/utime info)
   stime (profile/stime info)
   waits (profile/waits info)
   contests (profile/contests info)
   faults (profile/faults info)
   nsecs (profile/nsecs info)
   ncalls (profile/ncalls info)
   nitems (profile/nitems info)])

;; This doesn't work yet. There's an issue with not being able to free
;; the profile which needs to be sorted.

;; (evaltest #f (reflect/profile! ctest3 #f))
;; (applytest #f reflect/profiled? ctest3)

(applytest #t ambiguous? (getmodules))

(evaltest "reflect.scm" (with-sourcebase #f (get-component "reflect.scm")))
(evaltest "/tmp/reflect.scm" (with-sourcebase "/tmp" (get-component "reflect.scm")))
(errtest (with-sourcebase '(bad sourcebase) (get-component "reflect.scm")))

