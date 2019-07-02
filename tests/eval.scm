;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{ezrecords reflection stringfmts})

(define textmatch (get (get-module 'texttools) 'textmatch))

(define-tester (test-lexrefs (v 'foo))
  (let ((lexref (%lexref 0 1))
	(value v)
	(bad-lexref-1 (%lexref 0 5))
	(bad-lexref-2 (%lexref 8 0)))
    (lisp->string lexref)
    (dtype->packet lexref)
    (applytester '(0 . 1) %lexrefval lexref)
    (applytester '(8 . 0) %lexrefval bad-lexref-2)
    (applytester #t %lexref? lexref)
    (applytester #f %lexref? #f)
    (applytester #f %lexref? "three")
    (applytester #f %lexref? 3)
    (errtest (eval bad-lexref-1))
    (errtest (eval bad-lexref-2))
    (errtest (%lexref -9 3))
    (errtest (%lexref 2 777))
    (evaltest value (eval lexref))))

(test-lexrefs)
(test-lexrefs "foo")
(test-lexrefs 89)

(define-tester (test-coderefs)
  (let ((coderef (%coderef 16)))
    (applytester #t coderef? coderef)
    (applytester #f coderef? '(code))
    (applytester #f coderef? 0xc0de)
    (applytest string? lisp->string coderef)
    (applytest packet? dtype->packet coderef)
    (applytester #t coderef? coderef)
    (applytester 16 %coderefval coderef)))

(test-coderefs)

(define-tester (test-opcodes)
  (let ((op1 (name->opcode "op_until"))
	(op2 (name->opcode 'op_assign))
	(op3 (name->opcode 'op_center))
	(op4 (make-opcode 200))
	(bad-opcode (make-opcode 1666)))
    (errtest (name->opcode 0x334))
    (errtest (make-opcode 3192))
    (applytest string? (lisp->string op4))
    (applytest string? (lisp->string bad-opcode))
    (applytester #t opcode? (name->opcode "op_until"))
    (applytester #t opcode? bad-opcode)
    (applytester #f opcode? "op_until")
    (applytester #f opcode? 'op_until)
    (applytester (name->opcode "op_until") name->opcode 'op_until)
    (applytester #t string? (lisp->string (name->opcode "op_until")))
    (applytester "OP_UNTIL" opcode-name op1)
    (applytester #t opcode? op1)
    (applytester #t opcode? op2)))

(test-opcodes)

(define-tester (test-bindings (p 3) (q) (z #default) (g #f) (r))
  (evaltest #f (void? p))
  (evaltest #t (void? q))
  (evaltest #t (void? (void)))
  (evaltest #f (default? p))
  (evaltest #t (default? z))
  (errtest (setfalse! "g"))
  (errtest (setfalse! g))
  (errtest (setfalse! g (* 8 'p)))
  (errtest (setfalse! gzt 9))
  (evaltest 'void (setfalse! g 17))
  (evaltest 'void (setfalse! p 17))
  (errtest (default! q (* p 'p)))
  (errtest (default!))
  (errtest (default! "r"))
  (errtest (default! r))
  (errtest (default! r (* p 'p)))
  (default! q (* p p))
  (evaltest 17 g)
  (evaltest #f (symbol-bound-in? 'x #[a 3 b 4]))
  (evaltest #t (symbol-bound-in? 'a #[a 3 b 4]))
  (evaltest #t (symbol-bound-in? 'p (%env)))
  (evaltest #f (symbol-bound-in? 'z (%env)))
  (evaltest #f (symbol-bound-in? 'r (%env)))
  (evaltest #f (symbol-bound-in? 'xyzddr (%env)))
  (errtest (symbol-bound-in? "x" #[a 3 b 4]))
  (errtest (symbol-bound-in? 'z #("x" y "z")))
  (errtest (default))
  (errtest (default r))
  (evaltest 9 (default krrr 9))
  (evaltest 5 (default r (+ 2 3)))
  (errtest (define zayyx 5))
  (errtest (define-init zayyx 5))
  (errtest (define (zaayx) 3))
  (errtest (defambda (zaayx) 3))
  (errtest (defslambda (zaayx) 3))
  (let ((x (+ p p))
	(y (+ q q))
	(vals {}))
    (set+! vals x)
    (set+! vals y)
    (set+! vals p)
    (set+! vals q)
    (errtest (set+! altvals q))
    (errtest (set+! vals (* p 'p)))
    vals))

(test-bindings)

(errtest (let ((foo #f)) (set+! notbound 3)))

;; Read-only environment
(errtest (set! $num 3))

;;; Using define-tester on this breaks things because of the internal
;;; macros which get expanded like functions
(define (test-macros)
  (let ((swapf (macro expr 
		 (let ((arg1 (get-arg expr 1))
		       (arg2 (get-arg expr 2)))
		   `(let ((tmp ,arg1))
		      (set! ,arg1 ,arg2)
		      (set! ,arg2 tmp)))))
	(bad-swapf (macro expr 
		     (let ((arg1 (get-arg expr 1))
			   (arg2 (get-arg expr 2)))
		       `(let ((xtmp ,argI))
			  (set! ,arg1 ,arg2)
			  (set! ,arg2 tmp)))))
	(x 3)
	(y 4))
    (applytest string? lisp->string swapf)
    ;; From ezrecords, coverage for macros defined in module
    (applytest string? lisp->string defrecord)
    (swapf x y)
    (applytest -1 - y x)
    (errtest (bad-swapf x y))))

(test-macros)

(define (test-fcnids)
  (let ((lambda-id (fcnid/register test-macros))
	(prim-id (fcnid/register car))
	(plus-id (fcnid/register +))
	(n2s-id (fcnid/register number->string))
	(evalfn-id (fcnid/register if)))
    (applytest string? lisp->string lambda-id)
    (applytest string? lisp->string prim-id)
    (applytest string? lisp->string plus-id)
    (applytest string? lisp->string n2s-id)
    (applytest string? lisp->string evalfn-id)
    (applytest string? lisp->string evalfn-id)
    (applytest fcnid? fcnid/register prim-id)))

(test-fcnids)

;;; Typeof testing

(applytest "string" typeof "string")
(applytest "symbol" typeof 'symbol)
(applytest "packet" typeof #"data")
(applytest "secret" typeof #*"data")
(applytest "pair" typeof '(a . b))
(applytest "pair" typeof '(a b))
(applytest "vector" typeof #(a b))
(applytest "bigint" typeof (* 1000000000 4 1000000000 4))
(applytest "bigint" typeof (* -1000000000 4 1000000000 4))
(applytest "fixnum" typeof 1000000)
(applytest "fixnum" typeof -1000000)
(applytest "rational" typeof 1/3)
(applytest "flonum" typeof 0.3333)
(applytest "constant" typeof #f)
(applytest "constant" typeof #t)
(evaltest "choice" (typeof #{"one" "two" 3}))
(applytest {"fixnum" "string"} typeof #{"one" "two" 3})
(applytest "builtin function" typeof car)
(applytest "lambda procedure" typeof test-macros)

;;; Error in default args

(define (add32 x (y (+ 3 'a))) (+ x y))
(applytest 10 add32 7 3)
(errtest (add32 7))

;;; Reflection like tests

(applytester #t string? (lisp->string if))
(applytester #t packet? (dtype->packet if))

(evaltest #f (bad? 3))
(evaltest #f (bad? if))

(evaltest textmatch (eval `(%modref ,(get-module 'texttools) textmatch)))
(errtest (%modref))
(errtest (eval `(%modref)))
(errtest (eval `(%modref . bad)))
(errtest (eval `(%modref 'bad)))
(errtest (eval `(%modref ,(get-module 'texttools) 'notbound)))

(evaltest 3 (quote 3))
(evaltest 3 (eval (list 'quote 3)))
(evaltest 3 (eval (list quote 3)))

(errtest {(+ 2 3) (+ 2 'a) (+ 3 9)})
(errtest (eval (list quote)))
(errtest ({1+ if} 3))

(define (broken x y)
  (fizzbin x))

(evaltest #t (onerror (+ 3 "three") (lambda (ex) (exception? ex))))
(applytest #f exception? 3)
(applytest #f exception? "three")

(applytester "broken" procedure-name broken)
(applytester "IF" procedure-name if)
(errtest (procedure-name 'broken))
(errtest (procedure-name "broken"))

(errtest (broken "foo" "bar"))
(errtest (broken "foo"))
(errtest (broken "foo" "bar" "baz"))

(evaltest '|the variable is unbound|
	  (onerror (broken "foo" "bar")
	      (lambda (ex) (exception-condition ex))))
(evaltest '|the variable is unbound|
	  (onerror (+ (broken "foo" "bar") 9)
	      (lambda (ex) (exception-condition ex))))
(evaltest '|too many arguments|
	  (onerror (+ (broken "foo" "bar" "baz") 9)
	      (lambda (ex) (exception-condition ex))))
(errtest (apply broken "foo" "bar" '()))


(define tbl [x 3 y 4 z "foo"])
(errtest ((get tbl 'z) 4 5))

(applytester 3 get [x "three" y 3 z 'three] 'y)
(applytester "three" get [x "three" y 3 z 'three] 'x)
(applytester 'three get [x "three" y 3 z 'three] 'z)
(errtest [x "three" y 3 z three])

(evaltest #f (unbound? unbound?))
(evaltest #t (unbound? xyzayzdrs))

(errtest (unbound?))
(errtest (default? ayxxa))

(define x23f9b #f)

(errtest (letrec))
(errtest (letrec 3))
(errtest (letrec (()) 3))
(errtest (letrec ((z (+ 3 'p))) 3))
(errtest (letrec ((z)) 3))
(errtest (letrec ((z (if #f 3))) 3))
(errtest (reverse '(a b c . d)))

(evaltest 7 (letrec ((x 3) (y 4) (f (lambda (x y) (+ x y))) (z (f x y))) z))

(evaltest #t (void? (set! x23f9b #t)))
(evaltest #f (void? (not x23f9b)))
(evaltest #t (void? (void)))
(evaltest #t (void? (void (+ 2 3))))
(evaltest #t (void? (void (+ 2 3) (cons "foo" "bar"))))
(errtest (void (+ 2 "a") (cons "foo" "bar")))

(evaltest #t (default? #default))
(evaltest #f (default? 99))

(define s "string")
(evaltest #f (constant? "string"))
(evaltest #f (constant? s))
(evaltest #t (constant? #f))
(evaltest #t (constant? #default))

(evaltest #f (immediate? "string"))
(evaltest #f (immediate? @1/89))
(evaltest #f (immediate? 333))
(evaltest #t (immediate? #f))
(evaltest #t (immediate? 'foo))
(evaltest #t (immediate? #eof))

(applytester #t applicable? car)
(applytester #t applicable? broken)
(applytester #f applicable? 3)
(applytester #f applicable? "fcn")
(applytester #f applicable? 3.5)
(applytester #f applicable? '(lambda (x) (1+ X)))
(applytester #f applicable? #(lambda (x) (1+ X)))

(evaltest #t (defined? broken))
(evaltest #f (defined? xyzayzdrs))
(errtest (defined? 9))
(errtest (defined? "nein"))

(applytester #t environment? (%env))
(applytester #f environment? #(x y))
(applytester #f environment? #[x 3 y 4])
(applytester #t symbol-bound-in? 'x23f9b (%env))
(applytester #f symbol-bound-in? 'xyzayzdrs (%env))

(evaltest #t (symbol-bound? 'x23f9b))
(evaltest #t (symbol-bound? 'x23f9b #f))
(evaltest #f (symbol-bound? 'xyzayzdrs))
(evaltest #t (let ((xyzayzdrs 9)) (symbol-bound? 'xyzayzdrs)))
(evaltest #t (let ((xyzayzdrs 9)) (symbol-bound? 'x23f9b)))
(evaltest #f (let ((xyzayzdrs 9)) (symbol-bound? 'x23f9bxyz)))
(errtest (symbol-bound? "x23f9b"))
(errtest (symbol-bound? 'car '(ab)))
(errtest (symbol-bound? 'car '(ab)))

(applytester "one" get-arg '("one" "two" "three") 0)
(applytester "two" get-arg '("one" "two" "three") 1)
(evaltest #t (void? (get-arg '("one" "two" "three") 5)))
(errtest (get-arg '("one" "two" "three") "one"))
(errtest (get-arg #("one" "two" "three") 0))

(errtest (apply {+ - 5} (list 3 4)))
(errtest (apply {+ - car} (list 3 4)))
(errtest (apply {+ - if} (list 3 4)))

(define some-vec #("a" "b" "c"))
(errtest (elt some-vec))
(errtest (elt some-vec 1 'z))

(errtest (symbol-bound?))
(errtest (symbol-bound? xyzayzdrs))

(applytester #t string? (documentation test-opcodes))

(errtest (%choiceref #{a "b" c} 8))
(errtest (%choiceref "a" 1))
(evaltest 5 (%choiceref (qc {5 (symbol->string '{a b c})}) 0))

(applytester 0 compare 5 5)
(applytest positive? compare 5 4)
(applytest negative? compare 4 5)

(applytester 0 compare 'five 'five)
(applytester 0 compare "five" "five")
(applytest positive? compare "six" "five")
(applytest negative? compare "five" "six")

(applytester 0 compare '(5 3) '(5 3))
(applytest negative? compare '(5 3) '(6 3))
(applytest positive? compare '(5 4) '(5 3))

(applytester 0 compare #(5 3) #(5 3))
(applytest negative? compare #(5 3) #(6 3))
(applytest positive? compare #(5 4) #(5 3))


(applytester 0 compare/quick 5 5)
(applytest positive? compare/quick 5 4)
(applytest negative? compare/quick 4 5)

(applytester 0 compare/quick 'five 'five)
(applytester 0 compare/quick "five" "five")

(applytester 0 compare/quick '(5 3) '(5 3))

(applytester 0 compare/quick #(5 3) #(5 3))

(applytester "foobar" dontopt "foobar")

(applytester #t eqv? 3 3)
(applytester #t eqv? 3.0 3.0)
(applytester #f eqv? 3.0 5.0)

(applytester #t flonum? 3.5)
(applytester #f flonum? 3)
(applytester #f flonum? "three")

(applytester #t zero? 0.0)
(applytester #f zero? 0.1)
(applytester #f zero? -0.1)
(applytester #f zero? 1/3)
(applytester #f zero? (* 1024 1024 1024 1024 1024 1024 1024))
(applytester #f zero? "one")
(applytester #f zero? '(0))

(applytester #t contains? "foo" "foo")
(applytester #f contains? "bar" "foo")
(applytester #f contains? {} "foo")
(applytester #f contains? {} "foo")
(applytester #t contains? "foo" {"foo" "bar" "baz"})
(applytester #f contains?  {"foo" "bar" "baz"} "foo")
(applytester #f contains?  {"foo" "bar" "baz"} {"foo" "bar"})

(applytester #t true? #t)
(applytester #f true? #f)
(applytester #t true? 3)
(applytester #t true? "foo")
(applytester #t true? '("foo" "bar"))
(applytester {} true? (choice))

(applytester #f false? #t)
(applytester #t false? #f)
(applytester #f false? 3)
(applytester #f false? "foo")
(applytester #f false? '("foo" "bar"))
(applytester {} true? (choice))

(errtest (intern 3))
(errtest (intern 'three))
(errtest (intern #"foo"))

(errtest (string->lisp 'symbol))
(errtest (string->lisp #"packet"))
(errtest (string->lisp '(list)))

(applytester 'foo parse-arg 'foo)
(applytester 3 parse-arg 3)
(applytester 3 parse-arg "3")
(applytester #(a b) parse-arg "#(a b)")
(applytester '(a b) parse-arg "(a b)")
(applytester @1/889 parse-arg "@1/889")
(applytester #(a b) parse-arg ":#(a b)")
(applytester '(a b) parse-arg ":(a b)")
(applytester @1/889 parse-arg ":@1/889")
(applytester #f parse-arg "#f")
(applytester #t parse-arg "#t")

(applytester ":#(a b)" unparse-arg #(a b))
(applytester ":(a b)" unparse-arg '(a b))
(applytester "@1/889" unparse-arg @1/889)
(applytester ":foo" unparse-arg 'foo)
(applytester "33" unparse-arg 33)

(applytester "IF" procedure-name if)
(applytester "CAR" procedure-name car)
(applytester "broken" procedure-name broken)

(applytester #t ->lisp "#t")
(applytester 33.3333 ->lisp "33.3333")
(applytester "two words" ->lisp "two words")
(applytester "separated\tby\twords" ->lisp "separated\tby\twords")
(applytester "separated\tby\twords " ->lisp "separated\tby\twords ")
(applytester @1/8 ->lisp "@1/8")
(applytester 3 ->lisp 3)
(applytester @1/8 ->lisp @1/8)
(applytester #f ->lisp #f)
(applytester #t ->lisp #t)
(applytester '(a b c) ->lisp '(a b c))
(applytester #(a b c) ->lisp #(a b c))

(applytester #t pair? (getsourceinfo))

(applytester "#!5" hashref #f)
(applytester "#!86" hashref 33)
(applytest string? hashref "foo")
(applytest string? hashref '(x y))

(applytest fixnum? hashptr 33)
(applytest fixnum? hashptr 'thirtythree)
(applytest integer? hashptr "thirtythree")
(applytest integer? hashptr '(a b))

(applytest integer? hash-lisp '(a b))
(applytest integer? hash-lisp (timestamp))
(applytest integer? hash-lisp (getuuid))
(applytest integer? hash-lisp #(a b c))
(applytest integer? hash-lisp (vector 'a "a" (timestamp) (getuuid)))

;;;; Structure eval tests

(evaltest #(3 4 5) #.(3 4 (+ 4 1)))
(evaltest #[foo 3 bar 5] #.[foo (+ 2 1) bar (+ (* 2 2) 1)])
(errtest #.(3 4 (+ 4 "one")))
(errtest #.[foo (+ 2 "one") bar (+ (* 2 2) 1)])

;;; Other cases

(define (return-void) (void))
(applytester 'void return-void)

(applytest packet? dtype->packet return-void)

;;; Largest

(evaltest @1/44 (largest {@1/33 @1/44}))
(evaltest 200 (largest {30 200 120}))
(evaltest @1/33 (smallest {@1/33 @1/44}))
(evaltest 30 (smallest {30 200 120}))

;;; Syntax errors

(define exprs '((+ 2 3) (* 3 9)))

(errtest (lambda))
(errtest (ambda))
(errtest (slambda))
(errtest (sambda))
(errtest (define))
(errtest (define foo))
(errtest (define ("foo")))
(errtest (define "foo"))
(errtest (defslambda))
(errtest (defslambda foo))
(errtest (defslambda ("foo")))
(errtest (defslambda "foo"))
(errtest (defambda))
(errtest (defambda foo))
(errtest (defambda ("foo")))
(errtest (defambda "foo"))

(evaltest 7 (let* ((x 3) (y 4) (f (lambda (x y) (+ x y))) (z (f x y))) z))

(errtest (begin . exprs))
(errtest (dotimes (i 18) . exprs))
(errtest (dolist (i '(a b c)) . exprs))
(errtest (doseq (e #(a b c)) . exprs))
(errtest (let (()) (+ 3 4)))
(errtest (let ((x)) (+ 3 4)))
(errtest (let ((x (+ 3 "one"))) (+ 3 4)))
(errtest (let ((x (+ 3 "one"))) (+ 3 4)))
(errtest (let* (()) (+ 3 4)))
(errtest (let* ((x)) (+ 3 4)))
(errtest (let* ((x (+ 3 "one"))) (+ 3 4)))
(errtest (let* ((x (+ 3 "one"))) (+ 3 4)))
(errtest (let ((x 3)) x1))
(errtest (let* ((x 3)) x1))

(errtest (dotimes (i '(a b c)) . exprs))
(errtest (dolist (e 3) . exprs))
(errtest (doseq (e 3) . exprs))
(errtest (dotimes (i) i))
(errtest (dolist (e) i))
(errtest (doseq (e) i))
(errtest (forseq (e) i))
(errtest (dotimes (i "3") i))
(errtest (dotimes (i (+ 9 "nine")) i))
(errtest (dotimes ("i" 3) i))
(errtest (dolist ("e" '(x y)) i))
(errtest (doseq ("e" #(x y)) i))
(errtest (tryseq ("e" #(x y)) i))
(errtest (forseq ("e" #(x y)) i))
(errtest (dotimes (i "three") i))
(errtest (dolist ("e" #(x y)) e))
(errtest (doseq ("e" 'xyseq) e))
(errtest (forseq ("e" 'xyseq) e))
(errtest (doseq (e 'xyseq) e))
(errtest (forseq (e 'xyseq) e))
(errtest (dotimes (i 3) (+ i "one")))
(errtest (dolist (e '(a b c)) (string->symbol e)))
(errtest (doseq (e #(a b c)) (string->symbol e)))
(errtest (forseq (e #(a b c)) (string->symbol e)))
(errtest (dolist (e '#(a b c)) (symbol->string e)))
(errtest (tryseq))
(errtest (tryseq ()))
(errtest (tryseq (e 1)))
(errtest (tryseq ("e")))
(errtest (tryseq (e #(a b c)) . exprs))
(errtest (tryseq (e #(a b c)) (string->symbol e)))
(errtest (tryseq (e (append 'z #(a b c))) (string->symbol e)))
(errtest (while))
(errtest (while (set! x 3) x))
(errtest (until (set! x 3) x))
(errtest (while . broke))
(errtest (while (= 3 "three")))
(errtest (while (= 3 3) . body))
(errtest (until))
(errtest (until . broke))
(errtest (until (= 3 "three")))
(errtest (until (= 3 3.0) . body))

(evaltest 'void (doseq (x {}) x))

(errtest (prog1))
(errtest (prog1 (+ 2 "x") (* 3 9)))
(errtest (prog1 (+ 2 "x") (* 3 "nine") (* 3 9)))

(evaltest 9 (tryseq (e '(2 4 6 9)) (tryif (odd? e) e)))
(evaltest 9 (tryseq (e #(2 4 6 9)) (tryif (odd? e) e)))
(evaltest {} (tryseq (e #(2 4 6)) (tryif (odd? e) e)))
(evaltest {} (tryseq (e {}) (tryif (odd? e) e)))
(evaltest {} (tryseq (e '()) (tryif (odd? e) e)))
(evaltest {} (tryseq (e #()) (tryif (odd? e) e)))
(evaltest {} (tryseq (e "") (tryif (odd? e) e)))
(evaltest {} (tryseq (e "abc") (fail)))

(evaltest 'void (dolist (e '()) (+ 2 3)))
(evaltest 'void (dolist (e {}) (+ 2 3)))

(errtest (if))
(errtest (if (= 3 3)))
(errtest (when))
(errtest (unless))
(evaltest 'void (unless (fail) (error |NotSignalled|)))
(errtest (tryif))

(errtest (when (= 3 "three")))
(errtest (unless (= 9 'nine)))
(errtest (tryif (> 7 "seven")))

(evaltest 3 (let ((x 3))
	      (unless (pick x odd?) (set! x 4))
	      x))
(evaltest 4 (let ((x 3))
	      (when (pick x odd?) (set! x 4))
	      x))
(evaltest 3 (let ((x 3))
	      (unless (pick x odd?) (set! x 4))
	      x))

(errtest (case))
(errtest (case (* 3 'x)))
(errtest (case (* 3 5) (15 30)))
(evaltest 30 (case (* 3 5) ((15) 30)))
(errtest (case (* 3 5) (15 (+ 30 'z))))
(errtest (case (* 3 5) clause))

(errtest (tryif (= 3 3) (fail) (define foo 3)))

(errtest (let ((x 3)) (define q 9)))
(errtest (let ((x 3)) (define-local car)))
(errtest (let ((x 3)) (define-init y 9)))
(errtest (let ((x 3)) (defslambda (y z) 3)))
(errtest (let ((x 3)) (defamba (y z) 3)))

(errtest (define zzy (+ 3 "four")))

;;; XAPPLY

(applytest 8 xapply (lambda (x y) (+ x y)) #[x 3 y 5 z 9])
(applytest "foobar" xapply (lambda (x y) (glom x y)) #[x "foo" z "baz" y "bar"])

;;; Thunks

(applytester 3 (thunk (+ 2 1)))

;;; Lambda stuff

(define test-nlambda
  (nlambda 'test (x y) (+ x y)))
(applytester #t applicable? test-nlambda)
(applytester #t procedure? test-nlambda)
(applytester 5 test-nlambda 3 2)
(applytester "test" procedure-name test-nlambda)
(applytester 'test procedure-symbol test-nlambda)

(define test-nlambda-copy (deep-copy test-nlambda))
(applytester #f eq? test-nlambda test-nlambda-copy)
(applytester #f equal? test-nlambda test-nlambda-copy)
(applytester 5 test-nlambda-copy 3 2)

(define test-other-nlambda
  (nlambda "test" (x y) (+ x y)))
(errtest (nlambda '(test) (x y) (+ x y)))
(errtest (nlambda))
(errtest (nlambda "fcn"))
(errtest (nlambda (glom "fcn" usename)))

(define test-def (def (td x y) (+ x y)))
(applytester #t applicable? test-def)
(applytester #t procedure? test-def)
(applytester #f non-deterministic? test-def)
(applytester #f synchronized? test-def)
(applytester 5 test-def 3 2)
(applytester "td" procedure-name test-def)
(applytester 'td procedure-symbol test-def)
(evaltest #f (bound? testfn))
(errtest (def "fcn" (+ 2 3)))
(errtest (def fcn (+ 2 3)))
(errtest (def (3 x) (+ 2 3)))

(define test-defsync (defsync (td x y) (+ x y)))
(applytester #t applicable? test-defsync)
(applytester #t procedure? test-defsync)
(applytester #f non-deterministic? test-defsync)
(applytester #t synchronized? test-defsync)
(applytester 5 test-defsync 3 2)
(applytester "td" procedure-name test-defsync)
(applytester 'td procedure-symbol test-defsync)
(evaltest #f (bound? testfn))

(define test-defamb (defamb (td x y) (+ x y)))
(applytester #t applicable? test-defamb)
(applytester #t procedure? test-defamb)
(applytester #t non-deterministic? test-defamb)
(applytester #f synchronized? test-defamb)
(applytester 5 test-defamb 3 2)
(applytester "td" procedure-name test-defamb)
(applytester 'td procedure-symbol test-defamb)
(evaltest #f (bound? testfn))

(define test-sappend (def (string-append x (y "something")) (append x y)))
(applytester "foobar" test-sappend "foo" "bar")
(applytester "foosomething" test-sappend "foo")
(errtest (test-sappend))

(define test-sappend-copy (deep-copy test-sappend))
(applytester "foobar" test-sappend-copy "foo" "bar")
(applytester "foosomething" test-sappend-copy "foo")
(errtest (test-sappend-copy))
(errtest (test-sappend-copy 'foo))

;;;; Testing COND apply

(define-tester (cond-tester x)
  (cond ((number? x) x)
	((string? x) => list) 
	((symbol? x) => messedup)
	(else =>)))
(applytester 9 cond-tester 9)
(applytester '(#t) cond-tester "string")
(errtest (cond-tester 'symbol))
(errtest (cond-tester '(pair)))

;;; Some more tests

(applytester #t packet? (dtype->packet (make-hashset)))
(applytester #t packet? (dtype->packet (choice->hashset {1 2 3 "four"})))
(applytester #t packet? (dtype->packet (choice->hashset {1 2 3 "four"})))

(applytester #t packet? (dtype->packet [x 3 y (+ 2 3)]))

;;; DEFIMPORT

(errtest (defimport))
(errtest (defimport bar))
(errtest (defimport "foo" bar))
(errtest (defimport foo 'nomodule))
(errtest (defimport $num$ 'stringfmts))

(define (test-docstrings x y)
  "This takes two arguments"
  "They can be anything"
  "It adds them"
  (+ x y))
(applytest 
 "`(test-docstrings x y)`\nThis takes two arguments\nThey can be anything\nIt adds them"
 documentation test-docstrings)
(set! test-docstrings #f)
(define-tester (test-docstrings x y . more)
  "This takes at least two arguments"
  "They can be anything"
  "It adds them and conses them to all the others"
  (cons (+ x y) more))
(applytest 
 "`(test-docstrings x y [more...])`\nThis takes at least two arguments\nThey can be anything\nIt adds them and conses them to all the others"
 documentation test-docstrings)
(set! test-docstrings #f)

;;; Withenv tests

(define x 3)
(withenv #f (set! x 9) (applytest 10 1+ x))
(applytester 4 1+ x)

(withenv ((x 9)) (applytest 10 1+ x))
(withenv #[x 9] (applytest 10 1+ x))
(withenv [x 9] (applytest 10 1+ x))

(errtest (withenv))
(errtest (withenv ((x 3) (y 4) (z))))
(errtest (withenv ((x 3) (y 4) z)))
(errtest (withenv foo (+ 3 9)))
(errtest (withenv "bar" (+ 3 9)))

;;; Loading stuff

(dynamic-load (abspath "../lib/kno/sqlite.so"))
(dynamic-load "sqlite")
(errtest (dynamic-load "sqheavy" #t))
(errtest (dynamic-load 'sqlite))

(config 'used_modules)

(reload-module 'ezrecords)

;;; All done

(test-finished "EVALTEST")


