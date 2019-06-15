;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(define textmatch (get (get-module 'texttools) 'textmatch))

(define (test-lexrefs (v 'foo))
  (let ((lexref (%lexref 0 1))
	(value v)
	(bad-lexref-1 (%lexref 0 5))
	(bad-lexref-2 (%lexref 8 0)))
    (lisp->string lexref)
    (dtype->packet lexref)
    (applytest '(0 . 1) %lexrefval lexref)
    (applytest '(8 . 0) %lexrefval bad-lexref-2)
    (applytest #t %lexref? lexref)
    (applytest #f %lexref? #f)
    (applytest #f %lexref? "three")
    (applytest #f %lexref? 3)
    (errtest (eval bad-lexref-1))
    (errtest (eval bad-lexref-2))
    (evaltest value (eval lexref))))

(test-lexrefs)
(test-lexrefs "foo")
(test-lexrefs 89)

(define (test-coderefs)
  (let ((coderef (%coderef 16)))
    (applytest #t coderef? coderef)
    (applytest #f coderef? '(code))
    (applytest #f coderef? 0xc0de)
    (applytest? string? lisp->string coderef)
    (applytest? packet? dtype->packet coderef)
    (applytest #t coderef? coderef)
    (applytest 16 %coderefval coderef)))

(test-coderefs)

(define (test-opcodes)
  (let ((op1 (name->opcode "op_until"))
	(op2 (name->opcode 'op_assign))
	(op3 (name->opcode 'op_center))
	(bad-opcode (make-opcode 1666)))
    (errtest (name->opcode 0x334))
    (applytest #t opcode? (name->opcode "op_until"))
    (applytest #t opcode? bad-opcode)
    (applytest #f opcode? "op_until")
    (applytest #f opcode? 'op_until)
    (applytest (name->opcode "op_until") name->opcode 'op_until)
    (applytest #t string? (lisp->string (name->opcode "op_until")))
    (applytest "OP_UNTIL" opcode-name op1)
    (applytest #t opcode? op1)
    (applytest #t opcode? op2)))

(test-opcodes)

(define (test-bindings (p 3) (q) (z #default) (g #f))
  (evaltest #f (void? p))
  (evaltest #t (void? q))
  (evaltest #f (default? p))
  (evaltest #t (default? z))
  (errtest (setfalse! g))
  (errtest (setfalse! g (* 8 'p)))
  (evaltest 'void (setfalse! g 17))
  (errtest (default! q (* p 'p)))
  (errtest (default! r 9))
  (errtest (default!))
  (errtest (default! r))
  (default! q (* p p))
  (evaltest 17 g)
  (evaltest #f (symbol-bound-in? 'x #[a 3 b 4]))
  (evaltest #t (symbol-bound-in? 'a #[a 3 b 4]))
  (evaltest #t (symbol-bound-in? 'p (%env)))
  (evaltest #f (symbol-bound-in? 'xyzddr (%env)))
  (let ((x (+ p p))
	(y (+ q q))
	(vals {}))
    (set+! vals x)
    (set+! vals y)
    (set+! vals p)
    (set+! vals q)
    (errtest (set+! vals (* p 'p)))
    vals))

(test-bindings)

(define (test-macros)
  (let ((swapf (macro expr 
		 (let ((arg1 (get-arg expr 1))
		       (arg2 (get-arg expr 2)))
		   `(let ((tmp ,arg1))
		      (set! ,arg1 ,arg2)
		      (set! ,arg2 tmp)))))
	(x 3)
	(y 4))
    (applytest? string? lisp->string swapf)
    (swapf x y)
    (applytest -1 - y x)))

(test-macros)

(applytest #t string? (lisp->string if))
(applytest #t packet? (dtype->packet if))

(evaltest #f (bad? 3))
(evaltest #f (bad? if))

(evaltest textmatch (eval `(%modref ,(get-module 'texttools) textmatch)))

(evaltest 3 (quote 3))
(evaltest 3 (eval (list 'quote 3)))

(errtest {(+ 2 3) (+ 2 'a) (+ 3 9)})

(define (broken x y)
  (fizzbin x))

(applytest "broken" procedure-name broken)
(applytest "IF" procedure-name if)
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

(applytest 3 get [x "three" y 3 z 'three] 'y)
(applytest "three" get [x "three" y 3 z 'three] 'x)
(applytest 'three get [x "three" y 3 z 'three] 'z)
(errtest [x "three" y 3 z three])

(evaltest #f (unbound? unbound?))
(evaltest #t (unbound? xyzayzdrs))

(define x23f9b #f)

(errtest (letrec))
(errtest (letrec 3))
(errtest (reverse '(a b c . d)))

(evaltest #t (void? (set! x23f9b #t)))
(evaltest #f (void? (not x23f9b)))
(evaltest #t (void? (void)))
(evaltest #t (void? (void (+ 2 3))))
(evaltest #t (void? (void (+ 2 3) (cons "foo" "bar"))))

(evaltest #t (default? #default))
(evaltest #f (default? 99))

(define s "string")
(evaltest #f (constant? "string"))
(evaltest #f (constant? s))
(evaltest #t (constant? #f))
(applytest #t (constant? #default))

(evaltest #f (immediate? "string"))
(evaltest #f (immediate? @1/89))
(evaltest #f (immediate? 333))
(evaltest #t (immediate? #f))
(evaltest #t (immediate? 'foo))
(evaltest #t (immediate? #eof))

(applytest #t applicable? car)
(applytest #t applicable? broken)
(applytest #f applicable? 3)
(applytest #f applicable? "fcn")
(applytest #f applicable? 3.5)
(applytest #f applicable? '(lambda (x) (1+ X)))
(applytest #f applicable? #(lambda (x) (1+ X)))


(evaltest #t (defined? broken))
(evaltest #f (defined? xyzayzdrs))
(errtest (defined? 9))
(errtest (defined? "nein"))

(applytest #t environment? (%env))
(applytest #t symbol-bound-in? 'x23f9b (%env))
(applytest #f symbol-bound-in? 'xyzayzdrs (%env))

(evaltest #t (symbol-bound? 'x23f9b))
(evaltest #f (symbol-bound? 'xyzayzdrs))
(evaltest #t (let ((xyzayzdrs 9)) (symbol-bound? 'xyzayzdrs)))
(evaltest #t (let ((xyzayzdrs 9)) (symbol-bound? 'x23f9b)))
(evaltest #f (let ((xyzayzdrs 9)) (symbol-bound? 'x23f9bxyz)))
(errtest (symbol-bound? "x23f9b"))
(errtest (symbol-bound? 'car '(ab)))
(errtest (symbol-bound? 'car '(ab)))

(applytest "one" get-arg '("one" "two" "three") 0)
(applytest "two" get-arg '("one" "two" "three") 1)
(evaltest #t (void? (get-arg '("one" "two" "three") 5)))
(errtest (get-arg '("one" "two" "three") "one"))
(errtest (get-arg #("one" "two" "three") 0))

(errtest (apply {+ - 5} (list 3 4)))
(errtest (apply {+ - car} (list 3 4)))

(errtest (symbol-bound?))
(errtest (symbol-bound? xyzayzdrs))

(applytest #t string? (documentation test-opcodes))

(errtest (%choiceref #{a "b" c} 8))
(evaltest 5 (%choiceref (qc {5 (symbol->string '{a b c})}) 0))

(applytest 0 compare 5 5)
(applytest? positive? compare 5 4)
(applytest? negative? compare 4 5)

(applytest 0 compare 'five 'five)
(applytest 0 compare "five" "five")
(applytest? positive? compare "six" "five")
(applytest? negative? compare "five" "six")

(applytest 0 compare '(5 3) '(5 3))
(applytest? negative? compare '(5 3) '(6 3))
(applytest? positive? compare '(5 4) '(5 3))

(applytest 0 compare #(5 3) #(5 3))
(applytest? negative? compare #(5 3) #(6 3))
(applytest? positive? compare #(5 4) #(5 3))


(applytest 0 compare/quick 5 5)
(applytest? positive? compare/quick 5 4)
(applytest? negative? compare/quick 4 5)

(applytest 0 compare/quick 'five 'five)
(applytest 0 compare/quick "five" "five")

(applytest 0 compare/quick '(5 3) '(5 3))

(applytest 0 compare/quick #(5 3) #(5 3))

(applytest "foobar" dontopt "foobar")

(applytest #t eqv? 3 3)
(applytest #t eqv? 3.0 3.0)
(applytest #f eqv? 3.0 5.0)

(applytest #t flonum? 3.5)
(applytest #f flonum? 3)
(applytest #f flonum? "three")

(applytest #t zero? 0.0)
(applytest #f zero? 0.1)
(applytest #f zero? -0.1)
(applytest #f zero? 1/3)
(applytest #f zero? (* 1024 1024 1024 1024 1024 1024 1024))
(applytest #f zero? "one")
(applytest #f zero? '(0))

(applytest #t contains? "foo" "foo")
(applytest #f contains? "bar" "foo")
(applytest #f contains? {} "foo")
(applytest #f contains? {} "foo")
(applytest #t contains? "foo" {"foo" "bar" "baz"})
(applytest #f contains?  {"foo" "bar" "baz"} "foo")
(applytest #f contains?  {"foo" "bar" "baz"} {"foo" "bar"})

(applytest #t true? #t)
(applytest #f true? #f)
(applytest #t true? 3)
(applytest #t true? "foo")
(applytest #t true? '("foo" "bar"))
(applytest {} true? (choice))

(applytest #f false? #t)
(applytest #t false? #f)
(applytest #f false? 3)
(applytest #f false? "foo")
(applytest #f false? '("foo" "bar"))
(applytest {} true? (choice))

(errtest (intern 3))
(errtest (intern 'three))
(errtest (intern #"foo"))

(errtest (string->lisp 'symbol))
(errtest (string->lisp #"packet"))
(errtest (string->lisp '(list)))

(applytest 'foo parse-arg 'foo)
(applytest 3 parse-arg 3)
(applytest 3 parse-arg "3")
(applytest #(a b) parse-arg "#(a b)")
(applytest '(a b) parse-arg "(a b)")
(applytest @1/889 parse-arg "@1/889")
(applytest #(a b) parse-arg ":#(a b)")
(applytest '(a b) parse-arg ":(a b)")
(applytest @1/889 parse-arg ":@1/889")
(applytest #f parse-arg "#f")
(applytest #t parse-arg "#t")

(applytest ":#(a b)" unparse-arg #(a b))
(applytest ":(a b)" unparse-arg '(a b))
(applytest "@1/889" unparse-arg @1/889)
(applytest ":foo" unparse-arg 'foo)
(applytest "33" unparse-arg 33)

(applytest "IF" procedure-name if)
(applytest "CAR" procedure-name car)
(applytest "broken" procedure-name broken)

(applytest #t ->lisp "#t")
(applytest 33.3333 ->lisp "33.3333")
(applytest "two words" ->lisp "two words")
(applytest "separated\tby\twords" ->lisp "separated\tby\twords")
(applytest @1/8 ->lisp "@1/8")
(applytest 3 ->lisp 3)
(applytest @1/8 ->lisp @1/8)
(applytest #f ->lisp #f)
(applytest #t ->lisp #t)
(applytest '(a b c) ->lisp '(a b c))
(applytest #(a b c) ->lisp #(a b c))

(applytest #t pair? (getsourceinfo))

(applytest "#!5" hashref #f)
(applytest "#!86" hashref 33)
(applytest? string? hashref "foo")
(applytest? string? hashref '(x y))

(applytest? fixnum? hashptr 33)
(applytest? fixnum? hashptr 'thirtythree)
(applytest? integer? hashptr "thirtythree")
(applytest? integer? hashptr '(a b))

;;;; Structure eval tests

(evaltest #(3 4 5) #.(3 4 (+ 4 1)))
(evaltest #[foo 3 bar 5] #.[foo (+ 2 1) bar (+ (* 2 2) 1)])
(errtest #.(3 4 (+ 4 "one")))
(errtest #.[foo (+ 2 "one") bar (+ (* 2 2) 1)])

;;; Other cases

(define (return-void) (void))
(applytest 'void return-void)

;;; Syntax errors

(define exprs '((+ 2 3) (* 3 9)))

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

;;; Loading stuff

(dynamic-load "sqlite")
(errtest (dynamic-load "sqheavy" #t))
(errtest (dynamic-load 'sqlite))

(config 'used_modules)

;;; All done

(test-finished "EVALTEST")


