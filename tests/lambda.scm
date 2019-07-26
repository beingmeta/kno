;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{reflection stringfmts optimize})
(optimize! 'stringfmts)
;; TODO: This leaks for some reason. Find it out.
;;(reoptimize! 'stringfmts)

(errtest ((lambda (x (y . err) (z)) (+ x 3)) 8))
(errtest (nlambda (string-append 3 "bar") (x y) (+ x y)))
(errtest ((lambda (x "y" (z)) (+ x 3)) 8))

(errtest ((lambda) 8))
(errtest ((ambda) 8))
(errtest ((nlambda)))
(errtest ((slambda) 8))
(errtest ((sambda) 8))

(applytest #t procedure? (def (times p q) (* p q)))
(applytest #t procedure? (def ("times" p q) (* p q)))
(applytest #t non-deterministic? (defamb ("times" p q) (* p q)))

(applytest 5 (nlambda 'take5 (n) (+ 2 n)) 3)
(applytest 5 (nlambda "take5" (n) (+ 2 n)) 3)
(applytest 5 (nlambda (glom "take" 5) (n) (+ 2 n)) 3)
(errtest (nlambda (append "take" 5) (n) (+ 2 n)))
(errtest (nlambda (cons "take" 5) (n) (+ 2 n)))

(applytest packet? dtype->packet (lambda (x) (1+ x)))

;;; Arity checking

(define (doctest-1 x y)
  "<p>This is doctest-1</p>"
  x)
(define (doctest-2 x y)
  "`(doctest-2 x y)`"
  x)
(define (doctest-2 x y)
  "\nSome test"
  x)

(applytest 3 doctest-1 3 4)
(applytest 'err doctest-1 3)
(applytest 'err doctest-1 3 4 5)

;;; Thunks

(define just17 (thunk 17))
(define just42 (thunk (* 6 7)))

(applytester 17 just17)
(applytester 42 just42)
(applytester 3 (thunk (+ 2 1)))

;;; Call/cc

(evaltest 3 (call/cc (lambda (quit) (if (= 3 3) (quit 3) #f))))

(errtest (define (bad-def x "y" (z)) (+ x 3)))
(errtest (define-synchronized (bad-def x "y" (z)) (+ x 3)))
(errtest (defambda (bad-def x "y" (z)) (+ x 3)))

(errtest (der (bad-def x "y" (z)) (+ x 3)))
(errtest (defamb (bad-def x "y" (z)) (+ x 3)))
(errtest (defsync (bad-def x "y" (z)) (+ x 3)))

;;; XAPPLY

(applytest 8 xapply (lambda (x y) (+ x y)) #[x 3 y 5 z 9])
(applytest "foobar" xapply (lambda (x y) (glom x y)) #[x "foo" z "baz" y "bar"])
(applytest "foobar" xapply (lambda (x (y "bar")) (glom x y)) #[x "foo" z "baz"])
(applytest 'err xapply (lambda (x (y "bar")) (glom x y)) "just a string")

(applytest "foobar" xapply (slambda (x y) (string-append x y)) #[x "foo" z "baz" y "bar"])
(applytest 'err xapply (slambda (x y) (string-append x y)) #[x "foo" z "baz" y bar])

(define (pairget pair var)
  (if (pair? pair)
      (get (car pair) var)
      (irritant pair |NotAPair| pairget)))

(applytest "foobar" xapply 
	   (lambda (x (y "bar")) (glom x y))
	   '(#[x "foo" z "baz"])
	   pairget)

(applytest 'err xapply 
	   (lambda (x (y "bar")) (glom x y))
	   #[x "foo" z "baz"]
	   pairget)

;;; Unparsing cases

(do-choices (fn (with-sourcebase #f {(lambda (x) (+ x 3))
				     (lambda x (cons 3 x))
				     (lambda x (cons 3 x))}))
  (applytest string? lisp->string fn))


;;; Env reset (return to static env)

(define extfn #f)

(let ((x "three") (y "four") (z "zee"))
  (set! extfn (lambda () (glom x y)))
  (set! z "zoom")
  (%env/reset!)
  (applytest equal? z "zee"))

(applytest "threefour" extfn)

(define outer-z "eleven")
(errtest (withenv #["outer-z" 8] (+ outer-z 3)))


;;; With declarations and docstrings

(define (fact-iter n (result 1))
  "This is a tail recursive version of factorial"
  #[TAILCALL #t
    AUTHOR "the usual"]
  [SOURCEFILE (get-component)]
  (if (< n 1) 
      result
      (fact-iter (-1+ n) (* n result))))
