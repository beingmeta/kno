;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'ezrecords)

(defrecord type1
  x y (z 3) (q 4))

(defrecord (type2 mutable)
  x y (a 8) (b 9))

(defrecord (type3 opaque)
  x y (m 8) (n 9))

(define type1.1 (cons-type1 11 12))
(define type2.1 (cons-type2 33 99))
(define type2.2 (cons-type2 77 33))
(define type3.1 (cons-type3 2000 42))

(applytester "#<<type3 2000 42 8 9>>" lisp->string type3.1)
(applytester "#%(type1 11 12 3 4)" lisp->string type1.1)

(applytester {type1.1 type3.1} pick-compounds {type1.1 9 type3.1 "nine" #"nine" '(9)})
(applytester type1.1 pick-compounds {type1.1 9 type3.1 "nine" #"nine" '(9)} 'type1)
(applytester {type1.1 type3.1} pick-compounds {type1.1 type3.1})
(applytester {type2.1 type2.2} pick-compounds {type2.1 type2.2} 'type2)

(applytester 'type1 car (unpack-compound type1.1))
(applytester vector? cdr (unpack-compound type1.1))
(applytester 'err unpack-compound type1.1 'type2)

(applytester 'type2 car (unpack-compound type2.1))
(applytester vector? cdr (unpack-compound type2.1 'type2))

(applytester #t compound? type1.1)
(applytester #t compound? type1.1 'type1)
(applytester #f compound? type1.1 'type2)

(applytester #t tagged? type1.1 'type1)
(applytester #f tagged? type1.1 'type2)

(applytester #t compound? type2.1)
(applytester #t compound? type2.1 'type2)
(applytester #f compound? type2.1 'type1)

(applytester #t compound? type3.1)
(applytester #t exists compound? type1.1 '{type1 type2})
(applytester #t exists compound? type2.1 '{type1 type2})
(applytester #f exists compound? type3.1 '{type1 type2})

(applytester #t type1? type1.1)
(applytester #f compound-mutable? type1.1)
(applytester #f compound-opaque? type1.1)
(applytester 'type1 compound-tag type1.1)
(applytester 4 compound-length type1.1)
(applytester 3 type1-z type1.1)
(applytester 11 type1-x type1.1)

(applytester #t compound-mutable? type2.1)
(applytester #f compound-opaque? type2.1)
(applytester 'type2 compound-tag type2.1)
(applytester 4 compound-length type2.1)
(applytester 8 type2-a type2.1)
(applytester 33 type2-x type2.1)
(applytester 'err type1-x type2.1)

(applytester 33 compound-ref type2.1 0)
(applytester 33 compound-ref type2.1 0 'type2)
(applytester 'err compound-ref type2.1 0 'type1)
(applytester 'err compound-ref type2.1 22 'type2)
(applytester 'err compound-ref type2.1 22)
(applytester 'err compound-ref type2.1 1/2)
(applytester 'err compound-ref type2.1 #"packet")

(applytester 33 compound-ref type2.1 0)
(evaltest 77 (begin (compound-set! type2.1 0 77)
	       (compound-ref type2.1 0)))
(errtest (compound-set! type1.1 0 77))
(errtest (compound-set! type2.1 0 77 'type1))
(errtest (compound-set! type2.1 29 77 'type2))
(errtest (compound-set! type2.1 29 77))
(evaltest (* 2 77) (begin (compound-modify! type2.1 'type2 0 * 2)
		     (compound-ref type2.1 0)))
(errtest (compound-modify! type2.1 'type1 0 * 2))
(errtest (compound-modify! type2.1 'type2 11 * 2))

(evaltest 88 (begin (compound-set! {type2.1 type2.2} 0 88 'type2)
 	       (compound-ref type2.1 0)))
(errtest (compound-set! {type2.1 type2.2} 22 88 'type2))
(errtest (compound-set! {type2.1 type2.2} 0 88 'type1))
(errtest (compound-set! {type2.1 "type2.2"} 22 88 'type2))
(errtest (compound-set! {type2.1 type2.2} -5 88 'type2))
(errtest (compound-set! {type2.1 type2.2} 1/2 88 'type2))

(errtest (compound-set! type1.1 0 88 'type1))
(errtest (compound-set! type1.1 0 88))

(evaltest 89 (begin (compound-modify! {type2.1 type2.2} 'type2 0 + 1)
 	       (compound-ref type2.1 0)))
(evaltest 90 (begin (compound-modify! {type2.1 type2.2} 'type2 0 '+ 1)
	       (compound-ref type2.1 0)))
(evaltest 89 (begin (compound-modify! {type2.1 type2.2} 'type2 0 '- 1)
	       (compound-ref type2.1 0)))
(evaltest {4242 99}
	  (begin (compound-modify! type2.1 'type2 1 'add 4242)
	    (compound-ref type2.1 1 'type2)))
(evaltest 4242
	  (begin (compound-modify! type2.1 'type2 1 'drop 99)
	    (compound-ref type2.1 1 'type2)))
(evaltest 9999
	  (begin (compound-modify! type2.1 'type2 1 'store 9999)
	    (compound-ref type2.1 1 'type2)))
(evaltest 10000
	  (begin (compound-modify! type2.1 'type2 1 1+)
	    (compound-ref type2.1 1 'type2)))
(applytester {} compound-modify! {} 'type2 1 1+)

(applytester #f compound-mutable? type3.1)
(applytester #t compound-opaque? type3.1)
(applytester 'type3 compound-tag type3.1)
(applytester 4 compound-length type3.1)

(define (iscompound? x) (compound? x))

(applytester iscompound? make-compound 'type11 3 4 "foo" '(bar))
(applytester iscompound? make-opaque-compound 'type11 3 4 "foo" '(bar))
(applytester iscompound? make-mutable-compound 'type11 3 4 "foo" '(bar))
(applytester iscompound? make-opaque-mutable-compound 'type11 3 4 "foo" '(bar))
(applytester iscompound? make-xcompound 'type11 #f #f #f #f 3 4 "foo" '(bar))
(applytester sequence? make-xcompound 'type11 #f #f #f 3 #t 4 "foo" '(bar))
(applytest length= 4 make-xcompound 'type11 #f #f #f #t 3 4 "foo" '(bar))
(applytest length= 2 make-xcompound 'type11 #f #f #f 2 3 4 "foo" '(bar))

(applytester compound-opaque? make-opaque-compound 'type11 3 4 "foo" '(bar))
(applytester compound-mutable? make-mutable-compound 'type11 3 4 "foo" '(bar))
(applytester compound-mutable? make-opaque-mutable-compound 'type11 3 4 "foo" '(bar))
(applytester compound-opaque? make-opaque-mutable-compound 'type11 3 4 "foo" '(bar))

(applytester 'err sequence->compound #("a" b 3) 'typeX #t #f 1/2)
(applytester #f sequence? (sequence->compound #("a" b 3) 'typeX #t #f #f #f))

(applytester iscompound? sequence->compound #("a" b 3) 'typeX)
(applytester iscompound? sequence->compound #("a" b 3) 'typeX #f #f 0)
(applytester compound-mutable? sequence->compound #("a" b 3) 'typeX #t #f 0)
(applytester compound-opaque? sequence->compound #("a" b 3) 'typeX #f #t 0)
(applytester compound-opaque? sequence->compound #("a" b 3) 'typeX #t #t 0)
(applytester compound-mutable? sequence->compound #("a" b 3) 'typeX #t #t 0)

(define vec-compound (sequence->compound #("a" b 3 4 6 7) 'typeX #t #f 0 #f))
(define vec2-compound (sequence->compound #("a" b 3 4 6 7) 'typeX #t #f 2 #f))
(applytester #t iscompound? vec-compound)
(applytester #f compound-opaque? vec-compound)
(applytester #t compound-mutable? vec-compound)
(applytester #t sequence? vec-compound)
;;(applytester #t length vec-compound)
(applytester 3 compound-ref vec-compound 2)
(applytester 3 compound-ref vec-compound 2 'typex)
(applytest 6 length vec-compound)
(applytest 4 length vec2-compound)
(applytester "#%(typex \"a\" b 3 4 6 7)" lisp->string vec-compound)

(errtest (sequence->compound 'foo 'type4))
(applytester #%(TYPE4 A B C) sequence->compound #(A B C) 'type4)
(applytester #%(TYPE4 A B C) sequence->compound '(A B C) 'type4)
(applytester "#%(type4 a b c)" lisp->string (sequence->compound '(A B C) 'type4))
(define (type4-stringfn c)
  (stringout "#<TYPE4" (doseq (elt c) (printout " " elt))
    ">"))
(type-set-stringfn! 'type4 type4-stringfn)
(applytester "#<TYPE4 a b c>" lisp->string (sequence->compound '(A B C) 'type4))
(type-set-stringfn! 'type4 #f)
(applytester "#%(type4 a b c)" lisp->string (sequence->compound '(A B C) 'type4))
(define (type4-wrong-stringfn c) #f)
(type-set-stringfn! 'type4 type4-wrong-stringfn)
(applytester "#%(type4 a b c)" lisp->string (sequence->compound '(A B C) 'type4))
(type-set-stringfn! 'type4 type4-stringfn)
(applytester "#<TYPE4 a b c>" lisp->string (sequence->compound '(A B C) 'type4))

(define (type4-consfn . args)
  (sequence->compound (cons "extra" args) 'type4))
(type-set-consfn! 'type4 type4-consfn)
(applytester "extra" compound-ref #%(type4 3 4 5 "nine") 0)
(type-set-consfn! 'type4 #f)
(applytester 3 compound-ref #%(type4 3 4 5 "nine") 0)
(type-set-consfn! 'type4 type4-consfn)

(define type4.1 #%(type4 3 4 5 "nine"))

(applytester type4-consfn type-handlers 'type4 'consfn)
(applytester slotmap? type-props 'type4)

(applytester type4-consfn type-handlers type4.1  'consfn)
(applytester slotmap? type-handlers type4.1)

(type-set! 'type4 'someprop "someval")
