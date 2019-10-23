;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(define opts1 #[x "three" y "four" z (five)])
(define opts2 #[x "nine" a "apple" z {}])
(define opts12 (list opts1 opts2))
(define opts21 (list opts2 opts1))

(evaltest "three" (getopt opts12 'x #f))
(evaltest "nine" (getopt opts21 'x #f))

(errtest (getopt))
(errtest (getopt opts12))
(errtest (getopt opts1s))
(errtest (getopt opts12 name88))
(errtest (getopt "notopts" 'name))

(applytest #t opts? #[x 3])
(applytest #t opts? [x (+ 1 2)])
(applytest #f opts? 'x)
(applytest #f opts? "x")
(applytest #t opts? (cons [x (+ 1 2)] #f))
(applytest #t opts? (list [x (+ 1 2)] #f))
(applytest #t opts? (cons #[x (+ 1 2)] #f))
(applytest #t opts? (list #[x (+ 1 2)] #f))

(applytest 'err %getopt)
(applytest 'err %getopt "foo" 'x)
(applytest 'err %getopt #[x 3])
(applytest 3 %getopt #[x 3] 'x)
(applytest #f %getopt #[x 3] 'y)
(applytest {} %getopt #[x 3] 'y {})
(applytest "bar" %getopt #[x 3] 'y "bar")

(applytest #[] opt+)
(applytest #[foo 3] opt+ 'foo 3)
(applytest #[foo #t] opt+ 'foo)
(applytest #[foo bar bar 3] opt+ 'foo 'bar 'bar 3)
(applytest #[foo bar bar 3 quux "q"] opt+ 'foo 'bar 'bar 3 'quux "q")

(applytest 4 %getopt (opt+ #[x 3 y 4]) 'y)
(applytest 9 %getopt (opt+ #[x 3 y 4] 'y 9) 'y)
(applytest 5 %getopt (opt+ #[x 3 y 4] 'y 9 #[y 5]) 'y)

(evaltest 4 (getopt (opt+ #[x 3 y 4]) 'y))
(evaltest 9 (getopt (opt+ #[x 3 y 4] 'y 9) 'y))
(evaltest 5 (getopt (opt+ #[x 3 y 4] 'y 9 #[y 5]) 'y))

(applytest "foo" %getopt {#[var "foo"] #[bar "baz"]} 'var)
