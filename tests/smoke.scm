(use-module 'reflection)
(use-module 'logger)

(applytest 17 + 11 3 3)
(evaltest 17 ( + 11 3 3))
(define (p x y) (+ x y 17))
(applytest 42 p 12 13)
(evaltest 42 (p 12 13))
(define twelve 12)
(applytest 42 p twelve 13)
(evaltest 42 (p twelve 13))
(let ((thirteen 13))
  (applytest 42 p twelve thirteen)
  (evaltest 42 (p twelve thirteen)))

(applytest "foobar" glom "foo" "bar")

(define (g x y) (glom x y))
(applytest "foobar" g "foo" "bar")

