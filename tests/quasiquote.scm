;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(evaltester '(list 3 4) `(list ,(+ 1 2) 4)
	    'quasiquote)
(evaltester '(list a (quote a)) (let ((name 'a)) `(list ,name ',name))
	    'quasiquote)
(evaltester '(a 3 4 5 6 b) `(a ,(+ 1 2) ,@(map abs '(4 -5 6)) b)
	    'quasiquote)
(evaltester '((foo 7) . cons)
	    `((foo ,(- 10 3)) ,@(cdr '(c)) . ,(car '(cons)))
	    'quasiquote)

;;; sqt is defined here because not all implementations are required to
;;; support it.
(define (sqt x) (do ((i 0 (+ i 1))) ((> (* i i) x) (- i 1))))
(test-optimize! sqt)

(evaltester '#(10 5 2 4 3 8) `#(10 5 ,(sqt 4) ,@(map sqt '(16 9)) 8) 'quasiquote)
(evaltester 5 `,(+ 2 3) 'quasiquote)
;; Don't optimize this to avoid breaking the equality test
(evaltest '(a `(b ,(+ 1 2) ,(foo 4 d) e) f)
	    `(a `(b ,(+ 1 2) ,(foo ,(+ 1 3) d) e) f)
	    'quasiquote)
(evaltester '(a `(b ,x ,'y d) e)
	    (let ((name1 'x) (name2 'y)) `(a `(b ,,name1 ,',name2 d) e))
	    'quasiquote)
(evaltester '(list 3 4)
	    (quasiquote (list (unquote (+ 1 2)) 4))
	    'quasiquote)
(evaltester '`(list ,(+ 1 2) 4)
	    '(quasiquote (list (unquote (+ 1 2)) 4))
	    'quasiquote)

(errtest `(3 4 ,(if #f 6)))
(errtest `(3 4 ,@(if #t 6 '(6))))
(errtest `(3 4 ,@(if #t '(6 . 7))))
(errtest `(3 4 ,@(void)))
(errtest `(3 4 ,@(list unbound 9 )))

(errtest `#(3 4 ,(if #f 6)))
(errtest `#(3 4 ,@(if #t 6 '(6))))
(errtest `#(3 4 ,@(if #t '(6 . 7))))
(errtest `#(3 4 ,@(void)))
(errtest `#(3 4 ,@(list unbound 9 )))

(evaltest '(3 4 5 6) `(3 4 ,@(vector 5 6)))
(evaltest #(3 4 5 6) `#(3 4 ,@(vector 5 6)))
(evaltest #(3 4 5 6) `#(3 4 ,@(list 5 6)))

(evaltest '{3 4 5} `{,(+ 1 2) ,(* 2 2) ,(* 5/2 2)})
(evaltest '{(+ 1 2) (* 2 2) (* 5/2 2)} `{(+ 1 2) (* 2 2) (* 5/2 2)})

(errtest `(one two (three four . ,(error 'just-because))))
(errtest `(one two `(three four . , ,(error 'just-because))))
(errtest `(one two (three four . , `(,(error 'just-because)))))
(errtest `(one two (three four . , `(,@(error 'just-because)))))

(errtest `(one two . ,(error 'just-because)))
(errtest `(one two . ,(void)))
(errtest `(one two (three four . ,(void))))
(errtest `(one two `(three four . , ,(void))))
(errtest `(one two (three four . , `(,(void)))))
(errtest `(one two (three four . , `(,@(void)))))

(errtest `(one two (unquote)))
(errtest `(one two (unquote . cdr)))

(evaltest #(3 4 5) `#(3 4 ,@(list) 5))
(errtest `#(3 4 ,@(cons 6 . cdr) 5))

(evaltest #[x 3 y 9] `#[x 3 ,(string->symbol "y") ,(* 3 3)])
(errtest `{,(+ 2 3) (* 3 4) ,(+ 8 "eight")})
