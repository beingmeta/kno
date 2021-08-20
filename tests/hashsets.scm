;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module 'reflection)

(define hs1 (make-hashset))

(define vec (map (lambda (x) (cons x x)) #(1 2 3 "four" "five" "six")))

(hashset-add! hs1 {(elt vec 2) (elt vec 4)})

(applytest #f hashset-get hs1 '(1 . 1))
(applytest #t hashset-get hs1 '(3 . 3))
(applytest #t hashset-get hs1 (elt vec 2))
(applytest #f hashset-get hs1 (elt vec 0))
(applytest #t eq? (elt vec 2) (hashset/intern hs1 '(3 . 3)))
(evaltest #t (fail? (hashset/probe hs1 '(2 . 2))))
(evaltest #f (fail? (hashset/probe hs1 '(3 . 3))))
(evaltest #t (fail? (hashset/probe hs1 '("six" . "six"))))
(evaltest #t (exists? (hashset/probe hs1 '("five" . "five"))))
(applytest #t eq? (elt vec 4) (hashset/probe hs1 '("five" . "five")))
