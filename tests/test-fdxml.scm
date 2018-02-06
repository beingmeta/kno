(use-module 'xhtml/brico)

(define (sum x y) (+ x y))
(define (times x y) (* x y))

(module-export! '{sum times})
