;;; -*- Mode: Scheme; -*-

(define (postload) (error 'just-because))

(define xy #[x 3 y 4])

(define (return-slotmap (x 3) (y 4))
  #[x x y y])

(return-slotmap)

