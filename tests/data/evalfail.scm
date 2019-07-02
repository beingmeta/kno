;;; -*- Mode: Scheme; -*-

(define (postload) (message "We're loaded"))

(define xy #[x 3 y 4])

(define (return-slotmap x y)
  (irritant #[x x y y] |JustJoking|))

(return-slotmap)
