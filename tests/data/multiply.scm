;;; -*- Mode: Scheme; -*-

(define (main . args)
  (let ((product 1))
    (dolist (arg args) (set! product (* product arg)))
    (printout product))
  (length args))
