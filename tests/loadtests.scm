#!./kno

(define (main (repeat (config 'repeat 100)) . args)
  (unless (number? repeat)
    (set! args (cons repeat args))
    (set! repeat (config 'repeat 100)))
  (dotimes (i repeat)
    (dolist (arg args) (load arg))))


