;;; -*- Mode: Scheme; -*-

(module-export! 'get-loadok-count)

(define-init load-ok-count 0)

(set! load-ok-count (1+ load-ok-count))

(define (get-load-ok-count) load-ok-count)

(message "Loaded loadok, count=" load-ok-count)

