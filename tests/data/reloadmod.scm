;;; -*- Mode: Scheme; -*-

(in-module 'reloadmod)

(module-export! 'get-load-count)

(define-init load-count 0)

(set! load-count (1+ load-count))

(define (get-load-count) load-count)

(message "Reloaded reloadmod, count=" load-count)


