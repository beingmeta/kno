;;; -*- Mode: Scheme -*-

(config! 'quiet #t)

(use-module '{logger varconfig})

(define (main (address #f) (op 'uptime) . args)
  (if (or (not address) (not (or (symbol? op) (string? op))))
      (lineout "Usage: knoping *addr* *op* args...")
      (kno/call address op args)))

(define (kno/call address op args)
  (let* ((opts #f)
	 (service (open-service address opts))
	 (op (if (string? op) (string->symbol (downcase op)) op)))
    (let ((result (apply service/call service op args)))
      (lineout (listdata result)))))

