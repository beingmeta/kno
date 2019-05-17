;;; -*- Mode: Scheme; -*-

(in-module 'fdweb)

(define wt-module (get-module 'webtools))
(define export-symbols
  (filter-choices (key (getkeys wt-module))
    (applicable? (get wt-module key))))

(define (import-symbol symbol into)
  (store! into symbol (get wt-module symbol)))

(import-symbol export-symbols (%env))

(module-export! export-symbols)

