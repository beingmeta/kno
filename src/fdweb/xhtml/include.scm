;;; -*- Mode: Scheme; Text-encoding: latin-1; -*-

(in-module 'xhtml/include)

(use-module '{texttools getcontent})
(use-module '{fdweb xhtml})

(module-export! 'xhtml/include)

(define (xhtml/include file (base #f))
  (xhtml (getcontent (get-component file base) #f #t)))

