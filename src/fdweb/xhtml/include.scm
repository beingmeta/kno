;;; -*- Mode: Scheme; Text-encoding: latin-1; -*-

(in-module 'xhtml/include)

(use-module '{texttools getcontent})
(use-module '{fdweb xhtml})

(module-export! '{xhtml/include firebuglite})

(define (xhtml/include file (base #f))
  (xhtml (getcontent (get-component file base) #f #t)))

(define (firebuglite)
  (xhtml "<script type='text/javascript' src='http://getfirebug.com/releases/lite/1.2/firebug-lite-compressed.js'></script>"))
