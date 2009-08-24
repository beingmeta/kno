;;; -*- Mode: Scheme; Text-encoding: latin-1; -*-

(in-module 'xhtml/include)

(use-module '{texttools getcontent})
(use-module '{fdweb xhtml})

(module-export! '{xhtml/include firebuglite})

(define (xhtml/include file (base #f) (enc #t))
  (xhtml (getcontent (if base (get-component file base) file) enc)))

(define (firebuglite)
  (xhtml "<script type='text/javascript' src='http://getfirebug.com/releases/lite/1.2/firebug-lite-compressed.js'></script>"))
