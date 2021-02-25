;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved
Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'xhtml/include)

(use-module '{io/getcontent xhtml/pagedate})
(use-module '{webtools xhtml})
(define have-sundown #f)
(when (get-module 'sundown)
  (use-module 'sundown)
  (set! have-sundown #t))

(module-export! '{xhtml/include firebuglite})

(define (wrap/pre string)
  (glom "\n<pre>\n" string "\n</pre>\n"))

(define (xhtml/include file (base #f) (enc #t))
  (let* ((path (if base (get-component file base) file))
	 (content (if (has-suffix path {".nml" ".knoml" ".knoxml"})
		      (getcontent path enc knoml/parse)
		      (if (has-suffix path {".md" ".markdown"})
			  (if have-sundown
			      (getcontent path enc md->html)
			      (getcontent path enc wrap/pre))
			  (getcontent path enc))))
	 (mod (file-modtime path)))
    (pagedate! mod)
    (if (string? content) (xhtml content) (xmleval content))))

(define (firebuglite)
  (xhtml "<script type='text/javascript' src='http://getfirebug.com/releases/lite/1.2/firebug-lite-compressed.js'></script>"))

(define knoml
  (macro expr
    `(,xmleval ',(knoml/parse (glom ,@(cdr expr))))))

(module-export! 'knoml)
