;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc. All rights reserved

(in-module 'xhtml/include)

(use-module '{getcontent xhtml/pagedate})
(use-module '{fdweb xhtml})
(define have-sundown #f)
(when (get-module 'sundown)
  (use-module 'sundown)
  (set! have-sundown #t))

(module-export! '{xhtml/include firebuglite})

(define (wrap/pre string)
  (glom "\n<pre>\n" string "\n</pre>\n"))

(define (xhtml/include file (base #f) (enc #t))
  (let* ((path (if base (get-component file base) file))
	 (content (if (has-suffix path ".fdxml")
		      (getcontent path enc fdxml/parse)
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

(define fdxml
  (macro expr
    `(,xmleval ',(fdxml/parse (glom ,@(cdr expr))))))

(module-export! 'fdxml)
