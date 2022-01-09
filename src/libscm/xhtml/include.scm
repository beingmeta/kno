;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'xhtml/include)

(use-module '{io/getcontent xhtml/pagedate})
(use-module '{webtools texttools xhtml})
(define have-sundown #f)
(when (get-module 'sundown)
  (use-module 'sundown)
  (set! have-sundown #t))

(module-export! '{xhtml/include firebuglite})

(define suffix-pat #("." (isalnum+) (eos)))

(define-init suffix-handlers (make-hashtable))
(store! suffix-handlers {".nml" ".knoml" ".knoxml"} knoml/parse)
(config-def! 'xhtml:include:suffixfns
  (lambda (var (val))
    (if (unbound? val) suffix-handlers
	(if (pair? val)
	    (store! suffix-handlers (car val) (cdr val))
	    (irritant val |BadSuffix.Handler|)))))

(define (wrap/pre string)
  (glom "\n<pre>\n" string "\n</pre>\n"))

(define (xhtml/include file (base #f) (opts #f))
  (local enc (getopt opts 'character-encoding (getopt opts 'encoding #t)))
  (local transformer (getopt opts 'transformer))
  (let* ((path (if base (get-component file base) file))
	 (transformer (or transformer
			  (try (get suffix-handlers (gather suffix-pat path))
			       #f)))
	 (content (if transformer
		      (getcontent path enc transformer)
		      (getcontent path enc)))
	 (mod (file-modtime path)))
    (pagedate! mod)
    (if (string? content) (xhtml content) (xmleval content))))

(define (firebuglite)
  (xhtml "<script type='text/javascript' src='http://getfirebug.com/releases/lite/1.2/firebug-lite-compressed.js'></script>"))

(define knoml
  (macro expr
    `(,xmleval ',(knoml/parse (glom ,@(cdr expr))))))

(module-export! 'knoml)
