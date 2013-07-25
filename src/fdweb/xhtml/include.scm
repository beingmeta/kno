;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'xhtml/include)

(use-module '{texttools getcontent xhtml/pagedate})
(use-module '{fdweb xhtml})

(module-export! '{xhtml/include firebuglite})

(define (xhtml/include file (base #f) (enc #t))
  (let ((path (if base (get-component file base) file))
	(content (if (has-suffix path ".fdxml")
		     (getcontent path enc fdxml/parse)
		     (if (has-suffix path {".md" ".markdown"})
			 (getcontent path enc md->html)
			 (getcontent path enc))))
	(mod (file-modtime path)))
    (pagedate! mod)
    (if (string? content) (xhtml content) (xmleval content))))

(define (firebuglite)
  (xhtml "<script type='text/javascript' src='http://getfirebug.com/releases/lite/1.2/firebug-lite-compressed.js'></script>"))
