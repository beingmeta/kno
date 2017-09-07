;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/filenames)

(use-module '{ezrecords stringfmts logger texttools})

(module-export! '{flex/file})

(define (try-file . args)
  (let ((file (apply glom args)))
    (tryif (file-exists? file) file)))

(define (flex/file prefix (suffix "pool") (simple #t))
  (set! prefix
    (textsubst prefix 
	       `#("." (opt #((isxdigit+) ".")) ,suffix)
	       ""))
  (abspath
   (try (try-file prefix ".0000." suffix)
	(try-file prefix ".000." suffix)
	(try-file prefix ".00." suffix)
	(try-file prefix ".0." suffix)
	(tryif simple (try-file prefix "." suffix)))))

