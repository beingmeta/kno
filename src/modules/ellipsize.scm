;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'ellipsize)

(use-module 'texttools)

(module-export! 'ellipsize)

(define (ellipsize string (min 40) (max 60) (break '(bow))
		   (ellipsis "…"))
  (glom
    (if (< (length string) max) string
	(let ((breakat (textsearch break string min)))
	  (if (and breakat (< breakat max))
	      (subseq string 0 breakat)
	      (subseq string 0 (inexact->exact (floor (+ min (/~ (- max min) 2))))))))
    ellipsis))


