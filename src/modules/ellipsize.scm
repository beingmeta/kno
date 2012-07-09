;;; -*- Mode: Scheme; -*-

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
	      (subseq 0 (floor (+ min (/~ (- max min) 2)))))))
    ellipsis))


