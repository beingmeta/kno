;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc.  All rights reserved.

(in-module 'mimetable)

(use-module 'texttools)

(module-export! '{*mimetable* getsuffix path->ctype})

(define *mimetable*
  (let ((table (make-hashtable)))
    (do-choices (map '{("text/cache-manifest" "manifest")
		       ("text/html" "html" "htm")
		       ("application/xhtml+xml" "xhtml")
		       ("text/plain" "text" "txt")
		       ("image/jpeg" "jpeg" "jpg")
		       ("image/png" "png")
		       ("image/gif" "gif")
		       ("text/css" "css")
		       ("text/javascript" "js" "javascript")
		       ("application/x-dtbncx+xml" "ncx")})
      (store! table (string-append "." (elts (cdr map))) (car map))
      (store! table (car map) (elts (cdr map))))
    table))

(define (path->ctype path)
  (if (string? path)
      (get *mimetable* (gather #("." (isalnum+) (eos)) path))
      (if (and (pair? path) (string? (cdr path)))
	  (get *mimetable* (gather #("." (isalnum+) (eos)) (cdr path)))
	  (fail))))

(define (getsuffix path) (gather #("." (isalnum+) (eos)) path))

