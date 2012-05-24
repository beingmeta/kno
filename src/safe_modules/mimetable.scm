;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc.  All rights reserved.

(in-module 'mimetable)

(use-module '{texttools varconfig})

(module-export! '{*mimetable* getsuffix path->ctype path->mimetype})

(define *default-charset* "utf-8")
(varconfig! mime:charset *default-charset* config:goodstring)

(define *default-mimetype* #f)
(varconfig! mime:default *default-mimetype* config:goodstring)

(define *mimetable*
  (let ((table (make-hashtable)))
    (do-choices (map '{("text/cache-manifest" "manifest")
		       ("text/html" "html" "htm")
		       ("application/xhtml+xml; charset=utf8" "xhtml")
		       ("application/zip" "zip" "ZIP")
		       ("text/plain" "text" "txt")
		       ("image/jpeg" "jpeg" "jpg")
		       ("image/png" "png")
		       ("image/gif" "gif")
		       ("text/css" "css")
		       ("text/javascript" "js" "javascript")
		       ("application/x-dtbncx+xml" "ncx")})
      (store! table (choice (elts (cdr map))
			    (string-append "." (elts (cdr map))))
	      (if (has-prefix (car map) "text/")
		  (if *default-charset*
		      (string-append (car map) "; charset=" *default-charset*)
		      (car map))
		  (car map))))
    table))

(define (guess-ctype path)
  (if (string? path)
      (get *mimetable* (gather #("." (isalnum+) (eos)) path))
      (if (and (pair? path) (string? (cdr path)))
	  (get *mimetable* (gather #("." (isalnum+) (eos)) (cdr path)))
	  (fail))))

(define (path->mimetype path (default-value))
  (let ((ctype (guess-ctype path)))
    (or (and (exists? ctype) ctype (has-prefix ctype "text")
	     (and *default-charset* (not (search "charset" ctype))
		  (string-append ctype "; charset=" *default-charset*)))
	(and (exists? ctype) ctype)
	(if (bound? default-value) default-value (fail)))))
(define path->ctype path->mimetype)

(define (getsuffix path) (gather #("." (isalnum+) (eos)) path))

