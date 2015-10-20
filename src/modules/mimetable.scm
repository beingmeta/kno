;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc.  All rights reserved.

(in-module 'mimetable)

(use-module '{texttools varconfig texttools})
(define %used_modules 'varconfig)

(module-export!
 '{*mimetable* getsuffix
   ctype->suffix ctype->charset
   path->ctype path->mimetype
   path->encoding})

(define *default-charset* #f)
(varconfig! mime:charset *default-charset* config:goodstring)

(define *default-mimetype* #f)
(varconfig! mime:default *default-mimetype* config:goodstring)

(define-init *inv-mimetable* (make-hashtable))

(define *mimetable*
  (let ((table (make-hashtable)))
    (do-choices (map '{("text/cache-manifest" "manifest")
		       ("text/html" "html" "htm")
		       ("application/xhtml+xml" "xhtml")
		       ;;("text/html" "xhtml")
		       ("application/zip" "zip" "ZIP")
		       ("text/plain" "text" "txt")
		       ("image/jpeg" "jpeg" "jpg")
		       ("image/png" "png")
		       ("image/gif" "gif")
		       ("text/css" "css")
		       ("image/svg+xml" "svg" "svgz")
		       ("audio/aac" "aac")
		       ("audio/mp4" "mp4" "m4a")
		       ("audio/mpeg" "mp1" "mp3" "mpg" "mpeg")
		       ("audio/ogg" "ogg" "oga")
		       ("audio/wav" "wav")
		       ("audio/webm" "webm")
		       ("audio/mp4" "mp4" "m4v")
		       ("audio/webm" "webm")
		       ("audio/mpeg" "mp1" "mp2" "mp3" "mpg" "mpeg")		       
		       ("text/javascript" "js" "javascript")
		       ("font/opentype" "otf")
		       ("application/font-woff" "woff")
		       ("font/ttf" "ttf")
		       ("application/adobe-page-template+xml" "xpgt")
		       ("application/x-dtbncx+xml" "ncx")})
      (store! *inv-mimetable* (car map)
	      (pick-one (downcase (smallest (elts (cdr map)) length))))
      (store! table (choice (elts (cdr map))
			    (string-append "." (elts (cdr map))))
	      (car map)))
    table))

(define (guess-ctype path)
  (if (string? path)
      (if (has-suffix path ".gz")
	  (get *mimetable* (gather #("." (isalnum+) (eos)) (slice path 0 -3)))
	  (if (has-suffix path ".Z")
	      (get *mimetable* (gather #("." (isalnum+) (eos)) (slice path 0 -2)))
	      (get *mimetable* (gather #("." (isalnum+) (eos)) path))))
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

(define (path->encoding path)
  (if (has-suffix path ".gz") "gzip"
      (if (has-suffix path ".Z") "compress"
	  #f)))

(define (getsuffix path (default {}))
  (try (gather #("." (isalnum+) (eos)) path) default))

(define (ctype->suffix ctype) (get *inv-mimetable* ctype))

(define (ctype->charset string)
  (try (get (text->frames #("charset" (spaces*) "="
			    (spaces*) (label charset (not> {";" (eos)})))
			  string)
	    'charset)
       #f))

