;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc. All rights reserved.

(in-module 'savecontent)

;;; This handles automatic updating of the content of files
;;; It is a more flexible version of load-latest

(define havezip #f)

(cond ((get-module 'ziptools)
       (use-module 'ziptools)
       (set! havezip #t))
      (else 
       (define (zipfile? x) #f)
       (define (zip/add! . args) #f)))

(use-module '{fileio aws/s3 varconfig logger fdweb reflection
	      texttools mimetable})

(define-init %loglevel %info%)

(module-export! '{savecontent saveoutput save/path save/fetch})

(define (guess-ctype name)
  (set! name (uribase name))
  (cond ((or (has-suffix name ".html")
	     (has-suffix name ".htm")
	     (has-suffix name ".xhtml"))
	 "text/html; charset=utf8")
	((has-suffix name ".manifest") "text/cache-manifest")
	((has-suffix name ".css")
	 "text/css; charset=utf8")
	((has-suffix name ".js")
	 "text/javascript; charset=utf8")
	((has-suffix name ".mobi") "application/x-mobipocket-ebook")
	((has-suffix name ".epub") "application/epub+zip")
	((or (has-suffix name ".gif")  (has-suffix name ".GIF")) "image/gif")
	((or (has-suffix name ".png")  (has-suffix name ".PNG")) "image/png")
	((or (has-suffix name ".jpg")  (has-suffix name ".JPG")
	     (has-suffix name ".jpeg")  (has-suffix name ".JPEG"))
	 "image/jpeg")
	(else #f)))

(define (checkdir dir)
  (or (file-directory? dir)
      (begin (checkdir (dirname dir))
	     (loginfo "Creating directory " dir)
	     (system "mkdir " dir))))
(define (checkpath path)
  (checkdir (dirname path))
  path)

(define (get-charset ctype)
  (try
   (get (text->frames #("charset=" (label encoding (not> ";"))) ctype)
	'charset)
   #f))

(define (savecontent saveto name content (ctype) (charset))
  (default! ctype (try (guess-ctype name)
		       (if (packet? content) "application" "text")))
  (default! charset (get-charset ctype))
  (lognotice "Saving " (if ctype (printout (write ctype) " "))
	     "content for " (write name) " into " saveto)
  (when (and charset
	     (string? content)
	     (not (overlaps? (downcase charset) {"utf8" "utf-8"})))
    (set! content (packet->string (string->packet content) charset)))
  (cond ((string? saveto)
	 (write-file (checkpath (mkpath saveto name)) content))
	((s3loc? saveto)
	 (s3/write! (s3/mkpath saveto name) content ctype))
	((and (pair? saveto) (string? (car saveto)) (string? (cdr saveto)))
	 (write-file (mkpath (mkpath (car saveto) (cdr saveto)) name)
		     content))
	((and (pair? saveto)
	      (s3loc? (car saveto))
	      (string? (cdr saveto)))
	 (s3/write! (s3/mkpath (car saveto) (mkpath (cdr saveto) name))
		    content ctype))
	((and havezip (zipfile? saveto))
	 (zip/add! saveto name content))
	((and havezip (pair? saveto)
	      (zipfile? (car saveto))
	      (string? (cdr saveto)))
	 (zip/add! (car saveto) (mkpath (cdr saveto) name) content))
	(else (error "Bad SAVECONTENT call"))))

(define saveoutput
  (macro expr
    `(,savecontent
      ,(second expr) ,(third expr)
      (,stringout ,@(cdr (cdr (cdr expr))))
      (,guess-ctype ,(third expr)))))

(define (savepath root path)
  (cond ((s3loc? root) (s3/mkpath root path))
	((zipfile? root) (cons root path))
	((and (pair? root) (zipfile? (car root)) (string? (cdr root)))
	 (cons (car root) (mkpath (cdr root) path)))
	((and (pair? root) (string? (car root)) (string? (cdr root)))
	 (mkpath (mkpath (car root) (cdr root)) path))
	((string? root) (checkpath (mkpath root path)))
	(else (error "Weird docbase root" root " for " path))))
(define (save/path root path . more)
  (let ((result (savepath root path)))
    (when (string? result) (mkdirs result))
    (if (null? more) result
	(apply save/path result (car more) (cdr more)))))

(define (guess-ctype ref)
  (try (if (string? ref) (path->ctype ref)
	   (if (string? (cdr ref)) (path->ctype (cdr ref))
	       "text"))
       "text"))

(define (save/fetch ref (ctype))
  (default! ctype (guess-ctype ref))
  (cond ((s3loc? ref) (s3/get ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (zip/get (car ref)
		  (if (has-prefix (cdr ref) "/")
		      (subseq (cdr ref) 1)
		      (cdr ref))
		  (not (has-prefix ctype "text"))))
	((pair? ref) (save/fetch (save/path (car ref) (cdr ref))))
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (let ((response (urlget ref)))
	   (if (and (test response 'response)
		    (<= 200 (get response 'response) 299)
		    (get response '%content))
	       (get response '%content)
	       (fail))))
	((and (string? ref) (has-prefix ref "s3:")) (s3/get (->s3loc ref)))
	((string? ref) (filestring ref))
	(else (error "Weird docbase ref" ref))))



