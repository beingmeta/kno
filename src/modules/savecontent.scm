;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc. All rights reserved.

(in-module 'savecontent)

;;; This handles automatic updating of the content of files
;;; It is a more flexible version of load-latest
(define version "$Id: getcontent.scm 4048 2009-06-20 15:18:16Z bemeta $")
(define revision "$Revision: 4048 $")

(define havezip #f)

(cond ((get-module 'ziptools)
       (use-module 'ziptools)
       (set! havezip #t))
      (else 
       (define (zipfile? x) #f)
       (define (zip/add! . args) #f)))

(use-module '{fileio aws/s3 varconfig logger reflection})

(define %loglevel %info!)

(module-export! '{savecontent saveoutput save/path})

(define (guess-ctype name)
  (cond ((or (has-suffix name ".html")  (has-suffix name ".htm") (has-suffix name ".xhtml"))
	 "text/html")
	((has-suffix name ".manifest") "text/cache-manifest")
	((has-suffix name ".css") "text/css")
	((has-suffix name ".js") "text/javascript")
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

(define (savecontent saveto name content (ctype))
  (default! ctype (guess-ctype name))
  (lognotice "Saving " ctype " content for " (write name) " to " saveto)
  (cond ((string? saveto)
	 (write-file (checkpath (mkpath saveto name)) content))
	((s3loc? saveto)
	 (s3/write! (s3/mkpath saveto name) content))
	((and (pair? saveto)
	      (s3loc? (car saveto))
	      (string? (cdr saveto)))
	 (s3/write! (s3/mkpath (car saveto) (mkpath (cdr saveto) name))
		    content))
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
      (,stringout ,@(cdr (cdr (cdr expr)))))))

(define (save/path root path)
  (cond ((s3loc? root) (s3/mkpath root path))
	((zipfile? root) (cons zipfile path))
	((and (pair? root) (zipfile? (car root)) (string? (cdr root)))
	 (cons (car root) (mkpath (cdr root) path)))
	((string? root) (mkpath root path))
	(else (error "Weird docbase root" root " for " path))))
