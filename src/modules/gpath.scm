;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc. All rights reserved.

(in-module 'gpath)
(module-export!
 '{gp/write gp/writeout gp/writeout+ gp/fetch
   gp/path gp/mkpath gp/makepath})

;;; This is a generic path facility (it grew out of the savecontent
;;; module, which still exists for legacy and historical reasons).  A
;;; gpath is just a CONS (for now) of a root and a relative path.
;;; Currently the root can be a directory name, an S3 location, or a
;;; zip file.

(define havezip #f)

(cond ((get-module 'ziptools)
       (use-module 'ziptools)
       (set! havezip #t))
      (else 
       (define (zipfile? x) #f)
       (define (zip/add! . args) #f)))

(use-module '{fileio aws/s3 varconfig logger fdweb reflection
	      texttools mimetable})
(define %loglevel %info!)

(define (guess-mimetype name (content))
  (or (path->mimetype (uribase name) #f)
      (if (bound? content)
	  (if (string? content) "text"
	      (if (packet? content) "application"
		  "application/dtype"))
	  (config 'mime:default))))

(define (checkdir dirpath)
  (unless (string-starts-with? dirpath #((isalpha+) ":"))
    (mkdirs dirpath))
  (when (has-prefix dirpath "file:") (mkdirs (subseq dirpath 5)))
  dirpath)

(define (get-charset ctype)
  (try
   (get (text->frames #("charset=" (label encoding (not> ";"))) ctype)
	'charset)
   #f))

(define *default-dirmode* 0x775) ;; rwxrwxr_x
(varconfig! gpath:dirmode *default-dirmode*)

;;; Writing to a gpath

(define (gp/write saveto name content (ctype) (charset))
  (default! ctype (guess-mimetype name content))
  (default! charset (get-charset ctype))
  (lognotice "Saving " (if ctype (printout ctype " "))
	     "content for " (write name) " into " saveto)
  ;; Do any charset conversion required by the CTYPE
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
	(else (error "Bad GP/WRITE call"))))

;; For generating text files, printout style, this saves the standard
;; output to the designated gpath
(define gp/writeout
  (macro expr
    `(,gp/write
      ,(second expr) ,(third expr)
      (,stringout ,@(cdr (cdr (cdr expr))))
      (,guess-mimetype ,(third expr)))))

;; This writes out with an explicit mimetype
(define gp/writeout+
  (macro expr
    `(,gp/write
      ,(second expr) ,(third expr)
      (,stringout ,@(cdr (cdr (cdr (cdr expr)))))
      ,(fourth expr))))

(define (makepath root path (mode *default-dirmode*))
  (when (and (pair? root) (null? (cdr root)))
    (set! root (cons (car root) "")))
  (cond ((s3loc? root) (s3/mkpath root path))
	((zipfile? root) (cons root path))
	((and (pair? root) (not (string? (cdr root))))
	 (error "Bad GPATH root" root))
	((string? root) (checkdir (mkpath root path)))
	((and (pair? root) (string? (car root)))
	 (checkdir (mkpath (mkpath (car root) (cdr root)) path)))
	((pair? root) (cons (car root) (mkpath (cdr root) path)))
	((string? root) (cons (checkdir root) path))
	(else (error "Weird docbase root" root " for " path))))
(define gp/makepath makepath)
(define (gp/path root path . more)
  (let ((result (makepath root path)))
    (if (null? more) result
	(apply gp/path result (car more) (cdr more)))))
(define gp/mkpath gp/path)

(define (gp/fetch ref (ctype))
  (default! ctype (guess-mimetype ref))
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
	 (let ((response (urlget absref)))
	   (if (and (test response 'response)
		    (<= 200 (get response 'response) 299)
		    (get response '%content))
	       (get response '%content)
	       (fail))))
	((string? ref) (filestring ref))
	(else (error "Weird docbase ref" ref))))
