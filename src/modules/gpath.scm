;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc. All rights reserved.

(in-module 'gpath)
(module-export!
 '{gp/write! gp/save!
   writeout writeout/type
   gp/writeout gp/writeout! gp/writeout+!
   gp/fetch gp/fetch+ gp/modified gp/exists?
   gp/urlfetch gp/urlinfo
   gp/path gp/mkpath gp/makepath gpath->string
   gp/location gp/basename})

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

(define gp/urlsubst {})
(varconfig! gp:urlsubst gp/urlsubst #f)

(define (guess-mimetype name (content))
  (or (path->mimetype (gp/basename name) #f)
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
  (and ctype (try
	      (get (text->frames #("charset=" (label encoding (not> ";"))) ctype)
		   'charset)
	      #f)))

(define *default-dirmode* 0x775) ;; rwxrwxr_x
(varconfig! gpath:dirmode *default-dirmode*)

;;; Writing to a gpath

(defambda (gp/write! saveto name content (ctype #f) (charset #f))
  (do-choices name
    (let ((ctype (or ctype (guess-mimetype (get-namestring name) content)))
	  (charset (or charset (get-charset ctype))))
      ;; Do any charset conversion required by the CTYPE
      (when (and charset
		 (string? content)
		 (not (overlaps? (downcase charset) {"utf8" "utf-8"})))
	(set! content (packet->string (string->packet content) charset)))
      (gp/save! (gp/mkpath saveto name) content ctype charset))))

(define (get-namestring gpath)
  (if (string? gpath) gpath
      (if (s3loc? gpath) (s3loc-path gpath)
	  (if (pair? gpath) (cdr gpath)
	      ""))))

(defambda (gp/save! dest content (ctype #f) (charset #f))
  (do-choices dest
    (let ((ctype (or ctype (guess-mimetype (get-namestring dest) content)))
	  (charset (or charset (get-charset ctype))))
      (lognotice "Saving " (if ctype (printout (write ctype) " "))
		 "content into " dest)
      ;; Do any charset conversion required by the CTYPE
      (when (and charset
		 (string? content)
		 (not (overlaps? (downcase charset) {"utf8" "utf-8"})))
	(set! content (packet->string (string->packet content) charset)))
      (cond ((and (string? dest) (string-starts-with? dest {"http:" "https:"}))
	     (let ((req (urlput dest content ctype `#[METHOD "PUT"])))
	       (unless (response/ok? req)
		 (if (response/badmethod? req)
		     (let ((req (urlput dest content ctype `#[METHOD "POST"])))
		       (unless (response/ok? req)
			 (error "Couldn't save to URL" dest req)))
		     (error "Couldn't save to URL" dest req)))))
	    ((string? dest) (write-file dest content))
	((s3loc? dest) (s3/write! dest content ctype))
	((and (pair? dest) (string? (car dest)) (string? (cdr dest)))
	 (write-file (mkpath (car dest) (cdr dest)) content))
	((and (pair? saveto)
	      (s3loc? (car saveto))
	      (string? (cdr saveto)))
	 (s3/write! (s3/mkpath (car saveto) (cdr saveto)) content ctype))
	((and havezip (pair? saveto)
	      (zipfile? (car saveto))
	      (string? (cdr saveto)))
	 (zip/add! (car saveto) (cdr saveto) content))
	(else (error "Bad GP/SAVE call")))
      (lognotice "Saved " (if ctype (printout (write ctype) " "))
		 "content into " dest))))

(define writeout
  (macro expr
    `(,gp/save!
      ,(second expr)
      (,stringout ,@(cdr (cdr expr)))
      (,guess-mimetype ,(second expr)))))
(define gp/writeout writeout)

(define writeout/type
  (macro expr
    `(,gp/save!
	 ,(second expr)
	 (,stringout ,@(cdr (cdr (cdr expr))))
       ,(third expr))))

;; For generating text files, printout style, this saves the standard
;; output to the designated gpath
(define gp/writeout!
  (macro expr
    `(,gp/write!
      ,(second expr) ,(third expr)
      (,stringout ,@(cdr (cdr (cdr expr))))
      (,guess-mimetype ,(third expr)))))

;; This writes out with an explicit mimetype
(define gp/writeout+!
  (macro expr
    `(,gp/write!
      ,(second expr) ,(third expr)
      (,stringout ,@(cdr (cdr (cdr (cdr expr)))))
      ,(fourth expr))))

(define (string->root string)
  (cond ((has-prefix string "s3:") (->s3loc string))
	((has-prefix string "zip:")
	 (zip/open (subseq string 4)))
	(else string)))

(define (gp/basename path)
  (when (string? path) (set! path (string->root path)))
  (cond ((and (pair? path) (null? (cdr path)))
	 (gp/basename (car path)))
	((pair? path) (gp/basename (cdr path)))
	((s3loc? path) (basename (s3loc-path path)))
	((string? path) (basename path))
	(else "")))

(define (gp/location path)
  (when (string? path) (set! path (string->root path)))
  (cond ((and (pair? path)
	      (or (null? (cdr path)) (empty-string? (cdr path))))
	 (gp/location (car path)))
	((and (pair? path) (string? (cdr path))
	      (position #\/ (cdr path)))
	 (gp/mkpath (car path) (dirname (cdr path))))
	((pair? path) (car path))
	((and (s3loc? path) (string? (s3loc-path path))
	      (position #\/ (s3loc-path path)))
	 (cons-s3loc (s3loc-bucket path) (dirname (s3loc-path path))))
	((and (s3loc? path) (string? (s3loc-path path)))
	 (cons-s3loc (s3loc-bucket path) ""))
	((and (string? path) (has-prefix path "/")) (dirname path))
	((and (string? path) (position #\/ path))
	 (mkpath (getcwd) (dirname path)))
	((string? path) (mkpath (getcwd) path))
	(else path)))

(define (gpath->string path)
  (cond ((string? path) path)
	((and (pair? path)
	      (or (null? (cdr path)) (empty-string? (cdr path))))
	 (car path))
	((and (pair? path) (string? (car path))
	      (string? (cdr path))
	      (position #\/ (cdr path)))
	 (mkpath (car path) (cdr path)))
	((and (pair? path) (s3loc? (car path)))
	 (s3loc->string (s3/mkpath (car path) (cdr path))))
	((and (pair? path) (zipfile? (car path)))
	 (stringout "zip:" (zip/filename (car path)) "(" (cdr path) ")"))
	(else (stringout path))))

(define (makepath root path (mode *default-dirmode*))
  (when (and (pair? root) (null? (cdr root)))
    (set! root (cons (car root) "")))
  (if (string? root) (set! root (string->root root))
      (if (and (pair? root) (string? (car root)))
	  (set! root (cons (string->root (car root)) (cdr root)))))
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
  (default! ctype (guess-mimetype (get-namestring ref)))
  (cond ((s3loc? ref) (s3/get ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (zip/get (car ref)
		  (if (has-prefix (cdr ref) "/")
		      (subseq (cdr ref) 1)
		      (cdr ref))
		  (not (has-prefix ctype "text"))))
	((pair? ref) (gp/fetch (gp/path (car ref) (cdr ref))))
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (gp/urlfetch ref))
	((and (string? ref) (has-prefix ref "s3:")) (s3/get (->s3loc ref)))
	((string? ref)
	 (if (or (has-prefix ctype "text") (search "xml" ctype))
	     (filestring ref)
	     (filedata ref)))
	(else (error "Weird docbase ref" ref))))

(define (gp/urlfetch url (err #t) (max-redirects 10))
  (let* ((newurl (textsubst url (qc gp/urlsubst)))
	 (err (and err
		   (if (equal? url newurl)
		       (cons url (if (pair? err) err '()))
		       (cons* newurl (list url) (if (pair? err) err '())))))
	 (response (urlget newurl)))
    (if (and (test response 'response)
	     (<= 200 (get response 'response) 299)
	     (get response '%content))
	(get response '%content)
	(if (<= 300 (get response 'response) 399)
	    (if (and (number? max-redirects) (> max-redirects 0))
		(gp/urlfetch (get response 'location)
			     (and err (cons url (if (pair? err) err '())))
			     (-1+ max-redirects))
		(if err (error TOO_MANY_REDIRECTS url response)
		    (begin (logwarn "Too many redirects: "
				    (cons url err)))))
	    (tryif err (error URLFETCH_FAILED url response))))))

(define (gp/urlinfo url (err #t) (max-redirects 10))
  (let* ((newurl (textsubst url (qc gp/urlsubst)))
	 (err (and err
		   (if (equal? url newurl)
		       (cons url (if (pair? err) err '()))
		       (cons* newurl (list url) (if (pair? err) err '())))))
	 (response (urlhead newurl)))
    (if (and (test response 'response)
	     (<= 200 (get response 'response) 299))
	response
	(if (<= 300 (get response 'response) 399)
	    (if (and (number? max-redirects) (> max-redirects 0))
		(gp/urlinfo (get response 'location)
			    (and err (cons url (if (pair? err) err '())))
			    (-1+ max-redirects))
		(if err (error TOO_MANY_REDIRECTS url response)
		    (begin (logwarn "Too many redirects: "
				    (cons url err)))))
	    response))))

(define (gp/fetch+ ref (ctype))
  (default! ctype (guess-mimetype (get-namestring ref)))
  (cond ((s3loc? ref) (s3/get+ ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 `#[content ,(zip/get (car ref)
			      (if (has-prefix (cdr ref) "/")
				  (subseq (cdr ref) 1)
				  (cdr ref))
			      (not (has-prefix ctype "text")))
	    ctype ,ctype])
	((pair? ref) (gp/fetch+ (gp/path (car ref) (cdr ref))))
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (let ((response (urlget ref)))
	   (if (and (test response 'response)
		    (<= 200 (get response 'response) 299)
		    (exists? (get response '%content)))
	       `#[content ,(get response '%content)
		  ctype ,(try (get response 'content-type) ctype)
		  modified (try (get response 'last-modified)
				(timestamp))]
	       #f)))
	((and (string? ref) (has-prefix ref "s3:"))
	 (s3/get+ (->s3loc ref)))
	((string? ref)
	 (if (or (has-prefix ctype "text") (search "xml" ctype))
	     `#[content ,(filestring ref) ctype ,ctype
		modified ,(file-modtime ref)]
	     `#[content ,(filedata ref) ctype ,ctype
		modified ,(file-modtime ref)]))
	(else (error "Weird docbase ref" ref))))

(define (gp/modified ref)
  (cond ((s3loc? ref) (s3/modified ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 #f)
	((pair? ref) (gp/modified (gp/path (car ref) (cdr ref))))
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (let ((response (urlget ref)))
	   (if (and (test response 'response)
		    (<= 200 (get response 'response) 299)
		    (exists? (get response '%content)))
	       (try (get response 'last-modified) #f)
	       #f)))
	((and (string? ref) (has-prefix ref "s3:"))
	 (s3/modified (->s3loc ref)))
	((string? ref) (file-modtime ref))
	(else (error "Weird docbase ref" ref))))

(define (gp/exists? ref)
  (cond ((s3loc? ref) (s3loc/exists? ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (zip/exists? (car ref) (cdr ref)))
	((pair? ref) (gp/exists? (gp/path (car ref) (cdr ref))))
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (let ((response (urlget ref)))
	   (and (test response 'response)
		(<= 200 (get response 'response) 299)
		(exists? (get response '%content)))))
	((and (string? ref) (has-prefix ref "s3:"))
	 (s3/exists? (->s3loc ref)))
	((string? ref) (file-exists? ref))
	(else (error "Weird docbase ref" ref))))
