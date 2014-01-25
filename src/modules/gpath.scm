;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc. All rights reserved.

(in-module 'gpath)
(define %used_modules '{varconfig ezrecords})

(module-export!
 '{gp/write! gp/save!
   writeout writeout/type
   gp/writeout gp/writeout! gp/writeout+!
   gpath? ->gpath
   gp/location? gp/location gp/basename
   gp/has-suffix gp/has-prefix
   gp/fetch gp/fetch+ gp/etag gp/info
   gp/exists? gp/exists gp/modified gp/newer
   gp/path gp/mkpath gp/makepath gpath->string
   gp:config gpath/handler
   gp/urlfetch gp/urlinfo})
(module-export! '{zip/gopen zip/gclose})

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

(use-module '{fileio aws/s3 varconfig logger fdweb 
	      texttools mimetable ezrecords hashfs})
(define %loglevel %notice%)

(define gp/urlsubst {})
(varconfig! gp:urlsubst gp/urlsubst #f)

(defrecord memfile mimetype content modified (hash #f))

(defrecord gpath-handler name get (save #f))
(define gpath/handler cons-gpath-handler)

(define-init gpath-handlers (make-hashtable))

(config-def! 'GPATH:HANDLERS
	     (lambda (var (val))
	       (if (bound? val)
		   (if (gpath-handler? val)
		       (store! gpath-handlers (gpath-handler-name val)
			       val)
		       (error "NOT a GPATH HANDLER"))
		   (get gpath-handlers (getkeys gpath-handlers)))))

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
	      (get (text->frames #("charset=" (label charset (not> ";"))) ctype)
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
      (loginfo "Saving " (length content)
	       (if (string? content) " characters of "
		   (if (packet? content) " bytes of "))
	       (if (and (exists? ctype) ctype)
		   (printout (write ctype) " "))
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
	    ((and (pair? dest) (hashfs? (car dest)))
	     (hashfs/save! (car dest) (cdr dest) content ctype))
	    ((and (pair? dest) (hashtable? (car dest)))
	     (store! (car dest) (cdr dest)
		     (cons-memfile ctype content (timestamp)
				   (packet->base16 (md5 content)))))
	    ((and (pair? dest) (string? (car dest)) (string? (cdr dest)))
	     (write-file (mkpath (car dest) (cdr dest)) content))
	    ((and (pair? dest)
		  (s3loc? (car dest))
		  (string? (cdr dest)))
	     (s3/write! (s3/mkpath (car dest) (cdr dest)) content ctype))
	    ((and havezip (pair? dest)
		  (zipfile? (car dest))
		  (string? (cdr dest)))
	     (zip/add! (car dest) (cdr dest) content))
	    ((and (pair? dest) (compound-type? (car dest))
		  (test gpath-handlers (compound-tag (car dest)))
		  (gpath-handler-save (get gpath-handlers (compound-tag (car dest)))))
	     ((gpath-handler-save (get gpath-handlers (compound-tag (car dest))))
	      (car dest) (cdr dest) content ctype charset))
	    (else (error "Bad GP/SAVE call")))
      (loginfo "Saved " (length content)
	       (if (string? content) " characters of "
		   (if (packet? content) " bytes of "))
	       (if (and (exists? ctype) ctype)
		   (printout (write ctype) " "))
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
	(else (uribase string))))

(define (gp/basename path)
  (when (string? path) (set! path (string->root path)))
  (cond ((and (pair? path) (null? (cdr path)))
	 (gp/basename (car path)))
	((pair? path) (gp/basename (cdr path)))
	((s3loc? path) (basename (s3loc-path path)))
	((string? path) (basename path))
	(else "")))

(define (gp/location? path)
  (when (string? path) (set! path (string->root path)))
  (cond ((string? path) (has-suffix path "/"))
	((and (pair? path) (string? (cdr path)))
	 (has-suffix (cdr path) "/"))
	((and (s3loc? path) (string? (s3loc-path path)))
	 (has-suffix (s3loc-path path) "/"))
	(else #f)))

(define (gp/location path)
  (when (string? path) (set! path (string->root path)))
  (cond ((and (pair? path)
	      (or (null? (cdr path)) (empty-string? (cdr path))))
	 (gp/location (car path)))
	((and (pair? path) (string? (cdr path))
	      (has-suffix (cdr path) "/"))
	 (gp/mkpath (car path) (slice (cdr path) 0 -1)))
	((and (pair? path) (string? (cdr path))
	      (position #\/ (cdr path)))
	 (gp/mkpath (car path) (dirname (cdr path))))
	((pair? path) (car path))
	((and (s3loc? path) (string? (s3loc-path path))
	      (position #\/ (s3loc-path path)))
	 (make-s3loc (s3loc-bucket path) (dirname (s3loc-path path))))
	((and (s3loc? path) (string? (s3loc-path path)))
	 (make-s3loc (s3loc-bucket path) ""))
	((and (string? path) (has-prefix path "/")) (dirname path))
	((and (string? path) (has-prefix path {"ftp:" "http:" "https:"}))
	 (if (has-suffix path "/") path (dirname path)))
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
	((and (pair? path) (pair? (car path)))
	 (gpath->string (gp/mkpath (car path) (cdr path))))
	((and (pair? path) (hashtable? (car path)) (string? (cdr path)))
	 (stringout "hashtable:" (cdr path)
	   "(0x" (number->string (hashptr (car path)) 16) ")"))
	((and (pair? path) (hashfs? (car path)) (string? (cdr path)))
	 (hashfs/string (car path) (cdr path)))
	((s3loc? path) (s3loc->string path))
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
  (cond ((or (not path) (null? path)) root)
	((not (string? path))
	 (error |TypeError| makepath "Relative path is not a string" path))
	((and (pair? root) (string? (cdr root))
	      (not (position #\/ (cdr root))))
	 (cons (car root) path))
	((s3loc? root) (s3/mkpath root path))
	((zipfile? root) (cons root path))
	((hashtable? root) (cons root path))
	((hashfs? root) (cons root path))
	((and (compound-type? root) (test gpath-handlers (compound-tag root)))
	 (cons root path))
	((and (pair? root) (not (string? (cdr root))))
	 (error "Bad GPATH root" root))
	((string? root) (checkdir (mkpath root path)))
	((and (pair? root) (string? (car root)))
	 (checkdir (mkpath (mkpath (car root) (cdr root)) path)))
	((pair? root) (cons (car root) (mkpath (cdr root) path)))
	((string? root) (cons (checkdir root) path))
	(else (error "Weird GPATH root" root " for " path))))
(define gp/makepath makepath)
(define (gp/path root path . more)
  (let ((result (makepath (->gpath root) path)))
    (if (null? more) result
	(if (not (car more))
	    (if (null? (cdr more)) result
		(apply gp/path result (cdr more)))
	    (apply gp/path result (car more) (cdr more))))))
(define gp/mkpath gp/path)

(define (gp/fetch ref (ctype))
  (default! ctype (guess-mimetype (get-namestring ref)))
  (cond ((s3loc? ref) (s3/get ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (zip/get (car ref)
		  (if (has-prefix (cdr ref) "/")
		      (subseq (cdr ref) 1)
		      (cdr ref))
		  (or (not ctype) (not (has-prefix ctype "text")))))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (memfile-content (get (car ref) (cdr ref))))
	((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	 (hashfs/get (car ref) (cdr ref)))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	  (car ref) (cdr ref)))
	((pair? ref) (gp/fetch (gp/path (car ref) (cdr ref))))
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (get (gp/urlfetch ref) 'content))
	((and (string? ref) (has-prefix ref "s3:")) (s3/get (->s3loc ref)))
	((and (string? ref) (not (file-exists? ref))) #f)
	((string? ref)
	 (if  (and ctype
		   (or (has-prefix ctype "text")
		       (textsearch #{"xml" ".htm" ".txt" ".text" ".md" "charset"}
				   ctype)))
	      (filestring ref (or (and ctype (ctype->charset ctype)) "auto"))
	      (filedata ref)))
	(else (error "Weird GPATH ref" ref))))

(define (gp/urlfetch url (err #t) (max-redirects 10))
  (let* ((newurl (textsubst url (qc gp/urlsubst)))
	 (err (and err
		   (if (equal? url newurl)
		       (cons url (if (pair? err) err '()))
		       (cons* newurl (list url) (if (pair? err) err '())))))
	 (response (urlget newurl)))
    (if (and (test response 'response)
	     (<= 200 (get response 'response) 299)
	     (test response '%content))
	`#[content ,(get response '%content)
	   ctype ,(get response 'content-type)
	   encoding ,(get response 'content-encoding)
	   modified ,(get response 'last-modified)
	   etag ,(get response 'etag)]
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

(define (gp/fetch+ ref (default-ctype #f))
  (cond ((s3loc? ref) (s3/get+ ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (let* ((realpath (if (has-prefix (cdr ref) "/")
			      (subseq (cdr ref) 1)
			      (cdr ref)))
		(zip (car ref))
		(ctype (or default-ctype (guess-mimetype (get-namestring ref))))
		(charset (and ctype (has-prefix ctype "text")
			      (or (ctype->charset ctype) #t)))
		(content (zip/get zip realpath (not charset))))
	 `#[content ,(if (string? charset)
			 (packet->string content charset)
			 content)
	    ctype ,(or ctype {})
	    charset ,(if (string? charset) charset (if charset "utf-8" {}))
	    modified (tryif (bound? zip/modtime) (zip/modtime zip realpath))
	    etag ,(packet->base16 (md5 content))]))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (let ((mf (get (car ref) (cdr ref))))
	   (and (exists? mf)
		(if (memfile? mf)
		    `#[content ,(memfile-content mf)
		       ctype ,(or (memfile-ctype mf) default-ctype
				  (guess-mimetype (get-namestring ref)))
		       modified ,(memfile-modified mf)
		       etag ,(memfile-hash mf)]
		    `#[content ,mf
		       ctype ,(or default-ctype
				  (guess-mimetype (get-namestring ref)))
		       etag ,(packet->base16 (md5 mf))]))))
	((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	 (hashfs/get+ (car ref) (cdr ref)))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	  (car ref) (cdr ref) #t))
	((pair? ref) (gp/fetch+ (gp/path (car ref) (cdr ref)) default-ctype))
	((and (string? ref) (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (gp/urlfetch ref))
	((and (string? ref) (has-prefix ref "s3:")) (s3/get+ (->s3loc ref)))
	((and (string? ref) (not (file-exists? ref))) #f)
	((string? ref)
	 (let* ((ctype (or default-ctype (guess-mimetype (get-namestring ref))))
		(istext (and ctype (has-prefix ctype "text")))
		(charset (and istext (ctype->charset ctype)))
		(content (if istext (filestring ref charset) (filedata ref))))
	   `#[content  ,content
	      ctype    ,(or ctype {})
	      charset  ,(or charset {})
	      etag     ,(packet->base16 (md5 content))
	      modified ,(file-modtime ref)]))
	(else (error "Weird GPATH ref" ref))))

(define (gp/info ref (etag #t) (default-ctype #f))
  (cond ((s3loc? ref) (s3/info ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (let* ((realpath (if (has-prefix (cdr ref) "/")
			      (subseq (cdr ref) 1)
			      (cdr ref)))
		(zip (car ref))
		(ctype (or default-ctype (guess-mimetype (get-namestring ref))))
		(charset (and ctype (has-prefix ctype "text")
			      (or (ctype->charset ctype) #t)))
		(etag (tryif etag
			(packet->base16 (md5 (zip/get zip realpath #t))))))
	 `#[content ,(if (string? charset)
			 (packet->string content charset)
			 content)
	    ctype ,(or ctype {})
	    charset ,(if (string? charset) charset (if charset "utf-8" {}))
	    modified (tryif (bound? zip/modtime) (zip/modtime zip realpath))
	    etag ,etag]))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (let ((mf (get (car ref) (cdr ref))))
	   `#[path ,(gpath->string ref)
	      ctype ,(try (tryif (memfile? mf) (memfile-ctype mf))
			  default-ctype)
	      modified ,(tryif (memfile? mf)
			  (memfile-modified (get (car ref) (cdr ref))))
	      etag ,(tryif etag
		      (packet->base16
		       (if (memfile? mf)
			   (md5 (memfile-content mf))
			   (md5 mf))))]))
	((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	 (hashfs/info (car ref) (cdr ref)))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	  (car ref) (cdr ref) #f))
	((pair? ref) (gp/info (gp/path (car ref) (cdr ref))))
	((and (string? ref) (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (gp/urlinfo ref))
	((and (string? ref) (has-prefix ref "s3:"))
	 (s3/info (->s3loc ref)))
	((and (string? ref) (not (file-exists? ref))) #f)
	((string? ref)
	 (let* ((ctype (or default-ctype (guess-mimetype (get-namestring ref))))
		(istext (and ctype (has-prefix ctype "text")))
		(charset (and istext (ctype->charset ctype))))
	   (frame-create #f
	     'path (gpath->string ref) 'ctype (or ctype {})
	     'charset (or charset {})
	     'modified (file-modtime ref)
	     'etag (tryif etag
		     (packet->base16
		      (md5 (if charset
			       (filestring ref charset)
			       (if istext (filestring ref)
				   (filedata ref)))))))))
	(else (error "Weird GPATH ref" ref))))

(define (gp/modified ref)
  (cond ((s3loc? ref) (s3/modified ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (if (bound? zip/modtime)
	     (zip/modtime (car ref) (cdr ref))
	     (file-modtime (zip/filename (car ref)))))
	((and (pair? ref) (hashtable? (car ref)))
	 (memfile-modified (get (car ref) (cdr ref))))
	((and (pair? ref) (hashfs? (car ref)))
	 (get (hashfs/get+ (car ref) (cdr ref)) 'modified))
	((and (pair? ref) (pair? (car ref)))
	 (gp/modified (gp/path (car ref) (cdr ref))))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 (get ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	       (car ref) (cdr ref) #t)
	      'modified))
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
	(else (error "Invalid GPATH" ref))))

(define (gp/newer ref base)
  (if (gp/exists? base) 
      (and (gp/exists? ref)
	   (let ((bmod (gp/modified base))
		 (rmod (gp/modified ref)))
	     (if (and bmod rmod (not (time<? bmod rmod)))
		 ref
		 base)))
      (gp/exists ref)))

(define (gp/exists? ref)
  (cond ((s3loc? ref) (s3loc/exists? ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (zip/exists? (car ref) (cdr ref)))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (test (car ref) (cdr ref)))
	((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	 (exists? (hashfs/get+ (car ref) (cdr ref))))
	((and (pair? ref) (pair? (car ref)))
	 (gp/exists? (gp/path (car ref) (cdr ref))))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	  (car ref) (cdr ref) #t))
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
	(else (error "Weird GPATH ref" ref))))

(define (gp/exists ref) (and (gp/exists? ref) ref))

(define (gp/etag ref (compute #f))
  (cond ((s3loc? ref) (s3/etag ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (and (zip/exists? (car ref) (cdr ref)) compute
	      (md5 (zip/get (car ref) (cdr ref)))))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (and (test (car ref) (cdr ref)) compute
	      (md5 (get (car ref) (cdr ref)))))
	((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	 (try (md5 (hashfs/get (car ref) (cdr ref))) #f))
	((and (pair? ref) (pair? (car ref)))
	 (gp/etag (gp/path (car ref) (cdr ref)) compute))
	((pair? ref) (gp/etag (gp/path (car ref) (cdr ref)) compute))
	((and (string? ref) (exists has-prefix ref {"http:" "https:"}))
	 (let ((response (urlhead ref)))
	   (and (test response 'response)
		(<= 200 (get response 'response) 299)
		(try (get response 'tag)
		     (and compute
			  (begin (set! response (urlget ref))
			    (and (test response 'response)
				 (<= 200 (get response 'response) 299)
				 (try (get response 'etag)
				      (md5 (get response '%content))))))))))
	((and (string? ref) (has-prefix ref "s3:"))
	 (s3/etag (->s3loc ref) compute))
	((string? ref)
	 (and (file-exists? ref) compute (md5 (filedata ref))))
	(else (error "Weird GPATH ref" ref))))

;;; Recognizing and parsing GPATHs

(define (gpath? val)
  (if (pair? val)
      (and (string? (cdr val))
	   (or (s3loc? (car val)) (zipfile? (car val))))
      (if (string? val)
	  (and (not (position #\newline val))
	       (has-prefix val {"http:" "https:" "ftp:" "s3:" "/"}))
	  (or (s3loc? val) (zipfile? val)))))

(define (->gpath val (root #f))
  (if (string? val)
      (if (has-prefix val {"s3:" "S3:"}) (->s3loc val)
	  (if (has-prefix val {"http:" "https:" "ftp:"}) val
	      (if (has-prefix val "/") val
		  (if root (gp/mkpath root val) (abspath val)))))
      val))

(defambda (gp/has-suffix gpath suffixes (casematch #f))
  (if casematch
      (has-suffix (gp/basename gpath) suffixes)
      (has-suffix (downcase (gp/basename gpath)) (downcase suffixes))))

(defambda (gp/has-prefix gpath prefixes (casematch #f))
  (if (pair? gpath)
      (if casematch
	  (has-prefix (cdr gpath) prefixes)
	  (has-prefix (downcase (cdr gpath)) (downcase prefixes)))
      (if (string? gpath)
	  (if casematch
	      (has-prefix gpath prefixes)
	      (has-prefix (downcase gpath) (downcase prefixes)))
	  #f)))

(define (gp:config spec) (->gpath spec))

;;; Opening zip files at GPATHs

(define-init ziptemps (make-hashtable))

(define (zip/gopen path)
  (let* ((content (gp/fetch path))
	 (temporary (tempdir))
	 (file (mkpath temporary (gp/basename path))))
    (write-file file content)
    (prog1 (zip/open file)
      (store! ziptemps file temporary))))

(define (zip/gclose zip)
  (let* ((filename (zip/filename zip))
	 (dir (get ziptemps filename)))
    (prog1 (zip/close zip)
      (tempdir/done dir))))


;;;; Example configuration

(defrecord samplegfs (name #f) (data (make-hashtable)) (metadata (make-hashtable)))
(define (samplegfs-get gfs path (info))
  (if (bound? info)
      (let ((metadata (get (samplegfs-metadata gfs) path)))
	(when info
	  (set! metadata (deep-copy metadata))
	  (store! metadata 'content (get (samplegfs-data gfs) path)))
	metadata)
      (get (samplegfs-data gfs) path)))
(define (samplegfs-save gfs path content (ctype) (charset))
  (default! ctype (guess-mimetype path content))
  (default! charset (get-charset ctype))
  (store! (samplegfs-data gfs) path content)
  (store! (samplegfs-metadata gfs) path
	  (frame-create #f
	    'ctype ctype
	    'charset (tryif charset (if (string? charset) charset "utf-8"))
	    'modified (timestamp) 'length (length content)
	    'etag (md5 content))))
(config! 'gpath:handlers (gpath/handler 'samplegfs samplegfs-get samplegfs-save))
