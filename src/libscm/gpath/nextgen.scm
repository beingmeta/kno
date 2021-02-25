;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved.
Copyright (C) 2020-2021 beingmeta, LLC

(in-module 'gpath/nextgen)

(use-module '{varconfig logger webtools texttools fileio})

(define %used_modules '{varconfig})
(define-init %loglevel %notice%)

(use-module 'net/mimetable)

(define url-prefixes #/(http:|https:|ftp:)/i)
(define simple-schema-ids #/(http:|https:|ftp:|file:|local:)/i)

(define-init gpath-schema-ids (make-hashtable))
(store! gpath-schema-ids '{http https} #t)

(define schema-prefix #/[a-z0-9]*/i)

;;;; Core operations

(define (gpath:get ref (aspect #f) (opts #f))
  (cond ((string? ref)
	 (cond ((string-starts-with? ref url-prefixes) (url:get ref aspect opts))
	       ((has-prefix opts "file:///") (file:get (slice ref 8) aspect opts))
	       ((has-prefix? ref "data:") (datauri/fetch+ ref))
	       ((string-starts-with? ref schema-prefix)
		(gpath:get (string->gpath ref opts) aspect opts))
	       (else (file:get ref aspect opts))))
	((gpath-root? ref) (kno/send ref 'gpath:get #f aspect opts))
	((or (not (pair? ref)) (not (string? (cdr ref))))
	 (irritant ref |BadGPath| gpath:get))
	((string? (car ref)) 
	 (gpath:get (mkpath (car ref) (cdr ref)) aspect opts))
	((gpath-root? (car ref))
	 (kno/send (car ref) 'gpath:get (cdr ref) opts ))
	(else (irritant ref |BadGPath| gpath:get))))

;;;; Filesystem access

(define (file:get ref (aspect #f) (opts #f))
  (if (eq? aspect 'exists?)
      (file-exists? ref)
      (let* ((ctype (getopt opts 'content-type
			    (guess-mimetype (get-namestring ref) #f opts)))
	     (encoding (getopt opts 'content-encoding (path->encoding (get-namestring ref))))
	     (istext (and ctype (has-prefix ctype "text") (not encoding)))
	     (charset (and istext (getopt opts 'text-encoding (ctype->charset ctype))))
	     (content (and (or (not aspect) (overlaps? 'content aspect))
			   (if istext (if charset (filestring ref charset) (filecontent ref)) (filedata ref)))))
	(if (eq? aspect 'content) content
	    (frame-create #f
	      'gpath ref 'gpathstring ref
	      'rootpath (gp/rootpath ref)
	      'content-type (or ctype {})
	      'charset  (or charset {})
	      'modtime (file-modtime ref)
	      'content (tryif content content)
	      'md5 (tryif content (md5 hash)))))))

;;; URL access

(define gp/urlsubsts {})
(varconfig! gp:urlsubst gp/urlsubsts #f)

(define (gp/urlfetch url (opts #f) (w/content #f) (onerr) (max-redirects))
  (default! onerr (getopt opts 'errval #f))
  (default! max-redirects (getopt opts 'max-redirects #t))
  (let* ((newurl (textsubst url (qc (getopt opts 'urlsubsts) gp/urlsubsts)))
	 (curlopts (opt+ opts
		       (and max-redirects
			    #[follow ,(if (number? max-redirects) max-redirects #t)])))
	 (response (if w/content (urlget newurl curlopts) (urlhead newurl curlopts))))
    (if (and (exists? response) (table? response)
	     (test response 'response)
	     (<= 200 (get response 'response) 299)
	     (test response '%content))
	(let ((encoding (get response 'content-encoding))
	      (hash (md5 (get response 'content))))
	  (frame-create #f
	    'gpath url 'gpathstring url 'rootpath (uripath url)
	    'content-type (try (get response 'content-type) (guess-content-type url))
	    'content-length
	    (try (get response 'content-length) (length (get response 'content)))
	    'modtime (get response 'last-modified)
	    'content-encoding (get response 'content-encoding)
	    'etag (get response 'etag) 
	    'hash hash 'md5 (packet->base16 hash)
	    'charset (try (get info 'content-charset)
			  (ctype->charset (get info 'content-type)))
	    'content-encoding  (get response 'content-encoding)
	    'content (get response '%content)))
	(if (not onerr)
	    (if (equal? url newurl)
		(irritant response |FailedURL| gp/urlfetch newurl)
		(irritant response |FailedURL| gp/urlfetch newurl "(" url ")"))
	    (if (applicable? onerr)
		(onerr response opts url newurl)
		onerr)))))
(define (get-url-content ref opts)
  (urlcontent ref opts))

;;; Data URIs

(define datauri-pat
  #("data:" (label content-type (not> ";")) ";" (label enc (not> ",")) ","
    (label content (rest))))
(define (datauri/fetch+ string (justinfo #f))
  (let* ((parsed (text->frame datauri-pat string))
	 (ctype (get parsed 'content-type))
	 (enc (get parsed 'enc))
	 (data (get parsed 'content))
	 (base64 (identical? (downcase (get parsed 'enc)) "base64")))
    (if justinfo
	(if base64
	    `#[content-type ,ctype 
	       content-length ,(length (get parsed 'content))]
	    `#[content-type ,ctype 
	       content-length ,(length (get parsed 'content))
	       content-encoding ,enc])
	(let* ((charset (ctype->charset ctype))
	       (content (if base64 
			    (if (mimetype/text? ctype)
				(packet->string (base64->packet data) charset)
				(base64->packet data))
			    data)))
	  (if charset
	      `#[content-type ,ctype 
		 content-encoding ,enc
		 content-length ,(length (get parsed 'content))
		 content-encoded ,(length data)
		 content ,content charset ,charset]
	      `#[content-type ,ctype 
		 content-encoding ,enc
		 content-length ,(length (get parsed 'content))
		 content-encoded ,(length data)
		 content ,content])))))

;;; Resolving gpath strings

(define-init gpath-uri-recognizers '())
(varconfig! gpath:recognizers gpath-uri-recognizers #f config:push)

(define gpath-pattern
  `#((label scheme (isalnum+) #t) ":" 
     (opt (label domain {#("//" (not> "/") "/") #("//(" (not> ")/") ")/")}))
     (label path (rest))))

(define (gpath-root? x)
  (or (and (pair? x) (string? (cdr x)) (gpath-root? (car x)))
      (kno/handles? x 'gpath?)
      (kno/handles? x 'gpath:get)))

(define (gpath? x)
  (cond ((not (string? x)) (gpath-root? x))
	((has-prefix x {"http:" "https:" "ftp:" "data:"}) x)
	((position #\: x)
	 (test gpath-schema-ids (string->symbol (downcase (slice x 0 (position #\: x))))))
	(else (not (textsearch '(isspace) x)))))

(define (file-root? x)
  (and (string? x) 
       (or (string-starts-with? x #/file:/i)
	   (not (string-starts-with? x schema-prefix)))))

(define (string->gpath val (root #f) (opts #f))
  (cond ((string? val)
	 (cond ((not (string-starts-with? val schema-prefix)) val)
	       ((string-starts-with? val url-prefix)
		(recognize-url val))
	       (else (let* ((parsed (text->frame gpath-pattern val))
			    (scheme (get parsed 'scheme))
			    (domain (get parsed 'domain))
			    (path (get parsed 'path))
			    (handler (get gpath-schema-ids scheme)))
		       (cond ((eq? handler #t) val)
			     ((fail? handler) (irritant val |Bad GPath|))
			     (else (handler val opts root)))))))
	((and (pair? val) (or (string? (cdr val)) (empty-list? (cdr val)))
	      (gpath-root? val))
	 val)
	((gpath-root? val) val)
	(else (irritant val '|BadGPath|))))

(define (recognize-url url)
  (let ((found #f))
    (dolist (handler gpath-uri-recognizers)
      (set! found (handler url))
      (when found (break)))
    (or found url)))

;;;; GPATHs back to strings

(define (gpath->string path (opts #f))
  (cond ((string? path) path)
	((and (pair? path) (string? (car path))
	      (or (empty-list? (cdr path)) (empty-string? (cdr path))))
	 (car path))
	((and (pair? path) (string? (car path)) (string? (cdr path))
	      (position #\/ (cdr path)))
	 (mkpath (car path) (cdr path)))
	;;((and (pair? path) (pair? (car path))) (gpath->string (gp/mkpath (car path) (cdr path))))
	((and (pair? path) (hashtable? (car path)) (string? (cdr path)))
	 (stringout "hashtable:" (cdr path)
	   "//0x" (number->string (hashptr (car path)) 16) "/" (cdr path)))
	((and (pair? path) (string? (cdr path)) (gpath-root? (car path)))
	 (kno/send (car path) 'gpath:string opts (cdr path)))
	((gpath-root? path) (kno/send (car path) 'gpath:string opts #f))
	(else (irritant path |BadGPath|))))

;;; Other options

(defambda (gp/has-suffix gpath suffixes (casematch #f))
  (if casematch
      (string-ends-with? (gp/basename gpath) suffixes)
      (string-ends-with? (downcase (gp/basename gpath)) (downcase suffixes))))

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

(define (gp/localpath? gpath)
  (and (string? gpath)
       (or (string-starts-with? gpath #/file:/i)
	   (not (string-starts-with gpath schema-prefix)))))

(define (gp:config spec) (->gpath spec))

;;; Support functions

(define (guess-mimetype name (content) (opts #f))
  (when (and (bound? content) (table? content) (not opts))
    (set! opts content)
    (set! content #f))
  (or (and opts (getopt opts 'mimetype))
      (try (path->mimetype (gp/basename name) #f opts) #f)
      (if (and (bound? content) content)
	  (if (string? content) "text"
	      (if (packet? content) "application"
		  "application/dtype"))
	  (config 'mime:default))))

;;; Path operations

(define (gp/basename path)
  (cond ((pair? path)
	 (if (or (empty-list? (cdr path)) (empty-string? (cdr path)))
	     (gp/basename (car path))
	     (gp/basename (cdr path))))
	((not (string? path)) (irritant path |BadGPath| gp/basename))
	((not (string-starts-with? path schema-prefix)) (basename path))
	((string-starts-with? path simple-schema-ids) (basename path))
	((string-starts-with path #/data:/i)
	 (glom "data-" (packet->base16 (md5 path))
	   (try (ctype->suffix (get (datauri/info path) 'content-type) ".")
		"")))
	(else (basename path))))

(define (gp/rootpath path)
  (set! path (->gpath path))
  (cond ((pair? path)
	 (mkpath (gp/rootpath (car path))
		 (strip-prefix (cdr path) "/")))
	((and (string? path) (has-prefix path "data:")) 
	 (gp/basename path))
	((and (string? path) (file-directory? path)) (mkpath path ""))
	((string? path) path)
	(else "")))

(define (gp/location? path)
  (set! path (->gpath path))
  (cond ((string? path) (has-suffix path "/"))
	((and (pair? path) (string? (cdr path)))
	 (has-suffix (cdr path) "/"))
	(else #f)))

(define (gp/location path (opts #f))
  (cond ((and (string? path) (not (string-starts-with? path schema-prefix)))
	 (cond ((has-prefix path {"/" "~"}) (dirname path))
	       ((has-suffix path "/") (abspath (dirname path)))
	       ((position #\/ path) (abspath (dirname path)))
	       (else (abspath (dirname path)))))
	((and (string? path) (string-starts-with? path url-prefix)) (dirname path))
	((string? path) 
	 (let ((gpath (->gpath path)))
	   (if (equal? gpath path) (dirname path)
	       (gp/location gpath))))
	((not (pair? path))
	 (if (gpath-root? path)
	     (and (kno/handles? path 'gpath:location)
		  (kno/send path 'gpath:location opts))
	     (irritant path |BadGPath| gp/location)))
	((or (not (string? (cdr path))) (not (gpath-root? (car path))))
	 (irritant path |BadGPath| gp/location))
	((position #\/ (cdr path))
	 (cons (car path) (dirname (cdr path))))
	((kno/handles? (car path) 'gpath:location)
	 (kno/send (car path) 'gpath:location opts (cdr path)))
	(else #f)))

;;;; Manipulating GPATH paths

(define (makepath root path (mode *default-dirmode*) (require-subpath #f))
  (set! path 
    (cond ((string? path) path)
	  ((uuid? path) (glom "u:" (uuid->string path)))
	  ((packet? path) (glom "p:" (packet->base16 path)))
	  ((timestamp? path) (glom "t:" (get path 'iso)))
	  ((number? path) (number->string path))
	  ((gpath? path) (gp/basename path))
	  (else (irritant path |BadPathElement| gp/makepath))))
  (when (and (pair? root) (null? (cdr root)))
    (set! root (cons (car root) "")))
  (if (string? root) (set! root (string->root root))
      (if (and (pair? root) (string? (car root)))
	  (set! root (cons (string->root (car root)) (cdr root)))))
  (cond ((or (not path) (null? path)) root)
	((not (string? path))
	 (error |TypeError| MAKEPATH "Relative path is not a string" path))
	((string-starts-with? path schema-prefix) (->gpath path))
	((and (pair? root) (has-prefix path "/")) (cons (car root) (slice path 1)))
	((gpath-root? root) (cons root path))
	((and (pair? root) (not (string? (cdr root))))
	 (error "Bad GPATH root" MAKEPATH "not a valid GPATH root" root))
	((string? root) (checkdir (mkpath root path)))
	((and (pair? root) (string? (car root)))
	 (checkdir (mkpath (mkpath (car root) (cdr root)) path)))
	((pair? root) (cons (car root) (mkpath (cdr root) path)))
	((string? root) (cons (checkdir root) path))
	(else (error "Weird GPATH root" MAKEPATH
		     (stringout root " for " path)))))
(define gp/makepath makepath)
