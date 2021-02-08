;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved.

(in-module 'gpath)

(use-module '{varconfig logger webtools texttools fileio})

(define %used_modules '{varconfig ezrecords})
(define-init %loglevel %notice%)

(module-export!
 '{gp/write! gp/save!
   writeout writeout/type
   gp/writeout gp/writeout! gp/writeout+!
   gpath? ->gpath uri->gpath gp/localpath?
   gp/location? gp/location gp/basename gp/rootpath
   gp/has-suffix gp/has-prefix
   gp/fetch gp/fetch+ gp/etag gp/info
   gp/exists? gp/exists gp/modtime gp/newer?
   gp/modified gp/newer
   gp/list gp/list+
   gp/path gp/mkpath gp/subpath gp/makepath
   gpath->string gp/string gp>s gpath->location
   gp:config gpath/handler
   gp/urlfetch
   dtype->gpath gpath->dtype
   datauri/fetch+ datauri/info
   gp/copy! gp/copy*!})

(use-module '{net/mimetable ezrecords})

(define url-prefixes #/(http:|https:|ftp:)/i)
(define simple-schema-ids #/(http:|https:|ftp:|file:|local:)/i)

(define-init gpath-schema-ids (make-hashtable))
(store! gpath-schema-ids '{http https} #t)

(define schema-prefix #/[a-z0-9]*/i)

;;; This is a generic path facility.  A gpath is either a string
;;; or a gpath 'root' or a cons of a gpath 'root' and a string.

;;; Gpath roots take the following methods:
;;;  gpath? self (opts #f) (path #f)  ==> boolean
;;;  gpath:exists? self (opts #f) (path #f)  ==> boolean
;;;  gpath:location? self (opts #f) (path #f) ==> boolean
;;;  gpath:metadata self  (opts #f) (path #f) (slotid #f) ==> keymap or value (when slotid given)
;;;  gpath:content self (opts #f) (path #f) ==> string or packet
;;;  gpath:get self (opts #f) (path #f) ==> keymap with content slot
;;; And may take:
;;;  gpath:save self (content) (path #f) (opts #f) ==> keymap

(define (gp/exists? ref (opts #f))
  (cond ((string? ref)
	 (cond ((string-starts-with? ref url-prefixes)
		(let ((info (urlhead ref opts)))
		  (and (exists? info) info
		       (test info 'response)
		       (<= 200 (get info 'response) 299))))
	       ((has-prefix opts "file:///") (file-exists? (slice ref 8)))
	       ((has-prefix? ref "data:") #t)
	       ((string-starts-with? ref #((isalnum+) ":"))
		(try (gp/exists? (->gpath ref opts) opts) #f))
	       (else (file-exists? ref))))
	((and (pair? ref) (string? (cdr ref)) (kno/handles? (car ref) 'gpath:exists?))
	 (kno/send (car ref) 'gpath:exists? opts (cdr ref)))
	((and (pair? ref) (string? (cdr ref)) (kno/handles? (car ref) 'gpath:get))
	 (and (try (kno/send (car ref) 'gpath:get opts (cdr ref)) #f) #t))
	((kno/handles? ref 'gpath:exists?) (kno/send ref 'gpath:exists? opts))
	((kno/handles? ref 'gpath:get) 
	 (and (try (kno/send ref 'gpath:get opts) #f) #t))
	((testopt opts 'onerror)
	 (let ((errval (getopt opts 'onerror)))
	   (if (applicable? errval) (errval ref opts)
	       errval)))
	(else (irritant ref |BadGPath| gp/exists?))))

(define (gp/location? ref (opts #f))
  (cond ((and (string? ref) (has-suffix ref "/")) #t)
	((string? ref)
	 (cond ((string-starts-with? ref url-prefixes)
		(let ((info (urlhead ref opts)))
		  (and (exists? info) info
		       (test info 'response 404))))
	       ((has-prefix opts "file:///") 
		(file-directory? (slice ref 8)))
	       ((has-prefix? ref "data:") #f)
	       ((string-starts-with? ref #((isalnum+) ":"))
		(try (gp/location? (->gpath ref opts) opts) #f))
	       (else (and (file-exists? ref) (not (file-directory? ref))))))
	((and (pair? ref) (string? (cdr ref)) (has-suffix (cdr ref) "/")) #t)
	((and (pair? ref) (string? (cdr ref)) (kno/handles? (car ref) 'gpath:location?))
	 (kno/send (car ref) 'gpath:local? opts (cdr ref)))
	((kno/handles? ref 'gpath:location?) (kno/send ref 'gpath:location? opts))
	((testopt opts 'onerror)
	 (let ((errval (getopt opts 'onerror)))
	   (if (applicable? errval) (errval ref opts)
	       errval)))
	(else (irritant ref |BadGPath| gp/location?))))

(define (gp/metadata ref (opts #f) (attrib #f))
  (cond ((string? ref)
	 (cond ((string-starts-with? ref url-prefixes) 
		(if attrib 
		    (try (get (gp/urlfetch ref opts #f) attrib) #f)
		    (gp/urlfetch ref opts #f)))
	       ((has-prefix opts "file:///") 
		(get-file-metadata (slice ref 8) opts attrib))
	       ((has-prefix? ref "data:") (datauri/fetch+ ref #t))
	       ((string-starts-with? ref #((isalnum+) ":"))
		(gp/metadata (->gpath ref opts) opts attrib))
	       ((file-exists? ref) (get-file-metadata ref opts attrib))
	       (else #f)))
	((and (pair? ref) (not (string? (cdr ref))))
	 (irritant ref |BadGPath| gp/content))
	((not (or (pair? ref) (not (gpath-root? ref))))
	 (irritant ref  |BadGPath| gp/content))
	((and (pair? ref) (kno/handles? (car ref) 'gpath:metadata))
	 (kno/send (car ref) 'gpath:metadata opts (cdr ref) attrib))
	((and (pair? ref) (kno/handles? (car ref) 'gpath:get))
	 (if attrib
	     (try (get (kno/send (car ref) 'gpath:get opts (cdr ref)) attrib) #f)
	     (kno/send (car ref) 'gpath:get opts (cdr ref))))
	((kno/handles? ref 'gpath:metadata) (kno/send ref 'gpath:metadata opts attrib))
	((kno/handles? ref 'gpath:get) 
	 (if attrib
	     (try (get (kno/send ref 'gpath:get opts) attrib) #f)
	     (kno/send ref 'gpath:get opts)))
	((testopt opts 'onerror)
	 (let ((errval (getopt opts 'onerror)))
	   (if (applicable? errval) (errval ref opts)
	       errval)))
	(else (irritant ref |BadGPath| gp/metadata))))

(define (gp/content ref (opts #f))
  (cond ((string? ref)
	 (cond ((string-starts-with? ref url-prefixes)
		(get-url-content ref opts))
	       ((has-prefix opts "file:///") (get-file-content (slice ref 8) opts))
	       ((has-prefix? ref "data:") (get (datauri/fetch+ ref) 'content))
	       ((string-starts-with? ref #((isalnum+) ":"))
		(gp/content (->gpath ref opts) opts))
	       ((file-exists? ref) (get-file-content ref opts))))
	((and (pair? ref) (not (string? (cdr ref))))
	 (irritant ref |BadGPath| gp/content))
	((not (or (pair? ref) (not (gpath-root? ref))))
	 (irritant ref  |BadGPath| gp/content))
	((and (pair? ref) (kno/handles? (car ref) 'gpath:content))
	 (kno/send (car ref) 'gpath:content opts (cdr ref)))
	((and (pair? ref) (kno/handles? (car ref) 'gpath:get))
	 (try (get (kno/send (car ref) 'gpath:get opts (cdr ref)) 'content) #f))
	((kno/handles? ref 'gpath:content) (kno/send ref 'gpath:content opts))
	((kno/handles? ref 'gpath:get) 
	 (try (get (kno/send ref 'gpath:get opts) 'content) #f))
	((testopt opts 'onerror)
	 (let ((errval (getopt opts 'onerror)))
	   (if (applicable? errval) (errval ref opts)
	       errval)))
	(else (irritant ref |BadGPath| gp/content))))

(define (gp/get ref (opts #f))
  (cond ((string? ref)
	 (cond ((string-starts-with? ref url-prefixes) (gp/urlfetch ref opts #t))
	       ((has-prefix opts "file:///") (get-file-metadata (slice ref 8) opts #t))
	       ((has-prefix? ref "data:") (datauri/fetch+ ref))
	       ((string-starts-with? ref #((isalnum+) ":"))
		(gp/content (->gpath ref opts) opts))
	       ((file-exists? ref) (get-file-metadata ref opts #t))
	       (else #f)))
	((and (pair? ref) (string? (cdr ref)) (kno/handles? (car ref) 'gpath:get))
	 (kno/send (car ref) 'gpath:get opts (cdr ref)))
	((and (pair? ref) (string? (cdr ref))
	      (or (kno/handles? (car ref) 'gpath:content)
		  (kno/handles? (car ref) 'gpath:metadata)))
	 (let ((metadata (if (kno/handles? (car ref) 'gpath:metadata)
			     (deep-copy (kno/send (car ref) 'gpath:metadata opts (cdr ref)))
			     #[]))
	       (content (and (kno/handles? (car ref) 'gpath:content)
			     (kno/send (car ref) 'gpath:content opts (cdr ref)))))
	   (when (and (exists? content) content)
	     (store! metadata 'content content)
	     (store! metadata 'content-length (length content)))
	   metadata))
	((kno/handles? ref 'gpath:get) (kno/send ref 'gpath:get opts))
	((or (kno/handles? ref 'gpath:content) (kno/handles? ref 'gpath:metadata))
	 (let ((metadata (if (kno/handles? ref 'gpath:metadata)
			     (deep-copy (kno/send ref 'gpath:metadata opts))
			     #[]))
	       (content (and (kno/handles? (car ref) 'gpath:content)
			     (kno/send (car ref) 'gpath:content opts))))
	   (when (and (exists? content) content)
	     (store! metadata 'content content)
	     (store! metadata 'content-length (length content)))
	   metadata))
	((testopt opts 'onerror)
	 (let ((errval (getopt opts 'onerror)))
	   (if (applicable? errval) (errval ref opts)
	       errval)))
	(else (irritant ref |BadGPath| gp/content))))

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
(define gp/string gpath->string)
(define gp>s gpath->string)

;;;; Filesystem access

(define (get-file-metadata ref (opts #f) (w/content #f))
  (let* ((ctype (getopt opts 'content-type
			(guess-mimetype (get-namestring ref) #f opts)))
	 (encoding (getopt opts 'content-encoding (path->encoding (get-namestring ref))))
	 (istext (and ctype (has-prefix ctype "text") (not encoding)))
	 (charset (and istext (getopt opts 'text-encoding (ctype->charset ctype))))
	 (result (frame-create #f
		     'gpath ref 'gpathstring ref
		     'rootpath (gp/rootpath ref)
		     'content-type (or ctype {})
		     'charset  (or charset {})
		     'modtime (file-modtime ref))))
    (when w/content
      (let* ((content (if istext 
			  (if charset 
			      (filestring (abspath ref) charset)
			      (filecontent (abspath ref)))
			  (filedata (abspath ref))))
	     (hash (md5 content)))
	(store! result 'content content)
	(store! result 'hash    hash)
	(store! result 'md5     hash)))
    result))
(define (get-file-content ref (opts #f) (w/content #f))
  (let* ((ctype (getopt opts 'content-type
			(guess-mimetype (get-namestring ref) #f opts)))
	 (encoding (getopt opts 'content-encoding (path->encoding (get-namestring ref))))
	 (istext (and ctype (has-prefix ctype "text") (not encoding)))
	 (charset (and istext (getopt opts 'text-encoding (ctype->charset ctype)))))
    (if istext 
	(if charset 
	    (filestring (abspath ref) charset)
	    (filecontent (abspath ref)))
	(filedata (abspath ref)))))

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

;;; Path operations

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

(define (->gpath val (root #f) (opts #f))
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

(define (gpath->location path)
  (set! path (->gpath path))
  (if (gp/location? path) path (gp/location path)))

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

(define (gp/path root path . more)
  (let ((result (if path (makepath (->gpath root) path) (->gpath root))))
    (if (null? more) result
	(if (not (car more))
	    (if (null? (cdr more)) result
		(apply gp/path result (cdr more)))
	    (apply gp/path result (car more) (cdr more))))))
(define gp/mkpath gp/path)

;; (define (gp/subpath root path . more)
;;   (let ((result (makepath (->gpath root) path *default-dirmode* #t)))
;;     (if (null? more) result
;; 	(if (not (car more))
;; 	    (if (null? (cdr more)) result
;; 		(apply gp/subpath result (cdr more)))
;; 	    (apply gp/subpath result (car more) (cdr more))))))

;;; Fetching

(define (gp/fetch ref (ctype #f) (opts #f) (encoding #f))
  (when (and (table? ctype) (not opts))
    (set! opts ctype)
    (set! ctype #f))
  (unless ctype (set! ctype (guess-mimetype (get-namestring ref) #f opts)))
  (unless encoding 
    (set! encoding (getopt opts 'content-encoding (path->encoding (get-namestring ref)))))
  (loginfo |GP/FETCH| ref " expecting " ctype " given " (pprint opts))
  (cond ((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (memfile-content (get (car ref) (cdr ref))))
	((and (pair? ref) (string? (cdr ref)) (gpath-root? (car ref)))
	 (kno/send (car ref) 'gpath:get (cdr ref)))
	((pair? ref) (irritant ref |NotGPath|))
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (set! ref (try (uri->gpath ref) ref))
	 (if (string? ref)
	     (get (gp/urlfetch ref) 'content)
	     (get (gp/fetch ref) 'content)))
	((and (string? ref) (has-prefix ref "data:"))
	 (get (datauri/fetch+ ref) 'content))
	((and (string? ref) (string-starts-with? ref #((isalnum+) ":")))
	 (gp/fetch (->gpath ref) ctype opts encoding))
	((and (string? ref) (not (file-exists? (abspath ref)))) #f)
	((string? ref)
	 (if  (and ctype (not encoding)
		   (or (mimetype/text? ctype)
		       (textsearch #{"xml" "json"} ctype)))
	      (if (and ctype (ctype->charset ctype))
		  (filestring (abspath ref) (and ctype (ctype->charset ctype)))
		  (filecontent (abspath ref)))
	      (filedata (abspath ref))))
	(else (error "Weird GPATH ref" GP/FETCH #f ref))))

(define (gp/fetch+ ref (ctype #f) (opts #f) (encoding))
  (if (urish? ref) (set! ref (->gpath ref)))
  (when (and (table? ctype) (not opts))
    (set! opts ctype)
    (set! ctype #f))
  (unless ctype (set! ctype (guess-mimetype (get-namestring ref) #f opts)))
  (default! encoding 
    (getopt opts 'content-encoding (path->encoding (get-namestring ref))))
  (loginfo |GP/FETCH+| ref " expecting " ctype " given " (pprint opts))
  (cond ((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (let ((mf (get (car ref) (cdr ref))))
	   (and (exists? mf)
		(if (memfile? mf)
		    `#[gpath ,(->gpath ref) 
		       gpathstring ,(gpath->string (->gpath ref))
		       rootpath ,(gp/rootpath ref)
		       content ,(memfile-content mf)
		       content-length ,(length (memfile-content mf))
		       content-type
		       ,(or (memfile-mimetype mf) ctype
			    (guess-mimetype (get-namestring ref) #f opts))
		       last-modified ,(memfile-modified mf)
		       md5 ,(memfile-hash mf)]
		    `#[gpath ,(->gpath ref) 
		       gpathstring ,(gpath->string (->gpath ref))
		       rootpath ,(gp/rootpath ref)
		       content ,mf
		       content-length ,(length mf)
		       content-type
		       ,(or ctype
			    (guess-mimetype (get-namestring ref) #f opts))
		       md5 ,(packet->base16 (md5 mf))]))))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	  (car ref) (cdr ref) (if ctype (cons `#[content-type ,ctype] opts) opts)
	  #t))
	((pair? ref) (gp/fetch+ (gp/path (car ref) (cdr ref)) ctype))
	((and (string? ref) (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (set! ref (try (uri->gpath ref) ref))
	 (if (string? ref) (gp/urlfetch ref) (gp/fetch+ ref)))
	((and (string? ref) (has-prefix ref "data:"))
	 (datauri/fetch+ ref))
	((and (string? ref) (string-starts-with? ref #((isalnum+) ":")))
	 (irritant ref |Bad gpath scheme|))
	((and (string? ref) (not (file-exists? (abspath ref)))) #f)
	((string? ref)
	 (let* ((ctype (or ctype (guess-mimetype (get-namestring ref) #f opts)))
		(istext (and ctype (has-prefix ctype "text") (not encoding)))
		(charset (and istext (ctype->charset ctype)))
		(content (if istext 
			     (if charset 
				 (filestring (abspath ref) charset)
				 (filecontent (abspath ref)))
			     (filedata (abspath ref))))
		(hash (md5 content)))
	   (frame-create #f
	     'gpath (->gpath ref) 
	     'gpathstring (gpath->string (->gpath ref))
	     'rootpath (gp/rootpath ref)
	     'content  content
	     'content-type (or ctype {})
	     'content-length (length content)
	     'charset  (or charset {})
	     'hash     (packet->base16 hash)
	     'md5      (packet->base16 hash)
	     'last-modified (file-modtime ref))))
	(else (error "Weird GPATH ref" GP/FETCH+ #f ref))))

(define (gp/info ref (etag #t) (ctype #f) (opts #f) (encoding))
  (if (urish? ref) (set! ref (->gpath ref)))
  (when (and (table? ctype) (not opts))
    (set! opts ctype)
    (set! ctype #f))
  (unless ctype (set! ctype (guess-mimetype (get-namestring ref) #f opts)))
  (default! encoding
    (getopt opts 'content-encoding (path->encoding (get-namestring ref))))
  (loginfo |GP/INFO| ref " expecting " ctype " given " (pprint opts))
  (cond ((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (let* ((mf (get (car ref) (cdr ref)))
		(hash (tryif etag
			(if (memfile? mf)
			    (md5 (memfile-content mf))
			    (md5 mf)))))
	   (frame-create #f
	     'gpath (->gpath ref) 'gpathstring (gpath->string (->gpath ref))
	     'rootpath (gp/rootpath ref)
	     'content-type (try (tryif (memfile? mf) (memfile-mimetype mf))
				ctype)
	     'last-modified (tryif (memfile? mf)
			      (memfile-modified (get (car ref) (cdr ref))))
	     'etag (tryif hash hash)
	     'hash (tryif hash hash)
	     'md5 (tryif hash (packet->base16 hash)))))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	  (car ref) (cdr ref) (if ctype (cons `#[content-type ,ctype] opts) opts)
	  #f))
	((pair? ref) (gp/info (gp/path (car ref) (cdr ref))))
	((and (string? ref) (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (gp/urlinfo ref))
	((and (string? ref) (has-prefix ref "data:"))
	 (datauri/fetch+ ref #t))
	((and (string? ref) (file-directory? ref))
	 (frame-create #f
	   'path ref 'gpath ref 'location #t))
	((and (string? ref) (not (file-exists? ref))) #f)
	((string? ref) (file-info ref etag ctype opts encoding))
	(else (error "Weird GPATH ref" GP/INFO #f ref))))

(define (file-info ref etag ctype opts encoding)
  (let* ((ref (abspath ref))
	 (ctype (or ctype (guess-mimetype (get-namestring ref) #f opts)))
	 (istext (and ctype (mimetype/text? ctype) (not encoding)))
	 (charset (and istext (ctype->charset ctype)))
	 (gpathstring (gpath->string ref))
	 (hash (md5 (if charset
			(filestring ref charset)
			(if istext (filecontent ref)
			    (filedata ref))))))
    (frame-create #f
      'gpath (->gpath ref) 'gpathstring gpathstring
      'rootpath (gp/rootpath ref)
      'path gpathstring
      'content-type (or ctype {})
      'charset (or charset {})
      'last-modified (file-modtime ref)
      'hash (tryif hash hash)
      'md5 (tryif hash (packet->base16 hash)))))

(define (gp/modtime ref)
  (if (urish? ref) (set! ref (->gpath ref)))
  (cond ((and (compound-type? ref) 
	      (test gpath-handlers (compound-tag ref)))
	 (get ((gpath-handler-get (get gpath-handlers (compound-tag ref)))
	       ref "" #f #f)
	      'last-modified))
	((and (pair? ref) (hashtable? (car ref)))
	 (memfile-modified (get (car ref) (cdr ref))))
	((and (pair? ref) (pair? (car ref)))
	 (gp/modtime (gp/path (car ref) (cdr ref))))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 (get ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	       (car ref) (cdr ref) #f #f)
	      'last-modified))
	((and (string? ref) (has-prefix ref "data:")) #T2001-08-16)
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (let ((response (urlget ref)))
	   (if (and (test response 'response)
		    (<= 200 (get response 'response) 299)
		    (exists? (get response '%content)))
	       (try (get response 'last-modified) #f)
	       #f)))
	((string? ref) (file-modtime ref))
	(else (error "Invalid GPATH" GP/MODTIME #f ref))))
(define gp/modified (fcn/alias gp/modtime))

(define (gp/newer? ref base)
  (if (urish? ref) (set! ref (->gpath ref)))
  (if (urish? base) (set! base (->gpath base)))
  (if (gp/exists? base) 
      (and (gp/exists? ref)
	   (let ((bmod (gp/modtime base))
		 (rmod (gp/modtime ref)))
	     (if (and bmod rmod (not (time<? bmod rmod)))
		 ref
		 base)))
      (gp/exists ref)))
(define gp/newer (fcn/alias gp/newer?))

(define (gp/exists? ref)
  (cond ((and (compound-type? ref) 
	      (test gpath-handlers (compound-tag ref)))
	 ((gpath-handler-get (get gpath-handlers (compound-tag ref)))
	  ref "" #f #f))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (test (car ref) (cdr ref)))
	((and (pair? ref) (pair? (car ref)))
	 (gp/exists? (gp/path (car ref) (cdr ref))))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	  (car ref) (cdr ref) #f #f))
	((pair? ref) (gp/exists? (gp/path (car ref) (cdr ref))))
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (let ((response (urlget ref)))
	   (and (test response 'response)
		(<= 200 (get response 'response) 299)
		(exists? (get response '%content)))))
	((string? ref) (file-exists? ref))
	(else (error "Weird GPATH ref" GP/EXISTS? #f ref))))

(define (gp/exists ref) (and (gp/exists? ref) ref))

(define (gp/etag ref (compute #f))
  (cond ((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (and (test (car ref) (cdr ref)) compute
	      (md5 (get (car ref) (cdr ref)))))
	((and (pair? ref) (pair? (car ref)))
	 (gp/etag (gp/path (car ref) (cdr ref)) compute))
	((pair? ref) (gp/etag (gp/path (car ref) (cdr ref)) compute))
	((and (string? ref) (exists has-prefix ref {"http:" "https:"}))
	 (let ((response (urlhead ref)))
	   (and (test response 'response)
		(<= 200 (get response 'response) 299)
		(try (get response 'etag)
		     (and compute
			  (begin (set! response (urlget ref))
			    (and (test response 'response)
				 (<= 200 (get response 'response) 299)
				 (try (get response 'etag)
				      (packet->string (md5 (get response '%content))
						      16)))))))))
	((string? ref)
	 (and (file-exists? ref) compute (packet->string (md5 (filedata ref)) 16)))
	(else (error "Weird GPATH ref" GP/ETAG #f ref))))

(defambda (filter-list matches matcher (prefix #f))
  (if (not matcher) 
      matches
      (filter-choices (match matches)
	(and
	 (or (not prefix) (has-prefix (gp/rootpath match) prefix))
	 (textsearch (qc matcher) (gp/rootpath match))))))
(defambda (filter-info matches matcher (prefix #f))
  (if (not matcher) 
      matches
      (filter-choices (match matches)
	(and
	 (or (not prefix) (has-prefix (gp/rootpath match) prefix))
	 (textsearch (qc matcher) (gp/rootpath match))))))

(defambda (gp/list ref (matcher #f))
  (for-choices ref
    (cond ((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	   (filter-list (getkeys (car ref)) matcher (cdr ref)))
	  ((and (string? ref) (gp/localpath? ref))
	   (filter-list (choice (getfiles ref) (mkpath (getdirs ref) ""))
			matcher))
	  (else (irritant ref |NoListingMethod| )))))

(defambda (gp/list+ ref (matcher #f))
  (cond (else (for-choices (item (gp/list ref matcher))
		(add-relpath (gp/info item) (gp/rootpath ref))))))

(define (add-relpath info prefix)
  (modify-frame info 'relpath 
		(strip-prefix (get info 'rootpath) prefix)))

;;; Write support functions

(define-init urish?
  (lambda (x)
    (and (string? x)
	 (string-starts-with? x #/^[a-zA-Z.]{2,32}:/))))

(define (checkdir dirpath)
  (unless (string-starts-with? dirpath schema-prefix)
    (mkdirs dirpath))
  (when (string-starts-with? dirpath #/file:/i) (mkdirs (subseq dirpath 5)))
  dirpath)

(defambda (merge-headers . args)
  (apply append (choice->list (pickstrings (elts args)))
	 (choice->list (pick (elts args) pair?))))

(define charset-pat #("charset=" (label charset (not> ";"))))

(defambda (get-charset ctype (opts #f))
  (try (tryif opts (getopt opts 'charset))
       (tryif (and (exists? ctype) ctype)
	 (get (text->frames charset-pat (pickstrings ctype))
	      'charset))
       #f))

(define (root-path path)
  (if (not (string? path)) path
      (if (has-prefix path "/") (slice path 1)
	  (if (has-prefix path "./") (slice path 2)
	      path))))

(define (string->root string)
  (cond ((not (string? string)) (irritant string |GPathNotString|))
	(else (uribase string))))

;;; Writing to a gpath

(defambda (gp/write! saveto name content
	    (ctype #f) (charset #f) (opts #f) (encoding))
  (when (urish? saveto) (set! saveto (->gpath saveto)))
  (when (and ctype (table? ctype) (not opts))
    (set! opts ctype) (set! ctype #f))
  (when (and charset (table? charset) (not opts))
    (set! opts charset) (set! charset #f))
  (default! encoding
    (getopt opts 'content-encoding (path->encoding name)))
  (do-choices name
    (let ((ctype (or ctype (guess-mimetype (get-namestring name) content opts)))
	  (charset (or charset (get-charset ctype opts))))
      ;; Do any charset conversion required by the CTYPE
      (when (and charset (not encoding)
		 (string? content)
		 (not (overlaps? (downcase charset) {"utf8" "utf-8"})))
	(set! content (packet->string (string->packet content) charset)))
      (gp/save! (gp/mkpath saveto name) content ctype charset))))
(define (datauri/info string) (datauri/fetch+ string #t))

(define (get-namestring gpath)
  (if (string? gpath) gpath
      (if (pair? gpath) (cdr gpath)
	  "")))

(defambda (gp/save! dest content (ctype #f) (charset #f) (opts #f) (encoding #f))
  (when (ambiguous? content) (irritant content |AmbiguousContent| gp/save!))
  (set! dest (->gpath dest opts))
  (when (urish? dest) (set! dest (->gpath dest)))
  (when (and ctype (table? ctype) (not opts))
    (set! opts ctype) (set! ctype #f))
  (when (and charset (table? charset) (not opts))
    (set! opts charset) (set! charset #f))
  (unless encoding 
    (set! encoding (getopt opts 'content-encoding (path->encoding  (get-namestring dest)))))
  (if (exists? content)
      (do-choices dest
	(let ((ctype (or ctype (guess-mimetype content opts)))
	      (charset (try (or charset (get-charset ctype opts)) #f))
	      (headers (merge-headers (tryif encoding (glom "Content-Encoding: " encoding))
				      (getopt opts 'headers))))
	  (when (and (string? dest) (has-prefix dest {"http:" "https:"}))
	    (set! dest (try (uri->gpath dest) dest)))
	  (loginfo GP/SAVE! "Saving " (length content)
		   (if (string? content) " characters of "
		       (if (packet? content) " bytes of "))
		   (if (and (exists? ctype) ctype)
		       (printout (write ctype) " "))
		   "content into " dest)
	  ;; Do any charset conversion required by the CTYPE
	  (when (and charset (string? content)
		     (not (overlaps? (downcase charset) {"utf8" "utf-8"})))
	    (set! content (packet->string (string->packet content) charset)))
	  (cond ((and (string? dest) (string-starts-with? dest {"http:" "https:"}))
		 (let ((req (urlput dest content ctype `#[METHOD PUT HEADERS ,headers])))
		   (unless (response/ok? req)
		     (if (response/badmethod? req)
			 (let ((req (urlput dest content ctype `#[METHOD POST HEADERS ,headers])))
			   (unless (response/ok? req)
			     (error "Couldn't save to URL"  GP/SAVE!
				    dest req)))
			 (error "Couldn't save to URL" GP/SAVE!
				dest req)))))
		((string? dest) (write-file dest content))
		((and (pair? dest) (hashtable? (car dest)))
		 (store! (car dest) (cdr dest)
			 (cons-memfile ctype content (timestamp)
				       (packet->base16 (md5 content)))))
		((and (pair? dest) (string? (car dest)) (string? (cdr dest)))
		 (write-file (mkpath (car dest) (cdr dest)) content))
		((and (pair? dest) (compound-type? (car dest))
		      (test gpath-handlers (compound-tag (car dest)))
		      (gpath-handler-save (get gpath-handlers (compound-tag (car dest)))))
		 ((gpath-handler-save (get gpath-handlers (compound-tag (car dest))))
		  (car dest) (cdr dest) content ctype
		  (if charset (cons `#[charset ,charset] opts) opts)))
		(else (error "Bad GP/SAVE call" GP/SAVE! #f dest)))
	  (loginfo GP/SAVE! "Saved " (length content)
		   (if (string? content) " characters of "
		       (if (packet? content) " bytes of "))
		   (if (and (exists? ctype) ctype)
		       (printout (write ctype) " "))
		   "content into " dest)))))

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

;;;; Example configuration

(define-init samplegfs-registry (make-hashtable))

(defrecord samplegfs (name #f)
  (data (make-hashtable)) (metadata (make-hashtable))
  (id (getuuid)))

(define (init-samplegfs id)
  (try (get samplegfs-registry id)
       (let ((gfs (cons-gfs (make-hashtable) (make-hashtable) id)))
	 (store! samplegfs-registry id gfs)
	 gfs)))
(define-init register-samplegfs-sync
  (slambda (id) (init-samplegfs id)))
(define (register-samplegfs id)
  (try (get samplegfs-registry id) (register-samplegfs-sync id)))

(define (samplegfs:exists gfs (path #f) (opts #f))
  (or (not path)
      (try (test (samplegfs-data gfs) path) #f)))
(define (samplegfs:content gfs path (opts #f))
  (try (get (samplegfs-data gfs) path) #f))
(define (samplegfs:info gfs (path #f) (opts #f))
  (cond ((not path) #f)
	((test (samplegfs-metadata gfs) path)
	 (try (get (samplegfs-metadata gfs) path) #[]))
	(else #f)))
(define (samplegfs:save gfs path content (metadata #f) (opts #f) (ctype #f) (charset #f))
  (default! ctype (guess-mimetype path content opts))
  (default! charset (get-charset ctype opts))
  (when content (store! (samplegfs-data gfs) path content))
  (let* ((cur-data (get (samplegfs-metadata gfs) path))
	 (data (try cur-data #[])))
    (when (fail? cur-data) (store! (samplegfs-metadata gfs) path data))
    (when content
      (let ((hash (and content (md5 content))))
	(store! data 'content-type (or ctype (get cur-data 'content-type)))
	(store! data 'content-length (length content))
	(store! data 'charset (tryif charset (if (string? charset) charset "utf-8")))
	(store! data 'last-modified (timestamp))))))
(define (samplegfs:string gfs path)
  (stringout "samplegfs:" )
  (default! ctype (guess-mimetype path content opts))
  (default! charset (get-charset ctype opts))
  (when content (store! (samplegfs-data gfs) path content))
  (let* ((cur-data (get (samplegfs-metadata gfs) path))
	 (data (try cur-data #[])))
    (when (fail? cur-data) (store! (samplegfs-metadata gfs) path data))
    (when content
      (let ((hash (and content (md5 content))))
	(store! data 'content-type (or ctype (get cur-data 'content-type)))
	(store! data 'content-length (length content))
	(store! data 'charset (tryif charset (if (string? charset) charset "utf-8")))
	(store! data 'last-modified (timestamp))))))

(kno/handler! 'samplegfs 'gpath:exists samplegfs:exists)
(kno/handler! 'samplegfs 'gpath:content samplegfs:content)
(kno/handler! 'samplegfs 'gpath:info samplegfs:info)
(kno/handler! 'samplegfs 'gpath:save samplegfs:save)

(define (samplegfs:string gfs path)
  (let ((id (samplegfs-id gfs)))
    (stringout "samplegfs://"
      (if (symbol? id) (symbol->string id)
	  (if (uuid? id) (uuid->string id)
	      "??"))
      "/" (if (not (has-prefix path "/")) "/" )
      path)))
(kno/handler! 'samplegfs 'gpath:string samplegfs:string)

;;;; Copying

(define guess-encoding #t)
(varconfig! gpath:guess-encoding guess-encoding)

(define charset-patterns
  {#({";" (isspace)} (spaces*) "character-encoding:" (spaces*) (label charset (not> ";")))
   #((bol) (spaces* )"@charset " (label charset (not (eol))))})

(define (gp/copy! from (to #f) (opts #f))
  (let ((fetched (gp/fetch+ from)))
    (if fetched
	(let* ((dest (if to
			 (if (gp/location? to)
			     (gp/mkpath to (gp/basename from))
			     (->gpath to))
			 (gp/mkpath (getcwd) (gp/basename from))))
	       (ctype (getopt opts 'content-type (get fetched 'content-type)))
	       (charset (getopt opts 'charset
				(and (exists? ctype) ctype (has-prefix ctype "text/")
				     (or (get-charset ctype)
					 (content->charset (get fetched 'content)))))))
	  (gp/save! dest (get fetched 'content) ctype
		    (if charset
			(cons `#[charset ,charset] opts)
			opts)))
	(irritant from |GPATH/Not Found| gp/copy!
		  "The location " (gp/string from) " couldn't be fetched"))))

(define (content->charset content)
  (and (string? content) guess-encoding
       (singleton (get (text->frames charset-patterns content)
		       'charset))
       "utf-8"))

(define (gp/copy*! from into (opts #f) (prefix #f) (%loglevel) (force))
  (default! %loglevel (getopt opts 'loglevel %notice%))
  (default! force (getopt opts 'force #f))
  (set! from (->gpath from))
  (set! into (->gpath into))
  (unless prefix 
    (lognotice "Copying tree from " 
      (gpath->string from) " into " (gpath->string into))
    (set! prefix (gp/rootpath from)))
  (if force
      (force-copy*! from into opts prefix %loglevel)
      (smart-copy*! from into opts prefix %loglevel)))

(define (force-copy*! from into opts prefix %loglevel)
  (let ((contents (gp/list from)))
    (loginfo |GP/COPY*/force| 
      "Copying " (choice-size contents) " items from " (gpath->string from)
      "\n  into " (gpath->string into)
      "\n  stripping " prefix)
    (do-choices (src contents)
      (if (gp/location? src)
	  (force-copy*! src into opts prefix %loglevel)
	  (rel-copy! src into prefix %loglevel)))))

(define (smart-copy*! from into opts prefix %loglevel)
  (let* ((destroot (gp/mkpath into (strip-prefix (gp/rootpath from) prefix)))
	 (frominfo (gp/list+ from))
	 (destinfo (gp/list+ destroot)))
    (loginfo |GP/COPY*/smart| 
      "Copying " (choice-size frominfo) " items from " (gpath->string from)
      "\n  into " (gpath->string into)
      "\n  stripping " prefix)
    (do-choices (src frominfo)
      (if (gp/location? (get src 'gpath))
	  (smart-copy*! (get src 'gpath) into opts prefix %loglevel)
	  (let ((toinfo (pick destinfo 'relpath (get src 'relpath))))
	    (if (and (exists? toinfo) 
		     (or (test toinfo 'hash (get src 'hash))
			 (test toinfo 'md5 (get src 'md5))))
		(logdebug |GP/COPY*/smart| 
		  "Skipping redundant copy of " (gp>s src) 
		  "\n  into " (gp>s into))
		(rel-copy! (get src 'gpath) into prefix %loglevel)))))))

(define (rel-copy! src into prefix %loglevel)
  (logdebug |GP/COPY*/rel| 
    "Copying " (gp>s src) 
    "\n  into " 
    (gp>s (gp/mkpath into (strip-prefix (gp/rootpath src) prefix)))
    "\n  stripping " prefix)
  (gp/copy! src (gp/mkpath into (strip-prefix (gp/rootpath src) prefix))))

;;;; Writing dtypes to gpaths

(defambda (dtype->gpath dtype gpath)
  (do-choices gpath
    (let ((gp (if (string? gpath) (->gpath gpath) gpath)))
      (gp/save! gp (dtype->packet (qc dtype)) "application/dtype"))))

(define (gpath->dtype gpath)
  (let* ((gp (if (string? gpath) (->gpath gpath) gpath))
	 (fetched (gp/fetch gp)))
    (packet->dtype fetched)))

