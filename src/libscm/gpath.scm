;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc. All rights reserved.

(in-module 'gpath)
(define %used_modules '{varconfig ezrecords})

(module-export!
 '{gp/write! gp/save!
   writeout writeout/type
   gp/writeout gp/writeout! gp/writeout+!
   gpath? ->gpath uri->gpath gp/localpath?
   gp/location? gp/location gp/basename gp/rootpath
   gp/has-suffix gp/has-prefix
   gp/fetch gp/fetch+ gp/etag gp/info
   gp/exists? gp/exists gp/modified gp/newer 
   gp/list gp/list+
   gp/path gp/mkpath gp/subpath gp/makepath
   gpath->string gp/string gp>s gpath->location
   gp:config gpath/handler
   gp/urlfetch gp/urlinfo
   dtype->gpath gpath->dtype
   datauri/fetch+ datauri/info
   gp/copy! gp/copy*!})
(module-export! '{zip/gopen zip/gclose})

;;; This is a generic path facility (it grew out of the savecontent
;;; module, which still exists for legacy and historical reasons).  A
;;; gpath is just a CONS (for now) of a root and a relative path.
;;; Currently the root can be a directory name, an S3 location, or a
;;; zip file.

(define havezip #f)

(cond ((onerror (get-module 'ziptools) #f)
       (use-module 'ziptools)
       (set! havezip #t))
      (else 
       (define (zipfile? x) #f)
       (define (zip/add! . args) #f)))

(use-module '{fileio aws/s3 varconfig logger webtools 
	      texttools mimetable ezrecords hashfs zipfs})
(define-init %loglevel %notice%)

(define gp/urlsubst {})
(varconfig! gp:urlsubst gp/urlsubst #f)

(defrecord memfile mimetype content modified (hash #f))

(defrecord gpath-handler name get (save #f)
  (tostring #f) (fromstring #f) (fromuri #f) (rootpath #f) (prefixes {}))
(define gpath/handler cons-gpath-handler)

(define-init gpath-handlers (make-hashtable))

(config-def! 'GPATH:HANDLERS
	     (lambda (var (val))
	       (if (bound? val)
		   (if (gpath-handler? val)
		       (store! gpath-handlers (gpath-handler-name val)
			       val)
		       (irritant val |BadGPathHandler|))
		   (get gpath-handlers (getkeys gpath-handlers)))))

(define-init urish?
  (lambda (x)
    (and (string? x)
	 (string-starts-with? x #/^[a-zA-Z.]{2,32}:/))))

(define simple-gpath-prefixes {"s3:" "http:" "https:" "dropbox:"})

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

(define (checkdir dirpath)
  (unless (string-starts-with? dirpath #((isalnum+) ":"))
    (mkdirs dirpath))
  (when (has-prefix dirpath "file:") (mkdirs (subseq dirpath 5)))
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

(define *default-dirmode* 0x775) ;; rwxrwxr_x
(varconfig! gpath:dirmode *default-dirmode*)

(define (root-path path)
  (if (has-prefix path "/") (slice path 1)
      (if (has-prefix path "./") (slice path 2)
	  path)))

(define datauri-pat
  #("data:" (label content-type (not> ";")) ";" (label enc (not> ",")) ","
    (label content (rest)))  )
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
      (if (s3loc? gpath) (s3loc-path gpath)
	  (if (pair? gpath) (cdr gpath)
	      ""))))

(defambda (gp/save! dest content (ctype #f) (charset #f) (opts #f) (encoding #f))
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
	    (set! dest (try (->s3loc dest) (uri->gpath dest) dest)))
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
		((s3loc? dest) (s3/write! dest content ctype headers))
		((and (pair? dest) (hashfs? (car dest)))
		 (hashfs/save! (car dest) (cdr dest) content ctype))
		((and (pair? dest) (zipfs? (car dest)))
		 (zipfs/save! (car dest) (cdr dest) content ctype))
		((and (pair? dest) (hashtable? (car dest)))
		 (store! (car dest) (cdr dest)
			 (cons-memfile ctype content (timestamp)
				       (packet->base16 (md5 content)))))
		((and (pair? dest) (string? (car dest)) (string? (cdr dest)))
		 (write-file (mkpath (car dest) (cdr dest)) content))
		((and (pair? dest)
		      (s3loc? (car dest))
		      (string? (cdr dest)))
		 (s3/write! (s3/mkpath (car dest) (cdr dest)) content ctype (qc headers)))
		((and havezip (pair? dest)
		      (zipfile? (car dest))
		      (string? (cdr dest)))
		 (zip/add! (car dest) (cdr dest) content))
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

(define (string->root string)
  (cond ((has-prefix string "s3:") (->s3loc string))
	((has-prefix string "zip:")
	 (zip/open (subseq string 4)))
	(else (uribase string))))

(define (gp/basename path)
  (if (and (string? path) (has-prefix path simple-gpath-prefixes))
      (basename path)
      (begin
	(when (urish? path) (set! path (->gpath path)))
	(cond ((and (pair? path) (null? (cdr path)))
	       (gp/basename (car path)))
	      ((pair? path) (gp/basename (cdr path)))
	      ((s3loc? path) (basename (s3loc-path path)))
	      ((and (string? path) (has-prefix path "data:"))
	       (glom "data-" (packet->base16 (md5 path))
		 (try (ctype->suffix (get (datauri/info path) 'content-type) ".")
		      "")))
	      ((string? path) (basename path))
	      (else "")))))

(define (gp/rootpath path)
  (if (urish? path) (set! path (->gpath path)))
  (cond ((pair? path)
	 (mkpath (gp/rootpath (car path))
		 (strip-prefix (cdr path) "/")))
	((s3loc? path) (s3loc-path path))
	((or (zipfs? path) (zipfile? path)) "")
	((and (compound-type? path) (test gpath-handlers (compound-tag path)))
	 (if (gpath-handler-rootpath (get gpath-handlers (compound-tag path)))
	     ((gpath-handler-rootpath (get gpath-handlers (compound-tag path)))
	      path)
	     ""))
	((and (string? path) (has-prefix path "data:")) 
	 (gp/basename path))
	((and (string? path) (file-directory? path)) (mkpath path ""))
	((string? path) path)
	(else "")))

(define (gp/location? path)
  (when (urish? path) (set! path (string->root path)))
  (cond ((string? path) (has-suffix path "/"))
	((and (pair? path) (string? (cdr path)))
	 (has-suffix (cdr path) "/"))
	((and (s3loc? path) (string? (s3loc-path path)))
	 (has-suffix (s3loc-path path) "/"))
	((zipfile? path) #t)
	(else #f)))

(define (gp/location path)
  (when (urish? path) (set! path (->gpath path)))
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
	((and (string? path) (has-prefix path {"/" "~"})) (dirname path))
	((and (string? path) (has-prefix path {"ftp:" "http:" "https:"}))
	 (if (has-suffix path "/") path (dirname path)))
	((and (string? path) (position #\/ path))
	 (mkpath (getcwd) (dirname path)))
	((string? path) (mkpath (getcwd) path))
	(else path)))

(define (gpath->location path)
  (when (urish? path) (set! path (->gpath path)))
  (if (gp/location? path) path (gp/location path)))

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
	((and (pair? path) (zipfs? (car path)) (string? (cdr path)))
	 (zipfs/string (car path) (cdr path)))
	((and (compound-type? path) (test gpath-handlers (compound-tag path)))
	 ((gpath-handler-tostring (get gpath-handlers (compound-tag path)))
	  path #f))
	((and (pair? path) (string? (cdr path)) (compound-type? (car path))
	      (test gpath-handlers (compound-tag (car path))))
	 ((gpath-handler-tostring (get gpath-handlers (compound-tag (car path))))
	  (car path) (cdr path)))
	((s3loc? path) (s3loc->string path))
	((and (pair? path) (s3loc? (car path)))
	 (s3loc->string (s3/mkpath (car path) (cdr path))))
	((and (pair? path) (zipfile? (car path)))
	 (stringout "zip:" (zip/filename (car path)) "(" (cdr path) ")"))
	(else (stringout path))))
(define gp/string gpath->string)
(define gp>s gpath->string)

(define (makepath root path (mode *default-dirmode*) (require-subpath #f))
  (unless (string? path)
    (set! path 
	  (cond ((uuid? path) (glom "u:" (uuid->string path)))
		((packet? path) (glom "p:" (packet->base16 path)))
		((timestamp? path) (glom "t:" (get path 'iso)))
		((number? path) (number->string path))
		((gpath? path) (gp/basename path))
		(else (irritant path |BadPathElement| gp/makepath)))))
  (when (and (pair? root) (null? (cdr root)))
    (set! root (cons (car root) "")))
  (if (string? root) (set! root (string->root root))
      (if (and (pair? root) (string? (car root)))
	  (set! root (cons (string->root (car root)) (cdr root)))))
  (cond ((or (not path) (null? path)) root)
	((and require-subpath (not (string? path)))
	 (error |TypeError| MAKEPATH "Relative path is not a string" path))
	((or (s3loc? path) (zipfile? path) 
	     (hashtable? path) (hashfs? path)
	     (zipfs? path)
	     (and (pair? path) (string? (cdr path))
		  (or (s3loc? (car path)) (zipfile? (car path))
		      (hashtable? (car path)) (hashfs? (car path))
		      (zipfs? (car path)))))
	 path)
	((not (string? path))
	 (error |TypeError| MAKEPATH "Relative path is not a string" path))
	((and (not require-subpath)
	      (has-prefix path {"http:" "s3:" "https:" "~"
				"HTTP:" "S3:" "HTTPS:"}))
	 (->gpath path))
	((and (pair? root) (has-prefix path "/")) (cons (car root) path))
	((s3loc? root) (s3/mkpath root path))
	((zipfile? root) (cons root path))
	((hashtable? root) (cons root path))
	((hashfs? root) (cons root path))
	((zipfs? root) (cons root path))
	((and (compound-type? root) (test gpath-handlers (compound-tag root)))
	 (cons root path))
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
(define (gp/subpath root path . more)
  (let ((result (makepath (->gpath root) path *default-dirmode* #t)))
    (if (null? more) result
	(if (not (car more))
	    (if (null? (cdr more)) result
		(apply gp/subpath result (cdr more)))
	    (apply gp/subpath result (car more) (cdr more))))))

(define (gp/fetch ref (ctype #f) (opts #f) (encoding #f))
  (if (urish? ref) (set! ref (->gpath ref)))
  (when (and (table? ctype) (not opts))
    (set! opts ctype)
    (set! ctype #f))
  (unless ctype (set! ctype (guess-mimetype (get-namestring ref) #f opts)))
  (unless encoding 
    (set! encoding (getopt opts 'content-encoding (path->encoding (get-namestring ref)))))
  (loginfo |GP/FETCH| ref " expecting " ctype " given " (pprint opts))
  (cond ((s3loc? ref) (s3/get ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (zip/get (car ref) (root-path (cdr ref))
		  (or (not ctype) (not (has-prefix ctype "text")))))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (memfile-content (get (car ref) (cdr ref))))
	((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	 (hashfs/get (car ref) (cdr ref)))
	((and (pair? ref) (zipfs? (car ref)) (string? (cdr ref)))
	 (zipfs/get (car ref) (cdr ref) opts))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	  (car ref) (cdr ref) (if ctype (cons `#[content-type ,ctype] opts) opts)))
	((pair? ref) (gp/fetch (gp/path (car ref) (cdr ref))))
	((and (string? ref)
	      (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (set! ref (try (->s3loc ref) (uri->gpath ref) ref))
	 (if (string? ref)
	     (get (gp/urlfetch ref) 'content)
	     (get (gp/fetch ref) 'content)))
	((and (string? ref) (has-prefix ref "data:"))
	 (get (datauri/fetch+ ref) 'content))
	((and (string? ref) (has-prefix ref "s3:")) (s3/get (->s3loc ref)))
	((and (string? ref) (string-starts-with? ref #((isalnum+) ":")))
	 (irritant ref |Bad gpath scheme|))
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

(define (gp/urlfetch url (err #t) (max-redirects #t))
  (let* ((newurl (textsubst url (qc gp/urlsubst)))
	 (err (and err
		   (if (equal? url newurl)
		       (cons url (if (pair? err) err '()))
		       (cons* newurl (list url) (if (pair? err) err '())))))
	 (curlopts (and max-redirects `#[follow ,(if (number? max-redirects) max-redirects #t)]))
	 (response (urlget newurl curlopts))
	 (encoding (get response 'content-encoding))
	 (hash (md5 (get response 'content))))
    (if (and (test response 'response)
	     (<= 200 (get response 'response) 299)
	     (test response '%content))
	(frame-create #f
	  'gpath url 'gpathstring url 'rootpath (uripath url)
	  'content-type (get response 'content-type)
	  'content-length (length (get response 'content))
	  'last-modified (get response 'last-modified)
	  'content-encoding (get response 'content-encoding)
	  'etag (get response 'etag) 
	  'hash hash 'md5 (packet->base16 hash)
	  'content-encoding  (get response 'content-encoding)
	  'content (get response '%content))
	(tryif err
	  (error URLFETCH_FAILED GP/URLFETCH
		 url response)))))

(define (gp/urlinfo url (err #t) (max-redirects #t))
  (let* ((newurl (textsubst url (qc gp/urlsubst)))
	 (err (and err
		   (if (equal? url newurl)
		       (cons url (if (pair? err) err '()))
		       (cons* newurl (list url) (if (pair? err) err '())))))
	 (curlopts (and max-redirects `#[follow ,(if (number? max-redirects) max-redirects #t)]))
	 (response (urlhead newurl curlopts)))
    response))

(define (gp/fetch+ ref (ctype #f) (opts #f) (encoding))
  (if (urish? ref) (set! ref (->gpath ref)))
  (when (and (table? ctype) (not opts))
    (set! opts ctype)
    (set! ctype #f))
  (unless ctype (set! ctype (guess-mimetype (get-namestring ref) #f opts)))
  (default! encoding 
    (getopt opts 'content-encoding (path->encoding (get-namestring ref))))
  (loginfo |GP/FETCH+| ref " expecting " ctype " given " (pprint opts))
  (cond ((s3loc? ref) (s3/get+ ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (let ((realpath (root-path (cdr ref)))
	       (zip (car ref)))
	   (and (zip/exists? zip realpath)
		(let* ((ctype (or ctype 
				  (guess-mimetype (get-namestring ref) #f opts)))
		       (charset (and ctype (has-prefix ctype "text")
				     (or (ctype->charset ctype) #t)))
		       (content (zip/get zip realpath (not charset)))
		       (hash (md5 content)))
		  (frame-create #f
		    'gpath (->gpath ref) 
		    'gpathstring (gpath->string (->gpath ref))
		    'rootpath (gp/rootpath ref)
		    'content (if (string? charset)
				 (packet->string content charset)
				 content)
		    'content-length (length content)
		    'content-type (or ctype {})
		    'charset
		    (if (string? charset) charset (if charset "utf-8" {}))
		    'last-modified
		    (tryif (bound? zip/modtime) (zip/modtime zip realpath))
		    'md5 (packet->base16 hash) 'hash hash)))))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
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
	((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	 (hashfs/get+ (car ref) (cdr ref)))
	((and (pair? ref) (zipfs? (car ref)) (string? (cdr ref)))
	 (zipfs/get+ (car ref) (cdr ref)))
	((and (pair? ref) (compound-type? (car ref))
	      (test gpath-handlers (compound-tag (car ref))))
	 ((gpath-handler-get (get gpath-handlers (compound-tag (car ref))))
	  (car ref) (cdr ref) (if ctype (cons `#[content-type ,ctype] opts) opts)
	  #t))
	((pair? ref) (gp/fetch+ (gp/path (car ref) (cdr ref)) ctype))
	((and (string? ref) (exists has-prefix ref {"http:" "https:" "ftp:"}))
	 (set! ref (try (->s3loc ref) (uri->gpath ref) ref))
	 (if (string? ref) (gp/urlfetch ref) (gp/fetch+ ref)))
	((and (string? ref) (has-prefix ref "data:"))
	 (datauri/fetch+ ref))
	((and (string? ref) (has-prefix ref "s3:")) (s3/get+ (->s3loc ref)))
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
  (cond ((s3loc? ref) (s3/info ref))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (let ((realpath (root-path (cdr ref)))
	       (zip (car ref)))
	   (and (zip/exists? zip realpath)
		(let* ((ctype (or ctype
				  (guess-mimetype (get-namestring ref) #f opts)))
		       (charset (and ctype (has-prefix ctype "text")
				     (or (ctype->charset ctype) #t)))
		       (content (if etag (zip/get zip realpath #t) #f))
		       (md5 (tryif content (md5 content))))
		  (frame-create #f 
		    'gpath (->gpath ref) 'gpathstring (gpath->string (->gpath ref))
		    'rootpath (gp/rootpath ref)
		    'content-type (or ctype {})
		    'charset
		    (if (string? charset) charset (if charset "utf-8" {}))
		    'last-modified 
		    (tryif (bound? zip/modtime) (zip/modtime zip realpath))
		    'content-length
		    (tryif (bound? zip/getsize) (zip/getsize zip realpath))
		    'hash md5 'md5 (packet->base16 md5))))))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
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
	((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	 (hashfs/info (car ref) (cdr ref)))
	((and (pair? ref) (zipfs? (car ref)) (string? (cdr ref)))
	 (zipfs/info (car ref) (cdr ref)))
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
	((and (string? ref) (has-prefix ref "s3:"))
	 (s3/info (->s3loc ref)))
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

(define (gp/modified ref)
  (if (urish? ref) (set! ref (->gpath ref)))
  (cond ((s3loc? ref) (s3/modified ref))
	((and (compound-type? ref) 
	      (test gpath-handlers (compound-tag ref)))
	 (get ((gpath-handler-get (get gpath-handlers (compound-tag ref)))
	       ref "" #f #f)
	      'last-modified))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (if (bound? zip/modtime)
	     (zip/modtime (car ref) (cdr ref))
	     (file-modtime (zip/filename (car ref)))))
	((and (pair? ref) (hashtable? (car ref)))
	 (memfile-modified (get (car ref) (cdr ref))))
	((and (pair? ref) (hashfs? (car ref)))
	 (get (hashfs/info (car ref) (cdr ref)) 'last-modified))
	((and (pair? ref) (zipfs? (car ref)))
	 (get (zipfs/info (car ref) (cdr ref)) 'last-modified))
	((and (pair? ref) (pair? (car ref)))
	 (gp/modified (gp/path (car ref) (cdr ref))))
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
	((and (string? ref) (has-prefix ref "s3:"))
	 (s3/modified (->s3loc ref)))
	((string? ref) (file-modtime ref))
	(else (error "Invalid GPATH" GP/MODIFIED #f ref))))

(define (gp/newer ref base)
  (if (urish? ref) (set! ref (->gpath ref)))
  (if (urish? base) (set! base (->gpath base)))
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
	((and (compound-type? ref) 
	      (test gpath-handlers (compound-tag ref)))
	 ((gpath-handler-get (get gpath-handlers (compound-tag ref)))
	  ref "" #f #f))
	((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	 (zip/exists? (car ref) (cdr ref)))
	((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	 (test (car ref) (cdr ref)))
	((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	 (exists? (hashfs/get+ (car ref) (cdr ref))))
	((and (pair? ref) (zipfs? (car ref)) (string? (cdr ref)))
	 (exists? (zipfs/info (car ref) (cdr ref))))
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
	((and (string? ref) (has-prefix ref "s3:"))
	 (s3/exists? (->s3loc ref)))
	((string? ref) (file-exists? ref))
	(else (error "Weird GPATH ref" GP/EXISTS? #f ref))))

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
	((and (pair? ref) (zipfs? (car ref)) (string? (cdr ref)))
	 (try (md5 (zipfs/get (car ref) (cdr ref))) #f))
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
	((and (string? ref) (has-prefix ref "s3:"))
	 (s3/etag (->s3loc ref) compute))
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
    (cond ((s3loc? ref) (filter-list (s3/list ref) matcher))
	  ((zipfile? ref) (filter-list (zip/getfiles ref) matcher))
	  ((hashfs? ref) (filter-list (hashfs/list ref) matcher))
	  ((zipfs? ref) (filter-list (zipfs/list ref) matcher))
	  ((and (pair? ref) (s3loc? (car ref)) (string? (cdr ref)))
	   (filter-list (s3/list (s3/mkpath (car ref) (mkpath (cdr ref) "")))
			matcher (cdr ref)))
	  ((and (pair? ref) (zipfile? (car ref)) (string? (cdr ref)))
	   (filter-list (zip/getfiles (car ref)) matcher (cdr ref)))
	  ((and (pair? ref) (hashtable? (car ref)) (string? (cdr ref)))
	   (filter-list (getkeys (car ref)) matcher (cdr ref)))
	  ((and (pair? ref) (hashfs? (car ref)) (string? (cdr ref)))
	   (filter-list (hashfs/list (car ref)) matcher (cdr ref)))
	  ((and (pair? ref) (zipfs? (car ref)) (string? (cdr ref)))
	   (filter-list (zipfs/list (car ref)) matcher (cdr ref)))
	  ((and (string? ref) (has-prefix ref "s3:"))
	   (gp/list (->s3loc ref)))
	  ((and (string? ref) (gp/localpath? ref))
	   (filter-list (choice (getfiles ref) (mkpath (getdirs ref) ""))
			matcher))
	  (else (irritant ref |NoListingMethod| )))))

(defambda (gp/list+ ref (matcher #f))
  (cond ((s3loc? ref)
	 (if matcher
	     (filter-info (s3/list+ ref) matcher)
	     (s3/list+ ref)))
	((zipfs? ref)
	 (if matcher
	     (zipfs/list+ ref #f matcher)
	     (zipfs/list+ ref)))
	((and (pair? ref) (s3loc? (car ref)))
	 (if matcher
	     (filter-info (s3/list+ (s3/mkpath (car ref) (cdr ref)))
			  matcher)
	     (s3/list+ (s3/mkpath (car ref) (cdr ref)))))
	((and (pair? ref) (zipfs? (car ref)))
	 (if matcher
	     (zipfs/list+ (car ref) (cdr ref) matcher)
	     (zipfs/list+ (car ref) (cdr ref))))
	(else (for-choices (item (gp/list ref matcher))
		(add-relpath (gp/info item) (gp/rootpath ref))))))

(define (add-relpath info prefix)
  (modify-frame info 'relpath 
		(strip-prefix (get info 'rootpath) prefix)))

;;; Recognizing and parsing GPATHs

(define (gpath? val)
  (if (pair? val)
      (and (string? (cdr val))
	   (or (s3loc? (car val)) (zipfile? (car val))
	       (hashfs? (car val)) (zipfs? (car val))
	       (and (compound-type? (car val))
		    (test gpath-handlers (compound-tag (car val))))))
      (if (string? val)
	  (and (not (position #\newline val))
	       (has-prefix val {"http:" "https:" "ftp:" "s3:" "/" "~"}))
	  (or (s3loc? val) (zipfile? val) (hashfs? val) (zipfs? val)
	      (and (compound-type? val)
		   (test gpath-handlers (compound-tag val)))))))

(define (->gpath val (root #f))
  (cond ((not (string? val)) val)
	((has-prefix val {"s3:" "S3:"}) (->s3loc val))
	((has-prefix val "/") (if root (gp/mkpath root val) val))
	((has-prefix val "ftp:") val)
	((has-prefix val {"http:" "https:"}) (try (->s3loc val) val))
	((has-prefix val "~") (abspath val))
	(root (gp/mkpath root val))
	(else (abspath val))))

(define (uri->gpath val)
  (if (and (string? val) (has-prefix val {"http:" "https:" "ftp:"}))
      (try (->s3loc val)
	   (try-choices (handler (get gpath-handlers (getkeys gpath-handlers)))
	     (tryif (gpath-handler-fromuri handler)
	       (difference ((gpath-handler-fromuri handler) val) #f))))
      (fail)))

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

(define (gp/localpath? gpath)
  (and (string? gpath)
       (string? (->gpath gpath))
       (not (has-prefix gpath {"ftp:" "http:" "https:"}))))

(define (gp:config spec) (->gpath spec))

;;; Opening zip files at GPATHs

(define-init ziptemps (make-hashtable))

(define (zip/gopen path)
  (if (and (string? path)
	   (not (string-starts-with? path #((isalnum+) ":")))
	   (file-exists? (abspath path)))
      (zip/open path)
      (let* ((path (->gpath path))
	     (content (gp/fetch path))
	     (temporary (tempdir))
	     (file (mkpath temporary (gp/basename path))))
	(write-file file content)
	(prog1 (zip/open file)
	  (store! ziptemps file temporary)))))

(define (zip/gclose zip)
  (let* ((filename (zip/filename zip))
	 (dir (get ziptemps filename)))
    (prog1 (zip/close zip)
      (tempdir/done dir))))


;;;; Example configuration

(defrecord samplegfs (name #f)
  (data (make-hashtable)) (metadata (make-hashtable)))
(define (samplegfs-get gfs path (opts #f) (info))
  (if (bound? info)
      (let ((metadata (get (samplegfs-metadata gfs) path)))
	(when info
	  (set! metadata (deep-copy metadata))
	  (store! metadata 'content (get (samplegfs-data gfs) path)))
	metadata)
      (get (samplegfs-data gfs) path)))
(define (samplegfs-save gfs path content (ctype #f) (opts #f) (charset #f))
  (default! ctype (guess-mimetype path content opts))
  (default! charset (get-charset ctype opts))
  (store! (samplegfs-data gfs) path content)
  (store! (samplegfs-metadata gfs) path
	  (frame-create #f
	    'content-type ctype
	    'charset (tryif charset (if (string? charset) charset "utf-8"))
	    'last-modified (timestamp) 'content-length (length content)
	    'hash (md5 content) 'md5 (packet->base16 (md5 content))
	    'etag (md5 content))))
(config! 'gpath:handlers
	 (gpath/handler 'samplegfs samplegfs-get samplegfs-save))

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

