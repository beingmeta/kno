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
   gp/urlfetch gp/urlinfo
   dtype->gpath gpath->dtype
   datauri/fetch+ datauri/info
   gp/copy! gp/copy*!})

;;; This is a generic path facility (it grew out of the savecontent
;;; module, which still exists for legacy and historical reasons).  A
;;; gpath is just a CONS (for now) of a root and a relative path.

(use-module '{net/mimetable ezrecords})

(define gp/urlsubst {})
(varconfig! gp:urlsubst gp/urlsubst #f)

(defrecord memfile mimetype content modified (hash #f))

;; (defrecord gpath-handler name get (save #f)
;;   (tostring #f) (fromstring #f) (fromuri #f) (rootpath #f) (prefixes {}))
;; (define gpath/handler cons-gpath-handler)

;; (define-init gpath-handlers (make-hashtable))

;; (config-def! 'GPATH:HANDLERS
;; 	     (lambda (var (val))
;; 	       (if (bound? val)
;; 		   (if (gpath-handler? val)
;; 		       (store! gpath-handlers (gpath-handler-name val)
;; 			       val)
;; 		       (irritant val |BadGPathHandler|))
;; 		   (get gpath-handlers (getkeys gpath-handlers)))))

(define-init simple-gpath-prefixes {"http:" "https:"})
(varconfig! gpath:prefixes:simple simple-gpath-prefixes #f choice)

(define-init gpath-prefixes (make-hashtable))
(store! gpath-prefixes '{http https} #t)

(define-init *gpath-tags* {})

(define gpath-pattern
  `#((label scheme (isalnum+) #t) ":" 
     (opt (label domain #("//" (not> "/") "/")))
     (label path (rest))))

(define (gpath-root? x)
  (or (hashtable? x)
      (and (compound? x *gpath-tags*))
      (if (compound? x)
	  (and (handles? x 'gpath:exists)
	       (begin (set+! *gpath-tags* (compound-tag x))
		 #t))
	  (handles? x 'gpath:exists))))
(define (gpath? x)
  (cond ((not (string? x)) (gpath-root? x))
	((has-prefix x {"http:" "https:" "ftp:" "data:"}) x)
	((position #\: x)
	 (test gpath-prefixes (string->symbol (downcase (slice x 0 (position #\: x))))))
	(else (not (textsearch '(isspace) x)))))

(define (->gpath val (root #f) (opts #f))
  (cond ((and (string? val) (has-prefix val {"http:" "https:" "ftp:" "data:"})) val)
	((string? val)
	 (let* ((parsed (text->frame gpath-pattern val))
		(scheme (get parsed 'scheme))
		(domain (get parsed 'domain))
		(path (get parsed 'path))
		(handler (get gpath-prefixes scheme)))
	   (cond ((eq? handler #t) val)
		 ((fail? handler) (irritant val |Bad GPath|))
		 (else (handler val opts root)))))
	((gpath-root? val) val)
	((and (pair? val) (string? (cdr val)) (gpath-root? (car val))) val)
	(else (irritant val '|NotGPath|))))

(define-init urish?
  (lambda (x)
    (and (string? x)
	 (string-starts-with? x #/^[a-zA-Z.]{2,32}:/))))

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
  (if (not (string? path)) path
      (if (has-prefix path "/") (slice path 1)
	  (if (has-prefix path "./") (slice path 2)
	      path))))

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

(define (string->root string)
  (cond ((not (string? string)) (irritant string |GPathNotString|))
	(else (uribase string))))

(define (gp/basename path)
  (if (and (string? path) (has-prefix path simple-gpath-prefixes))
      (basename path)
      (begin
	(when (urish? path) (set! path (->gpath path)))
	(cond ((and (pair? path) (null? (cdr path)))
	       (gp/basename (car path)))
	      ((pair? path) (gp/basename (cdr path)))
	      ((and (string? path) (has-prefix path "data:"))
	       (glom "data-" (packet->base16 (md5 path))
		 (try (ctype->suffix (get (datauri/info path) 'content-type) ".")
		      "")))
	      ((string? path) (basename path))
	      (else "")))))

(define (gp/rootpath path)
  (set! path (->gpath path))
  (if (urish? path) (set! path (->gpath path)))
  (cond ((pair? path)
	 (mkpath (gp/rootpath (car path))
		 (strip-prefix (cdr path) "/")))
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
  (set! path (->gpath path))
  (cond ((string? path) (has-suffix path "/"))
	((and (pair? path) (string? (cdr path)))
	 (has-suffix (cdr path) "/"))
	(else #f)))

(define (gp/location path)
  (set! path (->gpath path))
  (cond ((and (pair? path) (or (null? (cdr path)) (empty-string? (cdr path))))
	 (car path))
	((and (pair? path) (string? (cdr path)) (has-suffix (cdr path) "/"))
	 (gp/mkpath (car path) (slice (cdr path) 0 -1)))
	((and (pair? path) (string? (cdr path)) (position #\/ (cdr path)))
	 (gp/mkpath (car path) (dirname (cdr path))))
	((pair? path) (car path))
	((and (string? path) (has-prefix path {"/" "~"})) (dirname path))
	((and (string? path) (has-prefix path {"ftp:" "http:" "https:"}))
	 (if (has-suffix path "/") path (dirname path)))
	((and (string? path) (position #\/ path))
	 (mkpath (getcwd) (dirname path)))
	((string? path) (mkpath (getcwd) path))
	(else path)))

(define (gpath->location path)
  (set! path (->gpath path))
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
	((and (pair? path) (string? (cdr path))
	      (or (compound? (car path) *gpath-tags*)
		  (gpath-root? (car path))))
	 (kno/dispatch (car path) 'gpath:string (cdr path)))
	((or (compound? path *gpath-tags*) (gpath-root? path))
	 (kno/dispatch (car path) 'gpath:string #f))
	(else (irritant path |NotGPath|))))
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
	((or (hashtable? path)
	     (and (pair? path) (string? (cdr path))
		  (hashtable? (car path))))
	 path)
	((not (string? path))
	 (error |TypeError| MAKEPATH "Relative path is not a string" path))
	((and (not require-subpath)
	      (has-prefix path {"http:" "https:" "~"
				"HTTP:" "HTTPS:"}))
	 (->gpath path))
	((and (pair? root) (has-prefix path "/")) (cons (car root) path))
	((hashtable? root) (cons root path))
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
(define (gp/subpath root path . more)
  (let ((result (makepath (->gpath root) path *default-dirmode* #t)))
    (if (null? more) result
	(if (not (car more))
	    (if (null? (cdr more)) result
		(apply gp/subpath result (cdr more)))
	    (apply gp/subpath result (car more) (cdr more))))))

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
	 (kno/dispatch (car ref) 'gpath:get (cdr ref)))
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

;;; Recognizing and parsing GPATHs

(define (gpath? val)
  (if (pair? val)
      (and (string? (cdr val))
	   (and (compound-type? (car val))
		(test gpath-handlers (compound-tag (car val)))))
      (if (string? val)
	  (and (not (position #\newline val))
	       (has-prefix val {"http:" "https:" "ftp:" "/" "~"}))
	  (and (compound-type? val)
	       (test gpath-handlers (compound-tag val))))))

(define gpath-scheme-pattern
  `#((label scheme (isalnum+)) ":" (label path (rest))))

(define-init gpath-scheme-handlers (make-hashtable))
(define-init gpath-uri-handlers '())

(define (->gpath val (root #f) (chroot #f))
  (if (and (string? val) (textmatch gpath-scheme-pattern val))
      (let* ((match (text->frame gpath-scheme-pattern val))
	     (scheme (try (get match 'scheme) #f))
	     (path (try (get match 'path) "")))
	(cond ((equal? scheme "ftp") val)
	      ((overlaps? scheme {"http" "https"})
	       (let ((found #f))
		 (dolist (handler gpath-uri-handlers)
		   (set! found (handler val))
		   (when found (break)))
		 (or found val)))
	      ((and scheme (test gpath-scheme-handlers scheme)
		    ((get gpath-scheme-handlers scheme) val))
	       (->gpath ((get gpath-scheme-handlers scheme) val) root chroot))
	      ((string? (config scheme))
	       (->gpath (mkpath (config scheme) path) root chroot))
	      ((config scheme) (cons scheme path))
	      (else (irritant scheme |InvalidScheme(gpath)| "For " val))))
      (if (not (string? val))
	  val ;; (irritant val |Not a path| ->gpath)
	  (if (has-prefix val "/") 
	      (if chroot (gp/mkpath chroot (slice val 1)) val)
	      (if (has-prefix val {"~" "./" "../"})
		  (abspath val)
		  (if (string? root)
		      (mkpath root val)
		      (if root (cons root val) (abspath val))))))))

(define (uri->gpath val)
  (if (and (string? val) (has-prefix val {"http:" "https:" "ftp:"}))
      (try (try-choices (handler (get gpath-handlers (getkeys gpath-handlers)))
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

;;; URLINFO

(define (gp/urlfetch url (err #t) (max-redirects #t))
  (let* ((newurl (textsubst url (qc gp/urlsubst)))
	 (err (and err
		   (if (equal? url newurl)
		       (cons url (if (pair? err) err '()))
		       (cons* newurl (list url) (if (pair? err) err '())))))
	 (curlopts (and max-redirects
			`#[follow ,(if (number? max-redirects) max-redirects #t)]))
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
	 (curlopts (and max-redirects
			`#[follow ,(if (number? max-redirects) max-redirects #t)]))
	 (response (urlhead newurl curlopts)))
    response))

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

