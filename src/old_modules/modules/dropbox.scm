;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Dropbox
(in-module 'dropbox)

(use-module '{fdweb xhtml signature oauth varconfig
	      gpath texttools mimetable ezrecords
	      logger})

(module-export! '{dropbox/get dropbox/get/req dropbox/get+
		  dropbox/list dropbox/info dropbox/put!
		  dropbox/gpath dropbox:appname
		  dropbox-oauth})

(define-init %loglevel %warning%)

(defrecord dropbox oauth (root-path #f))

(define-init dropbox:appname "")
(varconfig! dropbox:appname dropbox:appname)

(define-init dropbox-root "sandbox")
(config-def!
 'dropbox:live
 (lambda (var (val))
   (if (bound? val)
       (if val (set! dropbox-root "dropbox") (set! dropbox-root "sandbox"))
       (if (identical? dropbox-root "sandbox") #f
	   (if (identical? dropbox-root "dropbox") #t
	       (begin (logwarn "Fixing invalid dropbox-root " dropbox-root)
		 (set! dropbox-root "sandbox")))))))

(define (add-suffix string suffix)
  (if (has-suffix string suffix) string (glom string suffix)))

(define (mkurl base oauth path (root))
  (glom base
    (mkpath
     (if (testopt oauth 'live)
	 (if (getopt oauth 'live #f) "dropbox" "sandbox")
	 dropbox-root)
     (if (and (testopt oauth 'root)
	      (not (empty-string? (getopt oauth 'root))))
	 (mkpath (getopt oauth 'root) (strip-prefix path "/"))
	 (strip-prefix path "/")))))

(define (dropbox/get/req oauth path (revision #f))
  (let ((endpoint (mkurl "https://api-content.dropbox.com/1/files/"
			  oauth path)))
    (if revision
	(oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
	(oauth/call oauth 'GET endpoint '() #f #f #t))))
(define (dropbox/get oauth path (revision #f))
  (let* ((endpoint (mkurl "https://api-content.dropbox.com/1/files/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '() #f #f #t)))
	 (status (get result 'response)))
    (loginfo |DROPBOX/GET| path " has status " status " given\n\t" (pprint oauth))
    (if (>= 299 status 200) (get result '%content)
	(if (= status 404)
	    (begin (lognotice |Dropbox404| "Dropbox call returned 404" result)
	      (fail))
	    (irritant result CALLFAILED DROPBOX/GET
		      path " with " oauth)))))
(define (dropbox/get+ oauth path (revision #f) (err #f))
  (let* ((endpoint (mkurl "https://api-content.dropbox.com/1/files/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '() #f #f #t)))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-dropbox-metadata))))
    (loginfo |DROPBOX/GET+| path " has status " status " given\n\t" (pprint oauth))
    (if (>= 299 status 200)
	(if (exists? metadata)
	    (begin (store! metadata 'content (get result '%content))
	      (store! metadata 'ctype (get result 'content-type))
	      (store! metadata 'modified (timestamp (get metadata 'modified)))
	      (store! metadata 'length (string->number (get metadata 'size)))
	      (store! metadata 'length
		      (get (text->frame
			    #((label bytes (isdigit+) #t) (spaces) "bytes")
			    (get metadata 'size))
			   'bytes))
	      metadata)
	    (frame-create #f
	      'content (get result '%content)
	      'ctype (get result 'content-type)
	      'modified (get result 'modified)
	      'etag (get result 'etag)))
	(if (= status 404)
	    (begin (lognotice |Dropbox404| "Dropbox call returned 404" result)
	      (fail))
	    (irritant result CALLFAILED DROPBOX/GET+
		      path " with " oauth)))))

(define (dropbox/info oauth path (revision #f) (error #f))
  (let* ((endpoint (mkurl "https://api.dropbox.com/1/metadata/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '() #f #f #t)))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-dropbox-metadata))))
    (loginfo |DROPBOX/INFO| path " has status " status " given\n\t" (pprint oauth))
    (if (>= 299 status 200)
	(let ((parsed (jsonparse (get result '%content))))
	  (debug%watch "DROPBOX/INFO" parsed result)
	  (store! parsed 'ctype (get parsed 'mime_type))
	  (store! parsed 'modified (timestamp (get parsed 'modified)))
	  (store! parsed 'length
		  (get (text->frame #((label bytes (isdigit+) #t) (spaces) "bytes")
				    (get parsed 'size))
		       'bytes))
	  parsed)
	(if (= status 404) #f
	    (and (or error (not (<= 400 status 500)))
		 (irritant result CALLFAILED DROPBOX/INFO
			   path " with " oauth))))))

(define (dropbox/list oauth path (revision #f))
  (let* ((endpoint (mkurl "https://api.dropbox.com/1/metadata/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint
				 `("list" "true" rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '("list" "true") #f #f #t)))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-dropbox-metadata))))
    (loginfo |DROPBOX/LIST| path " has status " status " given\n\t" (pprint oauth))
    (if (>= 299 status 200)
	(jsonparse (get result '%content))
	(irritant result CALLFAILED DROPBOX/INFO
		  path " with " oauth))))

(define (dropbox/put! oauth path content (ctype #f) (revision #f))
  (unless ctype
    (set! ctype
	  (path->mimetype
	   path (if (packet? content) "application" "text"))))
  (loginfo |DROPBOX/PUT!| "Saving " (length content) " of " ctype " to " path
	   " given\n\t" (pprint oauth))
  (let* ((endpoint
	  (mkurl "https://api-content.dropbox.com/1/files_put/"
		  oauth path))
	 (result (oauth/call oauth 'put endpoint '() content ctype))
	 (status (get result 'response)))
    result))

;;;; GPATH handling

(define (dropbox-get dropbox path (opts #f) (info) (auth))
  (set! auth (getopt opts 'oauth (dropbox-oauth dropbox)))
  (if (bound? info)
      (if info
	  (dropbox/get+ auth path)
	  (dropbox/info auth path))
      (dropbox/get auth path)))

(define (dropbox-save dropbox path content (ctype #f) (opts #f)
		      (charset) (fullpath))
  (unless ctype
    (set! ctype
	  (path->mimetype path (if (packet? content) "application" "text")
			  opts)))
  (set! fullpath
	(if (dropbox-root-path dropbox)
	    (mkpath (dropbox-root-path dropbox) path)
	    path))
  (dropbox/put! (getopt opts 'oauth (dropbox-oauth dropbox))
		fullpath content ctype))

(define (dropbox-pathstring dropbox path)
  (let ((oauth (dropbox-oauth dropbox))
	(root (try (reject (dropbox-root-path dropbox) empty-string?) #f)))
    (glom "dropbox://"
      (getopt oauth 'email (getopt oauth 'remoteid)) ":" 
      (getopt oauth 'appname "*") 
      (and root ":") (and root (string-subst root "/" ":"))
      "/" (strip-prefix path "/"))))

(define (dropbox/gpath spec (path ""))
  (cons-dropbox spec (or path "")))

(config! 'gpath:handlers
	 (gpath/handler 'dropbox
			(lambda args (apply dropbox-get args))
			(lambda args (apply dropbox-save args))
			(lambda (root path) (dropbox-pathstring root path))))

