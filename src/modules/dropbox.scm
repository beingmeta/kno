;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Dropbox
(in-module 'dropbox)

(use-module '{fdweb xhtml signature oauth
	      gpath texttools mimetable ezrecords
	      logger})

(module-export! '{dropbox/get dropbox/get/req dropbox/get+
		  dropbox/list dropbox/info dropbox/put!
		  dropbox/gpath
		  dropbox-oauth})

(define %loglevel %warning%)

(defrecord dropbox oauth (root-path #f))

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

(define (db/url base oauth path (root))
  (glom base
    (getopt oauth 'root
	    (if (testopt oauth 'live)
		(if (getopt oauth 'live #f) "dropbox" "sandbox")
		dropbox-root))
    (if (has-prefix path "/") #f "/")
    path))

(define (dropbox/get/req oauth path (revision #f))
  (let ((endpoint (db/url "https://api-content.dropbox.com/1/files/"
			  oauth path)))
    (if revision
	(oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
	(%wc oauth/call oauth 'GET endpoint '() #f #f #t))))
(define (dropbox/get oauth path (revision #f))
  (let* ((endpoint (db/url "https://api-content.dropbox.com/1/files/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '() #f #f #t)))
	 (status (get result 'response)))
    (if (>= 299 status 200) (get result '%content)
	(if (= status 404)
	    (begin (lognotice |Dropbox404| "Dropbox call returned 404" result)
	      (fail))
	    (irritant result CALLFAILED DROPBOX/GET
		      path " with " oauth)))))
(define (dropbox/get+ oauth path (revision #f) (err #f))
  (let* ((endpoint (db/url "https://api-content.dropbox.com/1/files/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '() #f #f #t)))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-dropbox-metadata))))
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
  (let* ((endpoint (db/url "https://api.dropbox.com/1/metadata/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '() #f #f #t)))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-dropbox-metadata))))
    (if (>= 299 status 200)
	(let ((parsed (jsonparse (get result '%content))))
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
  (let* ((endpoint (db/url "https://api.dropbox.com/1/metadata/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint
				 `("list" "true" rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '("list" "true") #f #f #t)))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-dropbox-metadata))))

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
	  (db/url "https://api-content.dropbox.com/1/files_put/"
		  oauth path))
	 (result (oauth/call oauth 'put endpoint '() content ctype))
	 (status (get result 'response)))
    result))

;;;; GPATH handling

(define (dropbox-get dropbox path (info))
  (if (bound? info)
      (if info
	  (dropbox/get+ (dropbox-oauth dropbox) path)
	  (dropbox/info (dropbox-oauth dropbox) path))
      (dropbox/get (dropbox-oauth dropbox) path)))

(define (dropbox-save dropbox path content (ctype) (charset) (fullpath))
  (default! ctype
    (path->mimetype
     path (if (packet? content) "application" "text")))
  (set! fullpath
	(if (dropbox-root-path dropbox)
	    (mkpath (dropbox-root-path dropbox) path)
	    path))
  (dropbox/put! (dropbox-oauth dropbox) fullpath content ctype))

(define (dropbox-pathstring dropbox path)
  (let ((oauth (dropbox-oauth dropbox))
	(root (dropbox-root-path dropbox)))
    (glom "dropbox://"
      (getopt oauth 'email) ":" (getopt oauth 'remoteid)
      "/" (if (and path root) (mkpath root path)
	      (or root path)))))

(define (dropbox/gpath spec (path))
  (default! path (getopt spec 'rootpath))
  (when (string-starts-with? path #("/Apps/" (not> "/") "/"))
    (set! path (textsubst path #((bos) "/Apps/" (not> "/") "/") "")))
  (cons-dropbox spec path))

(config! 'gpath:handlers
	 (gpath/handler 'dropbox
			(lambda args (apply dropbox-get args))
			(lambda args (apply dropbox-save args))
			(lambda (root path) (dropbox-pathstring root path))))





