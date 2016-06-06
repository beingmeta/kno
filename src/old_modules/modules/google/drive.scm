;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Amazon Web Services
(in-module 'google/drive)

(use-module '{fdweb oauth varconfig})
(use-module 'google)

(use-module '{google fdweb texttools mimetable logger ezrecords oauth})

(define (gdrive-string loc (auth))
  (set! auth (gdrive-auth loc))
  (stringout "#%(GDRIVE "
    (write (getopt auth 'email (getopt auth 'handle))) " "
    (gdrive-path loc) ")"))

(define-init gdrive:appname "")
(varconfig! gdrive:appname gdrive:appname)

(defrecord (gdrive . #[stringfn gdrive-string])
  auth path (opts {}))

(module-export!
 '{cons-gdrive
   gdrive? gdrive/auth gdrive/path gdrive-string
   gdrive:appname})

(module-export! '{gdrive/info})

(define gdrive/auth gdrive-auth)
(define gdrive/path gdrive-path)

(define (make-gdpath auth (path #f) (opts #f))
  (if (or (not path) (empty-string? path))
      (cons-gdrive auth "")
      (if (string? path)
	  (if (has-prefix path "/")
	      (cons-gdrive auth path (or opts #{}))
	      (cons-gdrive auth (glom "/" path) (or opts #{})))
	  (error |BadPath| make-gdpath path))))

(define (->gdrive input)
  (if (gdrive? input) input
      (if (string? input)
	  (if (has-prefix input "google:")
	      (->gdrive (subseq input 7))
	      (if (has-prefix input "//")
		  (let ((slash (position #\/ input 2)))
		    (if slash
			(cons-gdrive (subseq input 2 slash)
				     (subseq input (1+ slash)))
			(cons-gdrive (subseq input 2) "")))
		  (let ((colon (position #\: input))
			(slash (position #\/ input)))
		    (if (and colon slash)
			(if (< colon slash)
			    (cons-gdrive (subseq input 0 colon)
					 (if (= slash (1+ colon))
					     (subseq input (+ colon 2))
					     (subseq input (1+ colon))))
			    (cons-gdrive (subseq input 0 slash)
					 (subseq input (1+ slash))))
			(if slash
			    (cons-gdrive (subseq input 0 slash)
					 (subseq input (1+ slash)))
			    (if colon
				(cons-gdrive (subseq input 0 colon)
					     (subseq input (1+ colon)))
				(cons-gdrive input "")))))))
	  (error "Can't convert to gdrive" input))))

;;; Methods

(define (gdrive/info oauth path (error #f))
  (let* ((endpoint (glom "https://www.googleapis.com/drive/v2/files" path))
	 (result (oauth/call oauth 'GET endpoint '() #f #f #t))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-gdrive-metadata))))
    (if (>= 299 status 200)
	(let ((parsed (jsonparse (get result '%content))))
	  (store! parsed 'ctype (get parsed 'mime_type))
	  (store! parsed 'modified (timestamp (get parsed 'modified)))
	  (store! parsed 'length
		  (get (text->frame #((label bytes (isdigit+) #t) (spaces) 
				      "bytes")
				    (get parsed 'size))
		       'bytes))
	  parsed)
	(if (= status 404) #f
	    (and (or error (not (<= 400 status 500)))
		 (irritant result CALLFAILED GDRIVE/INFO
			   path " with " oauth))))))
#|

(define (gdrive/get/req oauth path)
  (let ((endpoint (db/url "https://api-content.gdrive.com/1/files/"
			  oauth path)))
    (if revision
	(oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
	(oauth/call oauth 'GET endpoint '() #f #f #t))))
(define (gdrive/get oauth path (revision #f))
  (let* ((endpoint (db/url "https://api-content.gdrive.com/1/files/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '() #f #f #t)))
	 (status (get result 'response)))
    (if (>= 299 status 200) (get result '%content)
	(if (= status 404)
	    (begin (lognotice |Gdrive404| "Gdrive call returned 404" result)
	      (fail))
	    (irritant result CALLFAILED GDRIVE/GET
		      path " with " oauth)))))
(define (gdrive/get+ oauth path (revision #f) (err #f))
  (let* ((endpoint (db/url "https://api-content.gdrive.com/1/files/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '() #f #f #t)))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-gdrive-metadata))))
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
	    (begin (lognotice |Gdrive404| "Gdrive call returned 404" result)
	      (fail))
	    (irritant result CALLFAILED GDRIVE/GET+
		      path " with " oauth)))))

(define (gdrive/info oauth path (revision #f) (error #f))
  (let* ((endpoint (db/url "https://api.gdrive.com/1/metadata/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint `(rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '() #f #f #t)))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-gdrive-metadata))))
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
		 (irritant result CALLFAILED GDRIVE/INFO
			   path " with " oauth))))))

(define (gdrive/list oauth path (revision #f))
  (let* ((endpoint (db/url "https://api.gdrive.com/1/metadata/"
			   oauth path))
	 (result (if revision
		     (oauth/call oauth 'GET endpoint
				 `("list" "true" rev ,revision) #f #f #t)
		     (oauth/call oauth 'GET endpoint '("list" "true") #f #f #t)))
	 (status (get result 'response))
	 (metadata (jsonparse (get result 'x-gdrive-metadata))))

    (if (>= 299 status 200)
	(jsonparse (get result '%content))
	(irritant result CALLFAILED GDRIVE/INFO
		  path " with " oauth))))

(define (gdrive/put! oauth path content (ctype #f) (revision #f))
  (unless ctype
    (set! ctype
	  (path->mimetype
	   path (if (packet? content) "application" "text"))))
  (let* ((endpoint
	  (db/url "https://api-content.gdrive.com/1/files_put/"
		  oauth path))
	 (result (oauth/call oauth 'put endpoint '() content ctype)))
    result))

(define (gd/get path)
  )

(define (j->s x) (stringout (jsonout x)))

|#

#|
(define gdrive (get @/u/66 'gdrive_info))
(oauth/call gdrive 'GET "https://www.googleapis.com/drive/v2/about")
(oauth/call gdrive 'GET "https://www.googleapis.com/drive/v2/files")
(define file1
  (oauth/call gdrive 'POST
	      "https://www.googleapis.com/drive/v2/files"
	      '()
	      (cons (j->s #[title "spressotest"
			    "mimeType" "text/plaintext"
			    description "testing"])
		    "application/json")))
(oauth/call gdrive 'PUT
	    (glom "https://www.googleapis.com/upload/drive/v2/files/"
	      (get file1 'id))
	    '("uploadType" "media")
	    (cons "This is the sPresso GDRIVE test" "text/plaintext"))
|#
