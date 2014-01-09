;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Dropbox
(in-module 'dropbox)

(use-module '{fdweb xhtml signature oauth gpath mimetable ezrecords})

(module-export! '{dropbox/get dropbox/put!})

(defrecord dropbox oauth (root-path #f))

(define-init dropbox-root "sandbox")
(config-def! dropbox:live
	     (lambda (var (val))
	       (if (bound? val)
		   (if val (set! dropbox-root "dropbox") (set! dropbox-root "sandbox"))
		   (if (identical? dropbox-root "sandbox") #f
		       (if (identical? dropbox-root "dropbox") #t
			   (begin (logwarn "Fixing invalid dropbox-root " dropbox-root)
			     (set! dropbox-root "sandbox")))))))

(define (dropbox/get oauth path (revision #f))
  (let ((endpoint (glom "https://content-api.dropbox.com/1/files/"
		    (getopt oauth 'root
			    (if (testopt root 'live)
				(if (getopt root 'live #f) "dropbox" "sandbox")
				dropbox-root))
		    "/" path))
	(result (if revision (oauth/call* oauth "GET" endpoint 'rev revision)
		    (oauth/call* oauth "GET" endpoint 'rev revision))))
    result))

(define (dropbox/put! oauth path content (ctype #f) (revision #f))
  (unless ctype
    (set! ctype (path->mimetype path (if (packet? content) "application" "text"))))
  (let ((endpoint
	 (scripturl
	     (glom "https://content-api.dropbox.com/1/files_put/"
	       (getopt oauth 'root
		       (if (testopt root 'live)
			   (if (getopt root 'live #f) "dropbox" "sandbox")
			   dropbox-root))
	       "/" path)))
	(result (oauth/call* oauth "PUT" endpoint content ctype)))
    result))





