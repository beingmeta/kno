;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Amazon Web Services
(in-module 'google/drive)

(use-module '{fdweb oauth varconfig})
(use-module 'google)

(define (j->s x) (stringout (jsonout x)))

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
