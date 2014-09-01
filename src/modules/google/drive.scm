;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Amazon Web Services
(in-module 'google/drive)

(use-module '{fdweb oauth varconfig})
(use-module 'google)

(use-module '{google fdweb texttools mimetable logger ezrecords oauth})

(defrecord (gdrive . #[stringfn gdrive-string])
  auth bucket path (opts {}))

(define (gdrive-string loc)
  (stringout "#%(GDRIVE "
    (write (gdrive-auth loc))
    (write (gdrive-bucket loc))
    (if (> (compound-length loc) 2) 
	(printout " " (write (gdrive-path loc))))
    (if (and (> (compound-length loc) 2) (exists? (s3loc-opts loc)))
	(printout " " (write (gdrive-opts loc))))
    ")"))

(module-export!
 '{cons-gdrive
   gdrive?
   gdrive/auth gdrive/bucket gdrive/path gdrive-string})

(define gdrive/auth gdrive-bucket)
(define gdrive/bucket gdrive-bucket)
(define gdrive/path gdrive/path)

(define (make-gdpath bucket path (opts #f))
  (if (and (string? bucket) (not (position #\/ bucket)))
      (if (not path) (cons-gdrive bucket "")
	  (if (string? path)
	      (if (has-prefix path "/")
		  (cons-gdrive bucket path (or opts #{}))
		  (cons-gdrive bucket (glom "/" path) (or opts #{})))
	      (error badpath make-gdrive path)))
      (error badbucket make-gdrive bucket)))

(define (->gdrive input)
  (if (gdrive? input) input
      (if (string? input)
	  (if (has-prefix input "google:")
	      (->gdrive (subseq input 7))
	      (if (has-prefix input "//")
		  (let ((slash (position #\/ input 2)))
		    (if slash
			(make-gdrive (subseq input 2 slash)
				     (subseq input (1+ slash)))
			(make-gdrive (subseq input 2) "")))
		  (let ((colon (position #\: input))
			(slash (position #\/ input)))
		    (if (and colon slash)
			(if (< colon slash)
			    (make-gdrive (subseq input 0 colon)
					 (if (= slash (1+ colon))
					     (subseq input (+ colon 2))
					     (subseq input (1+ colon))))
			    (make-gdrive (subseq input 0 slash)
					 (subseq input (1+ slash))))
			(if slash
			    (make-gdrive (subseq input 0 slash)
					 (subseq input (1+ slash)))
			    (if colon
				(make-gdrive (subseq input 0 colon)
					     (subseq input (1+ colon)))
				(make-gdrive input "")))))))
	  (error "Can't convert to gdrive" input))))



(define (gd/get path)
  )

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
