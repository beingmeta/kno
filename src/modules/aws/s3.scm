;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'aws/s3)

;;; Accessing files with Amazon S3
(define version "$Id$")
(define revision "$Revision$")

(use-module '{aws fdweb texttools ezrecords})

(module-export! '{s3/signature s3/op s3/uri s3/signeduri s3/expected})
(module-export! '{s3/getloc s3loc/uri s3loc/filename s3loc/content})

;;; This is used by the S3 API sample code and we can use it to
;;;  test the signature algorithm
(define teststring
  "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg")

;;; Representing S3 locations

(defrecord s3loc bucket path)

(module-export! '{s3loc? s3loc-path s3loc-bucket})

;;; Computing S3 signatures

(define (s3/signature op bucket path (date (gmtimestamp)) (headers '())
		     (content-sig "") (content-ctype ""))
  (let* ((date (if (string? date) date
		   (if (number? date) (number->string date)
		       (get date 'rfc822))))
	 (description
	  (string-append
	   op "\n"
	   content-sig "\n"
	   content-ctype "\n"
	   date "\n" (canonical-headers headers) "/" bucket path)))
    ;; (message "Description=" (write description))
    (hmac-sha1 secretawskey description)))

(define (canonicalize-header x)
  (if (pair? x)
      (and (string? (car x))
	   (has-prefix (downcase (car x)) "x-amz-")
	   (list (downcase (car x)) (car x) (cdr x)))
      (if (string? x)
	  (let ((colon (position #\: x)))
	    (and (has-prefix (downcase x) "x-amz-")
		 (if colon
		     (list (downcase (stdspace (subseq x 0 colon)))
			   (stdspace (subseq x 0 colon))
			   (stdspace (subseq x (1+ colon))))
		     (stdspace x))))
	  #f)))

(define (canonical-headers headers)
  (let* ((cheaders (remove #f (map canonicalize-header headers)))
	 (pheaders (->list (lexsorted (elts cheaders) car))))
    (stringout
      (do ((scan pheaders (cdr scan)))
	  ((null? scan) (printout))
	(printout (second (car scan)) ":" (third (car scan)) "\n")))))

(define (s3/op op bucket path (content #f) (ctype "text") (headers '()))
  (let* ((date (gmtimestamp))
	 (cresource (string-append "/" bucket path))
	 (contentMD5 (and content (packet->base64 (md5 content))))
	 (sig (s3/signature op bucket path date  headers (or contentMD5 "") (or ctype "")))
	 (authorization (string-append "AWS " awskey ":" (packet->base64 sig)))
	 (url (string-append "http://" bucket ".s3.amazonaws.com" path))
	 ;; Hide the except field going to S3
	 (urlparams (frame-create #f 'header "Expect:")))
    ;; (message "sig=" sig)
    ;; (message "authorization: " authorization)
    (when (and content (overlaps? op {"GET" "HEAD"}))
      (add! urlparams 'content-type ctype))
    (add! urlparams 'header (string-append "Date: " (get date 'rfc822)))
    (add! urlparams 'header (string-append "Content-MD5: " contentMD5))
    (add! urlparams 'header (string-append "Authorization: " authorization))
    (add! urlparams 'header (elts headers))
    (if (equal? op "GET")
	(urlget url urlparams)
	(if (equal? op "HEAD")
	    (urlhead url urlparams)
	    (if (equal? op "POST")
		(urlpost url urlparams content)
		(if (equal? op "PUT")
		    (urlput url content ctype urlparams)
		    (urlget url urlparams content)))))))

(define (s3/uri bucket path)
  (stringout "http://" bucket ".s3.amazonaws.com"
	     (unless (has-prefix path "/") "/")
	     path))

(define (s3/signeduri bucket path expires (op "GET") (headers '()))
  (let* ((expires (if (number? expires) expires (get expires 'tick)))
	 (sig (s3/signature "GET" bucket path expires headers)))
    (string-append
     "http://" bucket ".s3.amazonaws.com" path "?"
     "AWSAccessKeyId=" awskey "&"
     "Expires=" (number->string expires) "&"
     "Signature=" (packet->base64 sig))))

(define (s3/expected response)
  (->string (map (lambda (x) (integer->char (string->number x 16)))
		 (segment (car (get (xmlget (xmlparse (get response '%content)) 'stringtosignbytes)
				    '%content))))))

;;; Functions over S3 locations, mapping URLs to S3 and back

(define s3urlrules
  '(("http://store.sbooks.net/" "storage.sbooks.net")
    ("http://offline.store.sbooks.net/" "storage.sbooks.net")
    ("http://library.sbooks.net/" "library.sbooks.net")
    ("http://public.sbooks.net/" "public.sbooks.net")))

(define (s3/getloc url)
  (tryseq (rule s3urlrules)
    (tryif (string-starts-with? url (car rule))
      (cons-s3loc (cadr rule)
		  (if (string? (car rule))
		      (subseq url (length (car rule)))
		      (textsubst url (car rule)))))))

(define (s3loc/uri s3loc)
  (stringout "http://"
	     (s3loc-bucket s3loc) ".s3.amazonaws.com"
	     (unless (has-prefix (s3loc-path s3loc) "/") "/")
	     (s3loc-path s3loc)))

(define s3diskrules '())

(define (s3loc/filename loc)
  (tryseq (rule s3diskrules)
    (tryif (equal? (s3loc-bucket loc) (car rule))
      (if (applicable? (cadr rule))
	  ((cdr rule) loc)
	  (if (string? (cdr rule))
	      (string-append (cadr rule)
			     (if (has-suffix (cadr rule) "/") "" "/")
			     (s3loc-path loc))
	      (tryif (exists? (textmatcher (cadr rule) (s3loc-path loc)))
		(textsubst (s3loc-path loc) (cadr rule))))))))

(define (s3loc/content loc (text #t))
  (try (if text (filestring (s3loc/filename loc))
	   (filedata (s3loc/filename loc)))
       (get (s3/op "GET" (s3loc-bucket loc)
		   (string-append "/" (s3loc-path loc))
		   "")
	    '%content)))

;;; Some test code

(comment
 (s3/op "GET" "data.beingmeta.com" "/brico/brico.db" "")
 (s3/op "PUT" "public.sbooks.net" "/uspto/6285999" content "text/html"
	(list "x-amz-acl: public-read")))

