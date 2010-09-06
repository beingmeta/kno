;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'aws/s3)

;;; Accessing files with Amazon S3
(define version "$Id$")
(define revision "$Revision$")

(use-module '{aws fdweb texttools ezrecords rulesets logger})

(module-export! '{s3/signature s3/op s3/uri s3/signeduri s3/expected})
(module-export! '{s3/getloc s3loc/uri s3loc/filename s3loc/content})

(define %loglevel %info!)

;;; This is used by the S3 API sample code and we can use it to
;;;  test the signature algorithm
(define teststring
  "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg")

(define suffix-map
  (let ((table (make-hashtable)))
    (do-choices (map '{("jpg" "image/jpeg") ("jpeg" "image/jpeg")
		       ("png" "image/png") ("gif" "image/gif")
		       ("css" "text/css") ("js" "text/javascript")
		       ("html" "text/html") ("htm" "text/html") ("xhtml" "text/html")
		       ("manifest" "text/manifest")})
      (add! table (car map) (cadr map)))
    table))

(define (getmimetype path)
  (let* ((lastdot (rposition #\. path))
	 (suffix (and lastdot (subseq path (1+ lastdot)))))
    (and suffix (get suffix-map (downcase suffix)))))

;;; Representing S3 locations

(defrecord s3loc bucket path)

(define (s3/mkpath loc path)
  (cons-s3loc (s3loc-bucket loc) (mkpath (s3loc-path loc) path)))

(module-export! '{s3loc? s3loc-path s3loc-bucket cons-s3loc s3/mkpath})

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
	 (sig (s3/signature op bucket path date  headers
			    (or contentMD5 "") (or ctype "")))
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
    (debug%watch url ctype urlparams (length content))
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
		 (segment (car (get (xmlget (xmlparse (get response '%content))
					    'stringtosignbytes)
				    '%content))))))

(define (s3/write! loc content (ctype))
  (default! ctype (getmimetype (s3loc-path loc)))
  (debug%watch
   (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc) content ctype) ;; '(("x-amx-acl" . "public-read"))
   loc ctype))

(module-export! 's3/write!)

;;; Functions over S3 locations, mapping URLs to S3 and back

;;; Rules for converting URLs into S3 locations

(define s3urlrules '())
(ruleconfig! AWS:S3URLMAP s3urlrules)
;;; Each rule is of the form (<pat> <bucket>) where
;;;   <bucket> is an S3 bucket <pat> is either a string
;;;   or a texttools pattern/subst rule
;;; If <pat> is a string, the rule applies to all URLs beinging
;;;   with that string and the S3 path is the reset of the string
;;;   after the prfix.
;;; These rules can be configured with the AWS:S3URLMAP config variable

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

;; Rules for mapping S3 locations into the local file system
(define s3diskrules '())
(ruleconfig! AWS:S3DISKMAP s3diskrules)
;; Each rule has the form (<bucket> <pat>)
;;  <pat> can be:
;;    a string to append to the S3 path to get a local file path
;;    a function to call on the S3LOC to get a local file path
;;    a text subst pattern which is called to generate a local file path

(define (s3loc/filename loc)
  (tryseq (rule s3diskrules)
    (tryif (equal? (s3loc-bucket loc) (car rule))
      (if (applicable? (cadr rule))
	  ((cdr rule) loc)
	  (if (string? (cadr rule))
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

