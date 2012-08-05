;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc.  All rights reserved.

(in-module 'aws/s3)

;;; Accessing files with Amazon S3
(define version "$Id$")
(define revision "$Revision$")

(use-module '{aws fdweb texttools mimetable
	      ezrecords rulesets logger varconfig})

(module-export! '{s3/signature s3/op s3/uri s3/signeduri s3/expected})
(module-export! '{s3loc s3/getloc s3loc/s3uri
		  s3loc/uri s3loc/filename s3loc/get
		  s3loc/head s3loc/content s3loc/put s3loc/copy!})
(module-export! '{s3/get s3/copy! s3/put s3/head s3/ctype})
(module-export! '{s3/bytecodes->string})

(define-init %loglevel %info!)
;;(define %loglevel %debug!)

(define s3root "s3.amazonaws.com")
(varconfig! s3root s3root)

(define s3scheme "https://")
(varconfig! s3scheme s3scheme)

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
(define s3/guessmimetype getmimetype)
(module-export! 's3/guessmimetype)

;;; Representing S3 locations

(defrecord s3loc bucket path)

(define (->s3loc input)
  (if (s3loc? input) input
      (if (string? input)
	  (if (has-prefix input "s3:")
	      (->s3loc (subseq input 3))
	      (if (has-prefix input "//")
		  (let ((slash (position #\/ input 2)))
		    (if slash
			(cons-s3loc (subseq input 2 slash)
				    (subseq input (1+ slash)))
			(cons-s3loc (subseq input 2) "")))
		  (let ((colon (position #\: input))
			(slash (position #\/ input)))
		    (if (and colon slash)
			(if (< colon slash)
			    (cons-s3loc (subseq input 0 colon)
					(if (= slash (1+ colon))
					    (subseq input (+ colon 2))
					    (subseq input (1+ colon))))
			    (cons-s3loc (subseq input 0 slash)
					(subseq input (1+ slash))))
			(if slash
			    (cons-s3loc (subseq input 0 slash)
					(subseq input (1+ slash)))
			    (if colon
				(cons-s3loc (subseq input 0 colon)
					    (subseq input (1+ colon)))
				(cons-s3loc input "")))))))
	  (error "Can't convert to s3loc" input))))
(define s3/loc ->s3loc)

(define (s3/mkpath loc path)
  (when (string? loc) (set! loc (->s3loc loc)))
  (cons-s3loc (s3loc-bucket loc) (mkpath (s3loc-path loc) path)))

(module-export!
 '{s3loc? s3loc-path s3loc-bucket cons-s3loc ->s3loc s3/loc s3/mkpath})

;;; Computing S3 signatures

(define (s3/signature op bucket path (date (gmtimestamp)) (headers '())
		     (content-sig "") (content-ctype ""))
  (let* ((date (if (string? date) date
		   (if (number? date) (number->string date)
		       (get date 'rfc822))))
	 (sigstring
	  (string-append
	   op "\n"
	   content-sig "\n"
	   content-ctype "\n"
	   date "\n" (canonical-headers headers)
	   (if (empty-string? bucket) "" "/") bucket
	   (if (or (has-suffix bucket "/") (has-prefix path "/")) ""
	       "/")
	   path)))
    (debug%watch (hmac-sha1 secretawskey sigstring)
		 sigstring secretawskey)))

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

(define (s3/bytecodes->string string)
  (->string (map (lambda (x) (integer->char (string->number x 16)))
		  (segment string))))

(define (canonical-headers headers)
  (let* ((cheaders (remove #f (map canonicalize-header headers)))
	 (pheaders (->list (lexsorted (elts cheaders) car))))
    (stringout
      (do ((scan pheaders (cdr scan)))
	  ((null? scan) (printout))
	(printout (second (car scan)) ":" (third (car scan)) "\n")))))

(define (s3op op bucket path (content #f) (ctype) (headers '()) args)
  (default! ctype
    (path->mimetype path (if (packet? content) "application" "text")))
  (let* ((date (gmtimestamp))
	 ;; Encode everything, then restore delimiters
	 (path (string-subst (uriencode path) "%2F" "/"))
	 (cresource (string-append "/" bucket path))
	 (contentMD5 (and content (packet->base64 (md5 content))))
	 (sig (s3/signature op bucket path date  headers
			    (or contentMD5 "") (or ctype "")))
	 (authorization (string-append "AWS " awskey ":" (packet->base64 sig)))
	 (baseurl
	  (string-append s3scheme bucket (if (empty-string? bucket) "" ".")
			 s3root
			 (if (has-prefix path "/") "" "/")
			 path))
	 (url (if (null? args) baseurl (apply scripturl baseurl args)))
	 ;; Hide the expect field going to S3
	 (urlparams (frame-create #f 'header "Expect:")))
    (debug%watch url sig authorization)
    (when (and content (overlaps? op {"GET" "HEAD"}))
      (add! urlparams 'content-type ctype))
    (when (equal? op "DELETE") (store! urlparams 'method 'DELETE))
    (add! urlparams 'header (string-append "Date: " (get date 'rfc822)))
    (when contentMD5
      (add! urlparams 'header (string-append "Content-MD5: " contentMD5)))
    (add! urlparams 'header (string-append "Authorization: " authorization))
    (add! urlparams 'header (elts headers))
    (debug%watch url ctype urlparams (and (sequence? content) (length content)))
    (if (equal? op "GET")
	(urlget url urlparams)
	(if (equal? op "HEAD")
	    (urlhead url urlparams)
	    (if (equal? op "POST")
		(urlpost url urlparams (or content ""))
		(if (equal? op "PUT")
		    (urlput url (or content "") ctype urlparams)
		    (urlget url urlparams)))))))
(define (s3/op op bucket path (content #f) (ctype) (headers '()) . args)
  (default! ctype
    (path->mimetype path (if (packet? content) "application" "text")))
  (let* ((result (s3op op bucket path content ctype headers args))
	 (status (get result 'status)))
    (debug%watch result)
    (if (>= 299 status 200) result
	(begin (log%warn "Bad result " status " (" (get result 'header)
			 ") for" (get result 'effective-url)
			 "\n#|" (get result '%content) "|#\n"
			 result)
	       result))))

(define (s3/uri bucket path (scheme s3scheme))
  (stringout scheme bucket (if (empty-string? bucket) "" ".") s3root
	     (unless (has-prefix path "/") "/")
	     path))

(define (s3/signeduri bucket path (scheme s3scheme)
		      (expires (* 17 3600))
		      (op "GET") (headers '()))
  (unless (has-prefix path "/") (set! path (string-append "/" path)))
  (let* ((exptick (if (number? expires)
		      (if (> expires (time)) expires
			  (+ (time) expires))
		      (get expires 'tick)))
	 (sig (s3/signature "GET" bucket path exptick headers)))
    (string-append
     scheme bucket (if (empty-string? bucket) "" ".") s3root path
     "?" "AWSAccessKeyId=" awskey "&"
     "Expires=" (number->string exptick) "&"
     "Signature=" (uriencode (packet->base64 sig)))))

(define (s3/expected response)
  (->string (map (lambda (x) (integer->char (string->number x 16)))
		 (segment (car (get (xmlget (xmlparse (get response '%content))
					    'stringtosignbytes)
				    '%content))))))

(define (s3/write! loc content (ctype))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! ctype
    (path->mimetype (s3loc-path loc)
		    (if (packet? content) "application" "text")))
  (debug%watch
   (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc) content ctype)
   ;; '(("x-amx-acl" . "public-read"))
   loc ctype))

(define (s3/delete! loc)
  (when (string? loc) (set! loc (->s3loc loc)))
  ;; '(("x-amx-acl" . "public-read"))
  (debug%watch (s3/op "DELETE" (s3loc-bucket loc) (s3loc-path loc) #f "") loc))

(module-export! '{s3/write! s3/delete!})

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

(define (s3loc bucket path)
  (if (s3loc? bucket)
      (s3/mkpath bucket path)
      (cons-s3loc (if (has-prefix bucket "s3://") (subseq bucket 5)
		      (if (has-prefix bucket "s3:")
			  (subseq bucket 3)
			  bucket))
		  (if (has-prefix path "/") (subseq path 1) path))))

(define (s3loc/uri s3loc)
  (stringout s3scheme s3root "/"
    (s3loc-bucket s3loc)
    (unless (has-prefix (s3loc-path s3loc) "/") "/")
    (s3loc-path s3loc)))

(define (s3loc/s3uri s3loc)
  (stringout "s3://"
    (s3loc-bucket s3loc)
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

(define (s3loc/get loc (text #t))
  (when (string? loc) (set! loc (->s3loc loc)))
  (s3/op "GET" (s3loc-bucket loc)
	 (string-append "/" (s3loc-path loc))
	 ""))

(define (s3loc/head loc (text #t))
  (when (string? loc) (set! loc (->s3loc loc)))
  (s3/op "HEAD" (s3loc-bucket loc)
	 (string-append "/" (s3loc-path loc))
	 ""))
(define s3/head s3loc/head)

(define (s3/ctype loc)
  (get (s3loc/head loc) 'content-type))

(define (s3loc/put loc content (ctype))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! ctype
    (path->mimetype (s3loc-path path)
		    (if (packet? content) "application" "text")))
  (s3/op "PUT" (s3loc-bucket loc)
	 (string-append "/" (s3loc-path loc))
	 content ctype))
(define s3/put s3loc/put)

(define (s3loc/copy! src loc)
  (when (string? loc) (set! loc (->s3loc loc)))
  (when (string? src) (set! src (->s3loc src)))
  (let* ((head (s3loc/head src))
	 (ctype (try (get head 'content-type)
		     (path->mimetype
		      (s3loc-path loc)
		      (path->mimetype (s3loc-path src) "text")))))
    (s3/op "PUT" (s3loc-bucket loc)
	   (string-append "/" (s3loc-path loc))
	   "" ctype
	   `(("x-amz-copy-source" .
	      ,(stringout "/" (s3loc-bucket src) 
		 "/" (s3loc-path src)))))))
(define s3/copy! s3loc/copy!)

(define (s3loc/content loc (text #t))
  (when (string? loc) (set! loc (->s3loc loc)))
  (try (if text (filestring (s3loc/filename loc))
	   (filedata (s3loc/filename loc)))
       (let* ((req (s3/op "GET" (s3loc-bucket loc)
			  (string-append "/" (s3loc-path loc))
			  ""))
	      (status (get req 'response)))
	 (if (and status (>= 299 status 200))
	     (get req '%content)
	     (error 's3failure (get req '%content))))))
(define s3/get s3loc/content)

;;; Working with S3 'dirs'

(define (s3/list loc)
  (when (string? loc) (set! loc (->s3loc loc)))
  (let* ((req (s3/op "GET" (s3loc-bucket loc) "/" "" "text" '()
		     "delimiter" "/" "prefix" (s3loc-path loc)))
	 (content (xmlparse (get req '%content))))
    (choice
     (for-choices (path (xmlcontent (xmlget (xmlget content 'commonprefixes)
					    'prefix)))
       (cons-s3loc (s3loc-bucket loc) path))
     (for-choices (path (xmlcontent (xmlget content 'key)))
       (cons-s3loc (s3loc-bucket loc) path)))))
(module-export! 's3/list)

;;; Some test code

(comment
 (s3/op "GET" "data.beingmeta.com" "/brico/brico.db" "")
 (s3/op "PUT" "public.sbooks.net" "/uspto/6285999" content "text/html"
	(list "x-amz-acl: public-read")))

