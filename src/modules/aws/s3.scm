;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

(in-module 'aws/s3)

(use-module '{aws fdweb texttools mimetable
	      ezrecords rulesets logger varconfig})
(define %used_modules '{aws varconfig ezrecords rulesets})

(module-export! '{s3/signature s3/op s3/expected
		  s3/uri s3/signeduri s3/pathuri s3/hosturi})
(module-export! '{s3loc s3/getloc s3loc/s3uri make-s3loc
		  s3loc/uri s3loc/filename})
(module-export! '{s3loc/get s3loc/head s3loc/exists?
		  s3loc/etag s3loc/modified s3loc/info
		  s3loc/content s3loc/put s3loc/copy! s3loc/link!})
(module-export! '{s3/get s3/get+ s3/head s3/ctype s3/exists?
		  s3/modified s3/etag  s3/info
		  s3/bucket? s3/copy! s3/link! s3/put
		  s3/download!})
(module-export! '{s3/bytecodes->string})

(define-init %loglevel %notify%)
;;(set! %loglevel %debug%)

(define s3root "s3.amazonaws.com")
(varconfig! s3root s3root)

(define s3scheme "https://")
(varconfig! s3scheme s3scheme)

(define s3errs #t)
(varconfig! s3errs s3errs config:boolean)

(define default-usepath #t)
(varconfig! s3pathstyle default-usepath)

(define-init website-buckets (make-hashset))
(config-def! 's3websites
	     (lambda (var (val))
	       (if (bound? val)
		   (if (string? val)
		       (hashset-add! website-buckets val)
		       (if (and (pair? val) (overlaps? (car val) '{not drop})
				(or (string? (cdr val)) (string? (cadr val))))
			   (hashset-drop! website-buckets
					  (if (string? (cdr val)) (cdr val) (cadr val)))
			   (error 'NOT_AN_S3BUCKET val)))
		   (hashset-elts website-buckets))))

(define s3cache #f)
(varconfig! s3cache s3cache)

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

(define (s3loc-string loc)
  (stringout "#%(S3LOC " (write (s3loc-bucket loc))
    (if (> (record-length loc) 1) (printout " " (write (s3loc-path loc))))
    (if (and (> (record-length loc) 2) (exists? (s3loc-opts loc)))
	(printout " " (write (s3loc-opts loc))))
    ")"))

(defrecord (s3loc . #[stringfn s3loc-string])
  bucket path (opts {}))
(define s3loc/bucket s3loc-bucket)
(define s3loc/path s3loc-path)
(define (s3loc/opts loc)
  (if (s3loc? loc)
      (tryif (> (compound-length loc) 2)
	(s3loc-opts loc))
      (irritant loc "Not an S3LOC")))

(define (make-s3loc bucket path (opts #f))
  (if (and (string? bucket) (not (position #\/ bucket)))
      (if (not path) (cons-s3loc bucket "")
	  (if (string? path)
	      (if (has-prefix path "/")
		  (cons-s3loc bucket path (or opts #{}))
		  (cons-s3loc bucket (glom "/" path) (or opts #{})))
	      (error badpath make-s3loc path)))
      (error badbucket make-s3loc bucket)))

(define (->s3loc input)
  (if (s3loc? input) input
      (if (string? input)
	  (if (has-prefix input "s3:")
	      (->s3loc (subseq input 3))
	      (if (has-prefix input "//")
		  (let ((slash (position #\/ input 2)))
		    (if slash
			(make-s3loc (subseq input 2 slash)
				    (subseq input (1+ slash)))
			(make-s3loc (subseq input 2) "")))
		  (let ((colon (position #\: input))
			(slash (position #\/ input)))
		    (if (and colon slash)
			(if (< colon slash)
			    (make-s3loc (subseq input 0 colon)
					(if (= slash (1+ colon))
					    (subseq input (+ colon 2))
					    (subseq input (1+ colon))))
			    (make-s3loc (subseq input 0 slash)
					(subseq input (1+ slash))))
			(if slash
			    (make-s3loc (subseq input 0 slash)
					(subseq input (1+ slash)))
			    (if colon
				(make-s3loc (subseq input 0 colon)
					    (subseq input (1+ colon)))
				(make-s3loc input "")))))))
	  (error "Can't convert to s3loc" input))))
(define s3/loc ->s3loc)

(define (s3/mkpath loc path . more)
  (when (string? loc) (set! loc (->s3loc loc)))
  (if (null? more)
      (make-s3loc (s3loc-bucket loc) (mkpath (s3loc-path loc) path)
		  (qc (s3loc-opts loc)))
      (apply s3/mkpath
	     (make-s3loc (s3loc-bucket loc) (mkpath (s3loc-path loc) path)
			 (qc (s3loc-opts loc)))
	     more)))

(define (s3loc->string s3)
  (stringout "s3://" (s3loc-bucket s3) (s3loc-path s3)))

(module-export!
 '{s3loc? s3loc-path s3loc-bucket make-s3loc ->s3loc s3/loc s3/mkpath
   s3loc->string})

;;; Computing signatures for S3 calls

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

;;; Making S3 calls

(define (s3op op bucket path (content #f) (ctype) (headers '()) args)
  (default! ctype
    (path->mimetype path (if (packet? content) "application" "text")))
  (let* ((date (gmtimestamp))
	 ;; Encode everything, then restore delimiters
	 (path (string-subst (uriencode path) "%2F" "/"))
	 (contentMD5 (and content (packet->base64 (md5 content))))
	 (sig (s3/signature op bucket path date  headers
			    (or contentMD5 "") (or ctype "")))
	 (authorization (string-append "AWS " awskey ":" (packet->base64 sig)))
	 (baseurl
	  (if default-usepath
	      (glom s3scheme s3root "/" bucket path)
	      (glom s3scheme bucket (if (empty-string? bucket) "" ".") s3root path)))
	 (url (if (null? args) baseurl (apply scripturl baseurl args)))
	 ;; Hide the expect field going to S3
	 (urlparams (frame-create #f 'header "Expect:")))
    (when (and content (overlaps? op {"GET" "HEAD"}))
      (add! urlparams 'content-type ctype))
    (when (equal? op "DELETE") (store! urlparams 'method 'DELETE))
    (add! urlparams 'header (string-append "Date: " (get date 'rfc822)))
    (when contentMD5
      (add! urlparams 'header (string-append "Content-MD5: " contentMD5)))
    (add! urlparams 'header (string-append "Authorization: " authorization))
    (add! urlparams 'header (elts headers))
    (when (>= %loglevel %detail%) (add! urlparams 'verbose #t))
    (loginfo |S3OP| op " " bucket ":" path " "
	     (when (and content (not (= (length content) 0)))
	       (glom " ("
		 (if ctype ctype "content") ", " (length content) 
		 (if (string? content) " characters)" "bytes)")))
	     (if (null? headers) " headers=" " headers=\n\t") headers
	     "\n\turl:\t" url)
    (debug%watch "S3OP/sig" url sig authorization)
    (if (equal? op "GET")
	(urlget url urlparams)
	(if (equal? op "HEAD")
	    (urlhead url urlparams)
	    (if (equal? op "POST")
		(urlpost url urlparams (or content ""))
		(if (equal? op "PUT")
		    (urlput url (or content "") ctype urlparams)
		    (urlget url urlparams)))))))
(define (s3/op op bucket path (err s3errs)
	       (content #f) (ctype) (headers '()) . args)
  (default! ctype
    (path->mimetype path (if (packet? content) "application" "text")))
  (let* ((s3result (s3op op bucket path content ctype headers args))
	 (content (get s3result '%content))
	 (status (get s3result 'response)))
    (unless (>= 299 status 200)
      (onerror
	  (store! s3result '%content (xmlparse content))
	(lambda (ex) #f)))
    (if (>= 299 status 200)
	(detail%watch s3result)
	(cond ((and (not err) (equal? op "HEAD") (overlaps? status {404 410}))
	       ;; Don't generate warnings for HEAD probes
	       s3result)
	      ((and (not err) (= status 404))
	       (logwarn |S3/NotFound| S3/OP s3result)
	       s3result)
	      ((and (not err) (= status 403))
	       (logwarn |S3/Forbidden| S3/OP s3result)
	       s3result)
	      ((not err)
	       (logwarn |S3/Failure| S3/OP
			(try (get s3result 'header) "HTTP return failed")
			":\n\t" s3result)
	       s3result)
	      ((and err (= status 404))
	       (irritant s3result |S3/NotFound| S3/OP
			 "s3://" bucket path))
	      ((and err (= status 403))
	       (irritant s3result |S3/Forbidden| S3/OP
			 "s3://" bucket path))
	      (else (irritant s3result |S3/Failure| S3/OP
			      "s3://" bucket path))))))

(define (s3/expected response)
  (->string (map (lambda (x) (integer->char (string->number x 16)))
		 (segment (car (get (xmlget (xmlparse (get response '%content))
					    'stringtosignbytes)
				    '%content))))))

;;; Getting S3 URIs for the API

(define (s3/uri bucket path (scheme s3scheme) (usepath default-usepath))
  (when (not scheme) (set! scheme s3scheme))
  (if usepath
      (stringout scheme s3root "/" bucket path)
      (if (and (hashset-get website-buckets bucket) (equal? scheme "http://"))
	  (stringout scheme bucket path)
	  (stringout scheme bucket "." s3root path))))
(define (s3/pathuri bucket path (scheme s3scheme))
  (s3/uri bucket path scheme #t))
(define (s3/hosturi bucket path (scheme s3scheme))
  (s3/uri bucket path scheme #f))

;;; Getting signed URIs

(define (signeduri bucket path (scheme s3scheme)
		   (expires (* 17 3600))
		   (op "GET") (headers '())
		   (usepath #t))
  (unless (has-prefix path "/") (set! path (glom "/" path)))
  (let* ((exptick (if (number? expires)
		      (if (> expires (time)) expires
			  (+ (time) expires))
		      (get expires 'tick)))
	 (sig (s3/signature "GET" bucket path exptick headers)))
    (if (or usepath (equal? scheme "https://"))
	(string-append
	 scheme s3root "/" bucket path
	 "?" "AWSAccessKeyId=" awskey "&"
	 "Expires=" (number->string exptick) "&"
	 "Signature=" (uriencode (packet->base64 sig)))
	(string-append
	 scheme bucket (if (empty-string? bucket) "" ".") s3root path
	 "?" "AWSAccessKeyId=" awskey "&"
	 "Expires=" (number->string exptick) "&"
	 "Signature=" (uriencode (packet->base64 sig))))))
(define (s3/signeduri arg . args)
  (if (or (null? args) (number? (car args)) (timestamp? (car args)))
      (let ((s3loc (->s3loc arg)))
	(signeduri (s3loc-bucket s3loc) (s3loc-path s3loc) "https://"
		   (if (pair? args)
		       (if (number? (car args))
			   (timestamp+ (car args))
			   (car args))
		       (* 48 3600))
		   "GET" (try (get (s3loc-opts s3loc) 'headers) '())))
      (apply signeduri arg args)))

;;; Operations

(define (s3/write! loc content (ctype) (headers) (err s3errs))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! ctype
    (path->mimetype (s3loc-path loc)
		    (if (packet? content) "application" "text")))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (debug%watch
      (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc)
	err content ctype headers)
    loc ctype headers))

(define (s3/delete! loc (headers) (err s3errs))
  (when (string? loc) (set! loc (->s3loc loc)))
  ;; '(("x-amx-acl" . "public-read"))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (debug%watch (s3/op "DELETE"
		   (s3loc-bucket loc)
		   (s3loc-path loc) err "" #f headers) loc))

(define (s3/bucket? loc (headers))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (let ((req (s3/op "HEAD" (s3loc-bucket loc) "" #f "" #f headers)))
    (response/ok? req)))

(module-export! '{s3/bucket? s3/write! s3/delete!})

;;; User facing s3loc constructors, exporters, etc

(define (s3loc bucket path)
  (if (s3loc? bucket)
      (s3/mkpath bucket path)
      (make-s3loc (if (has-prefix bucket "s3://") (subseq bucket 5)
		      (if (has-prefix bucket "s3:")
			  (subseq bucket 3)
			  bucket))
		  path)))

(define (s3loc/uri s3loc)
  (if (string? s3loc) (set! s3loc (->s3loc s3loc)))
  (stringout s3scheme s3root "/" (s3loc-bucket s3loc) (s3loc-path s3loc)))

(define (s3loc/s3uri s3loc)
  (if (string? s3loc) (set! s3loc (->s3loc s3loc)))
  (stringout "s3://" (s3loc-bucket s3loc) (s3loc-path s3loc)))

;;; Basic S3LOC network methods

(define (s3loc/get loc (headers) (err))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (default! err (try (get (s3loc-opts loc) 's3errs) s3errs))
  (s3/op "GET" (s3loc-bucket loc) (s3loc-path loc) err "" "text" headers))

(define (s3loc/head loc (headers))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (s3/op "HEAD" (s3loc-bucket loc) (s3loc-path loc) #f "" "" headers))
(define s3/head s3loc/head)

(define (s3loc/get+ loc (text #t) (headers) (err s3errs))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (let* ((req (s3/op "GET" (s3loc-bucket loc) (s3loc-path loc)
		err "" "text" headers))
	 (status (get req 'response)))
    (if (and status (>= 299 status 200))
	`#[content ,(get req '%content)
	   ctype ,(try (get req 'content-type)
		       (guess-mimetype (s3loc-path loc))
		       (if text "text" "application"))
	   encoding ,(get req 'content-encoding)
	   modified ,(try (get req 'last-modified) (timestamp))
	   etag ,(try (get req 'etag) (md5 (get req '%content)))]
	(and err (irritant req S3FAILURE S3LOC/CONTENT
			   (s3loc->string loc))))))
(define s3/get+ s3loc/get+)

;;; Basic S3 network metadata methods

(define (s3loc/modified loc)
  (let ((info (s3loc/head loc)))
    (try (get info 'last-modified) #f)))
(define s3/modified s3loc/modified)

(define (s3loc/exists? loc)
  (when (string? loc) (set! loc (->s3loc loc)))
  (let ((req (s3/op "HEAD" (s3loc-bucket loc) (s3loc-path loc) #f "")))
    (response/ok? req)))
(define s3/exists? s3loc/exists?)

(define (s3loc/etag loc (compute #f) (headers))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (let ((req (s3/op "HEAD" (s3loc-bucket loc) (s3loc-path loc)
	       #f "" "" headers)))
    (and (response/ok? req)
	 (try (get req 'etag) (and compute (md5 (s3loc/content loc)))))))
(define s3/etag s3loc/etag)

(define (s3loc/ctype loc)
  (when (string? loc) (set! loc (->s3loc loc)))
  (get (s3loc/head loc) 'content-type))
(define s3/ctype s3loc/ctype)

(define (s3loc/info loc (headers))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (let ((req (s3/op "HEAD" (s3loc-bucket loc) (s3loc-path loc)
	       #f "" "" headers)))
    (and (response/ok? req)
	 `#[path ,(s3loc/s3uri loc)
	    ctype ,(try (get req 'content-type)
			(guess-mimetype (s3loc-path loc))
			(if text "text" "application"))
	    encoding ,(get req 'content-encoding)
	    modified ,(try (get req 'last-modified) (timestamp))
	    etag ,(try (get req 'etag) (md5 (get req '%content)))])))
(define s3/info s3loc/info)

;;; Basic S3 network write methods

(define (s3loc/put loc content (ctype) (headers) (err s3errs))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! ctype
    (path->mimetype (s3loc-path loc)
		    (if (packet? content) "application" "text")))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc) err
    content ctype headers))
(define s3/put s3loc/put)

(define (s3loc/copy! src loc (err s3errs) (inheaders) (outheaders))
  (when (string? loc) (set! loc (->s3loc loc)))
  (when (string? src) (set! src (->s3loc src)))
  (default! inheaders (try (get (s3loc-opts src) 'headers) '()))
  (default! outheaders (try (get (s3loc-opts loc) 'headers) '()))
  (let* ((head (s3loc/head src inheaders))
	 (ctype (try (get head 'content-type)
		     (path->mimetype
		      (s3loc-path loc)
		      (path->mimetype (s3loc-path src) "text")))))
    (loginfo |S3/copy| "Copying " src " to " loc)
    (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc) err "" ctype
	   `(("x-amz-copy-source" .
	      ,(stringout "/" (s3loc-bucket src) (s3loc-path src)))
	     ,@inheaders
	     ,@outheaders))))
(define s3/copy! s3loc/copy!)

(define (s3loc/link! src loc (err s3errs))
  (when (string? loc) (set! loc (->s3loc loc)))
  (when (and (string? src) (not (has-prefix src {"http:" "https:" "ftp:"})))
    (set! src (->s3loc src)))
  (let* ((head (tryif (s3loc? src) (s3loc/head src)))
	 (ctype (try (get head 'content-type)
		     (path->mimetype
		      (s3loc-path loc)
		      (path->mimetype
		       (if (s3loc? src) (s3loc-path src)
			   (uripath src))
		       "text")))))
    (loginfo |S3/link| "Linking (via redirect) " src " to " loc)
    (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc) err "" ctype
	   `(("x-amz-website-redirect-location" .
	      ,(if (s3loc? src)
		   (stringout "/" (s3loc-bucket src) (s3loc-path src))
		   src))))))
(define s3/link! s3loc/link!)

;;; Working with S3 'dirs'

(define (s3/list loc (headers) (err s3errs))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc-opts loc) 'headers) '()))
  (let* ((req (s3/op "GET" (s3loc-bucket loc) "/" 
		err "" "text" headers
		"delimiter" "/"
		"prefix" (if (has-prefix (s3loc-path loc) "/")
			     (slice (s3loc-path loc) 1)
			     (s3loc-path loc))))
	 (content (xmlparse (get req '%content))))
    (choice
     (for-choices (path (xmlcontent (xmlget (xmlget content 'commonprefixes)
					    'prefix)))
       (make-s3loc (s3loc-bucket loc) path))
     (for-choices (path (xmlcontent (xmlget content 'key)))
       (make-s3loc (s3loc-bucket loc) path)))))
(module-export! 's3/list)

(define (s3/list* loc (headers '()) (err s3errs))
  (when (string? loc) (set! loc (->s3loc loc)))
  (let* ((req (s3/op "GET" (s3loc-bucket loc) "/" err "" "text" headers
		     "prefix" (if (has-prefix (s3loc-path loc) "/")
				  (slice (s3loc-path loc) 1)
				  (s3loc-path loc))))
	 (content (xmlparse (get req '%content))))
    (choice
     (for-choices (path (xmlcontent (xmlget (xmlget content 'commonprefixes)
					    'prefix)))
       (make-s3loc (s3loc-bucket loc) path))
     (for-choices (path (xmlcontent (xmlget content 'key)))
       (make-s3loc (s3loc-bucket loc) path)))))
(module-export! 's3/list*)

;;; Recursive deletion

(define (s3/axe! loc (headers '()) (err s3errs))
  (when (string? loc) (set! loc (->s3loc loc)))
  (let ((paths (s3/list* loc err)))
    (do-choices (path (s3/list* loc err))
      (s3/delete! path headers))))
(module-export! 's3/axe!)

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
      (make-s3loc (cadr rule)
		  (if (string? (car rule))
		      (subseq url (length (car rule)))
		      (textsubst url (car rule)))))))

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
	      (string-append (if (has-suffix (cadr rule) "/")
				 (slice (cadr rule) 0 -1)
				 (cadr rule))
			     (s3loc-path loc))
	      (tryif (exists? (textmatcher (cadr rule) (s3loc-path loc)))
		(textsubst (s3loc-path loc) (cadr rule))))))))

(define (s3loc/content loc (text #t) (headers '()) (err s3errs))
  (when (string? loc) (set! loc (->s3loc loc)))
  (try (if text (filestring (s3loc/filename loc))
	   (filedata (s3loc/filename loc)))
       (let* ((req (s3/op "GET" (s3loc-bucket loc) (s3loc-path loc)
		     err "" "text" headers ))
	      (status (get req 'response)))
	 (if (and status (>= 299 status 200))
	     (get req '%content)
	     (and err (irritant req S3FAILURE S3LOC/CONTENT
				(s3loc->string loc)))))))
(define s3/get s3loc/content)

;;;; Downloads

(define (s3/download! src (dest (getcwd)))
  (%watch "S3/DOWNLOAD!" src dest)
  (when (string? src) (set! src (->s3loc src)))
  (when (not dest)
    (set! dest (tempdir))
    (message "Writing files to " dest))
  (if (and (has-suffix (s3loc-path src) "/") (file-exists? dest)
	   (not (file-directory? dest)))
      (error "Destination is not a directory" dest)
      (mkdirs (if (has-suffix dest "/") dest (glom dest "/"))))
  (when (s3/exists? src)
    (message "Downloading " src " to "
	     (mkpath dest (basename (s3loc-path src))))
    (write-file (mkpath dest (basename (s3loc-path src))) (s3/get src)))
  (do-choices (down (s3/list src))
    (if (has-suffix (s3loc-path down) "/")
	(s3/download! down (mkpath dest (basename (slice (s3loc-path down) 0 -1))))
	(message "Downloading " down " to "
		 (mkpath dest (basename (s3loc-path down))))
	(write-file (mkpath dest (basename (s3loc-path down))) (s3/get down)))))

;;; Some test code

(comment
 (s3/op "GET" "data.beingmeta.com" "/brico/brico.db" #t "")
 (s3/op "PUT" "public.sbooks.net" "/uspto/6285999" content #t "text/html"
	(list "x-amz-acl: public-read")))

