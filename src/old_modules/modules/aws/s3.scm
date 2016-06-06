;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'aws/s3)

(use-module '{aws aws/v4 fdweb texttools mimetable regex logctl
	      ezrecords rulesets logger varconfig meltcache
	      curlcache mttools})
(define %used_modules '{aws varconfig ezrecords rulesets})

(module-export!
 '{s3loc? s3loc-path s3loc-bucket make-s3loc  s3/mkpath
   ->s3loc s3/loc >s3
   s3loc->string ->s3string >s3s
   s3ish?})
(module-export! '{s3loc s3/getloc s3loc/s3uri make-s3loc
		  s3loc/uri s3loc/filename s3/bytecodes->string})
(module-export! '{s3loc/get s3loc/head s3loc/exists?
		  s3loc/etag s3loc/modified s3loc/info
		  s3loc/content s3loc/put s3loc/copy! s3loc/link!
		  s3loc/modify!})
(module-export! '{s3/get s3/get+ s3/head s3/ctype s3/exists?
		  s3/modified s3/etag  s3/info
		  s3/bucket? s3/copy! s3/link! s3/modify! s3/put
		  s3/download!})
(module-export! '{s3/signature s3/op s3/expected
		  s3/uri s3/signeduri s3/pathuri s3/hosturi})
(module-export! '{s3/axe! s3/copy*! s3/push!})
(module-export! '{s3/getpolicy s3/setpolicy! s3/addpolicy!
		  s3/policy/endmarker})

(define-init %loglevel %notice%)
;;(set! %loglevel %info%)
;;(logctl! 'aws/v4 %info%)
;;(logctl! 'gpath %info%)

(define headcache #f)

(define s3root "s3.amazonaws.com")
(varconfig! s3root s3root)

(define s3scheme "https://")
(varconfig! s3scheme s3scheme)

(define-init s3errs #t)
(varconfig! aws:s3errs s3errs config:boolean)

(define default-usepath #t)
(varconfig! s3pathstyle default-usepath)

(define n-retries 3)
(varconfig! s3:retries n-retries)
(define retry-wait 3)
(varconfig! s3:retry-wait retry-wait)

(define-init s3opts #[])

(define default-ctype "application")

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
		       ("html" "text/html") ("htm" "text/html")
		       ("xhtml" "text/html")
		       ("manifest" "text/manifest")})
      (add! table (car map) (cadr map)))
    table))

(define s3/guessmimetype path->mimetype)
(module-export! 's3/guessmimetype)

(define (encode-path path) (string-subst (uriencode path) "%2F" "/"))

;;; Representing S3 locations

(define (s3loc-string loc)
  (stringout "#%(S3LOC " (write (s3loc-bucket loc))
    (if (> (compound-length loc) 1) (printout " " (write (s3loc-path loc))))
    (if (and (> (compound-length loc) 2) (exists? (s3loc-opts loc)))
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

(define bucket-part
  '(EXPANSIVE #({(islower) (isdigit)}
		(+ {(islower) (isdigit) "-"})
		{(islower) (isdigit)})))
(define bucket-pat `#(,bucket-part (* #("." ,bucket-part))))

(define s3uripat
  `(GREEDY {#("http" {"s:" ":"} "//"
	      (label bucket ,bucket-pat)
	      ".s3.amazonaws.com/"
	      (label path (rest)))
	    #("http" {"s:" ":"} "//s3.amazonaws.com/"
	      (label bucket ,bucket-pat) "/" (label path (rest)))
	    #("s3://" (label bucket ,bucket-pat) "/" (label path (rest)))
	    #("//" (label bucket ,bucket-pat) "/" (label path (rest)))
	    #("s3:" (label bucket ,bucket-pat) {"/" ":"} (label path (rest)))}))
(define (->s3loc input)
  (cond ((s3loc? input) input)
	((not (string? input))
	 (irritant input |S3Loc| ->S3LOC))
	((textmatch s3uripat input)
	  (let ((info (text->frame s3uripat input)))
	    (try (make-s3loc (get info 'bucket) (get info 'path))
		 (error "Can't convert to s3loc" input))))
	 (else (fail))))
(define s3/loc ->s3loc)
(define >s3 ->s3loc)

(define (s3ish? loc)
  (if (string? loc)
      (or (has-prefix loc "s3:")
	  (textmatch s3uripat loc))
      (s3loc? loc)))


(define (s3/mkpath loc path . more)
  (when (string? loc) (set! loc (->s3loc loc)))
  (if (null? more)
      (make-s3loc (s3loc-bucket loc) (mkpath (s3loc-path loc) path)
		  (qc (s3loc/opts loc)))
      (apply s3/mkpath
	     (make-s3loc (s3loc-bucket loc) (mkpath (s3loc-path loc) path)
			 (qc (s3loc/opts loc)))
	     more)))

(define (s3loc->string s3)
  (stringout "s3://" (s3loc-bucket s3) (s3loc-path s3)))
(define (->s3string x) (s3loc->string (->s3loc x)))
(define >s3s ->s3string)

;;; Computing signatures for S3 calls

(define (s3/signature op bucket path (date (gmtimestamp))
		      (headers '())
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
    (debug%watch (hmac-sha1 aws:secret sigstring)
      sigstring aws:secret)))

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

(define (s3op op bucket path (content #f) (ctype) 
	      (headerlist '()) (opts #[]) args)
  (default! ctype
    (path->mimetype path (if (packet? content) "application" "text")
		    (getopt opts 'mimetable #f)))
  (let* ((date (gmtimestamp))
	 (endpoint (glom s3scheme s3root "/" bucket path))
	 (urlparams (frame-create #f 'header "Expect:"))
	 (headers (headerlist->headers headerlist))
	 (params (args->params args)))
    (when (and content (overlaps? op {"GET" "HEAD"}))
      (add! urlparams 'content-type ctype))
    (store! urlparams 'method (string->symbol op))
    (when (>= %loglevel %detail%) (add! urlparams 'verbose #t))
    (loginfo |S3OP| op " " bucket ":" path " "
	     (when (and content (not (= (length content) 0)))
	       (glom " ("
		 (if ctype ctype "content") ", " (length content) 
		 (if (string? content) " characters)" " bytes)")))
	     (if (null? headers) " headers=()" " headers=\n\t") headers
	     (if (null? params) " params=()" " params=\n\t") params
	     "\n\tendpoint:\t" endpoint)
    (aws/v4/op (frame-create #f)
	       op endpoint opts params headers
	       content ctype date urlparams)))

(define (headerlist->headers headerlist)
  (let ((headers (frame-create #f)))
    (dolist (header headerlist)
      (if (and (string? header) (position #\: header))
	  (store! headers (slice header 0 (position #\: header))
		  (trim-spaces (slice header (1+ (position #\: header)))))
	  (if (table? header)
	      (do-choices (key (getkeys header))
		(store! headers key (get header key)))
	      (irritant header "Bad HTTP header"))))
    headers))

(define (args->params args)
  (let ((params (frame-create #f))
	(scan args))
    (while (and (pair? scan) (pair? (cdr scan)))
      (cond ((or (string? (car scan)) (string? (cdr scan)))
	     (store! params (car scan) (stringout (cadr scan)))
	     (set! scan (cddr scan)))
	    ((table? (car scan))
	     (do-choices (key (getkeys (car scan)))
	       (store! params key (stringout (get (car scan) key))))
	     (set! scan (cdr scan)))
	    (else (set! scan (cdr scan)))))
    params))

(define (s3/op op bucket path (opts #f)
	       (content "") (ctype) (headers '()) . args)
  (default! ctype
    (path->mimetype path (if (packet? content) "application" "text")
		    (getopt opts 'mimetable #f)))
  (when (has-prefix bucket {"." "/"}) (set! bucket (slice bucket 1)))
  (if opts (set! opts (cons opts s3opts)) (set! opts s3opts))
  (let* ((err (getopt opts 'errs (getopt opts 's3errs s3errs)))
	 (retries (getopt opts 'retry n-retries))
	 (tries 0)
	 (wait 1)
	 (s3result
	  (s3op op bucket path content ctype headers opts args)
	  #|
	  (if (and retries (> retries 0))
	      (onerror (s3op op bucket path content ctype headers opts args)
		(lambda (ex)
		  (op-failed ex op bucket path)
		  (curlcache/reset!)
		  (set! tries (1+ tries))
		  (let ((r (error-irritant ex)))
		    (if (and (pair? r) (table? (car r)) (test (car r) 'response))
			r ex))))
	      (s3op op bucket path content ctype headers opts args))
	  |#)
	 (request (and s3result (cdr s3result)))
	 (response (and s3result (car s3result)))
	 (content (and response (get response '%content)))
	 (status (and response (get response 'response)))
	 (retry (and (exists? status) status (>= status 500)))
	 (success (and status (>= 299 status 200))))
    (debug%watch "S3/OP" 
      op bucket path success status retry
      "\nREQUEST" request "\nRESPONSE" response)
    (while (and retry retries (> retries 0))
      (logwarn |S3/Retry|
	"Retrying " op " (" status ") for " path " in " bucket " after:\n"
	(pprint s3result))
      (sleep wait)
      (set! retries (-1+ retries))
      (set! wait (+ wait retry-wait))
      (set! s3result
	    (onerror (s3op op bucket path content ctype headers opts args)
	      (lambda (ex)
		(op-failed ex op bucket path)
		(curlcache/reset!)
		(let ((r (error-irritant ex)))
		  (if (and (pair? r) (table? (car r)) (test (car r) 'response))
		      r ex)))))
      (set! request (and s3result (cdr s3result)))
      (set! response (and s3result (car s3result)))
      (set! content (and response (get response '%content)))
      (set! status (and response (get response 'response)))
      (set! retry (or (not status) (>= status 500)))
      (set! success (and status (>= 299 status 200))))
    (when (and success (equal? op "GET") (exists? content))
      (loginfo |S3OP/result| "GET s3://" bucket "/" path
	       "\n\treturned " (length content)
	       (if (string? content) " characters of " " bytes of ")
	       (try (get response 'content-type)
		    "stuff")))
    (unless success
      (onerror (store! response '%content (xmlparse content))
	(lambda (ex) #f)))
    (cond (success response)
	  ;; Don't generate warnings for HEAD probes
	  ((equal? op "HEAD") response)
	  ((testopt opts 'ignore status) response)
	  ((testopt opts 's3pass status) response)
	  (err (s3-error status s3result op bucket path opts))
	  (else (s3-warn status request response op bucket path opts)
		response))))

(define (s3-error status s3result op bucket path opts)
  (set! s3result (cons* (car s3result) (cdr s3result)
			(if (exists? opts)
			    (if (pair? opts) opts (list opts))
			    '())))
  (cond ((not status)
	 (irritant s3result |S3/Failure| S3/OP
		   op " " "s3://" bucket
		   (if (has-prefix path "/") "" "/")
		   path))
	((= status 404)
	 (irritant s3result |S3/NotFound| S3/OP
		   op " " "s3://" bucket
		   (if (has-prefix path "/") "" "/") path))
	((= status 403)
	 (irritant s3result |S3/Forbidden| S3/OP
		   op " " "s3://" bucket
		   (if (has-prefix path "/") "" "/") path))
	(else (irritant s3result |S3/Failure| S3/OP
			op " " "s3://" bucket
			(if (has-prefix path "/") "" "/")
			path))))

(define (s3-warn status request response op bucket path opts)
  (cond ((= status 404)
	 (lognotice |S3/NotFound| 
	   op " " (glom "s3://" bucket
		    (if (has-prefix path "/") "" "/")
		    path)
	   (if (or (not opts) (empty? (getkeys opts)))
	       " (no opts)"
	       (printout "\n\t" (pprint opts)))
	   "\nREQ\t" (pprint request)
	   "\nRESP\t" (pprint response)))
	((= status 403)
	 (logwarn |S3/Forbidden|
	   op " " (glom "s3://" bucket "/" path)
	   (if (or (not opts) (empty? (getkeys opts)))
	       " (no opts)"
	       (printout "\n\t" (pprint opts)))
	   "\nREQ\t" (pprint request)
	   "\nRESP\t" (pprint response)))
	(else
	 (logwarn |S3/Failure|
	   op " " (glom "s3://" bucket
		    (if (has-prefix path "/") "" "/")
		    path) " "
	   (try (get request 'header) "")
	   (if (or (not opts) (empty? (getkeys opts)))
	       " (no opts)"
	       (printout "\n\t" (pprint opts)))
	   "\nREQ\t" (pprint request)
	   "\nRESP\t" (pprint response)))))

(define (s3/expected response)
  (->string (map (lambda (x) (integer->char (string->number x 16)))
		 (segment (car (get (xmlget (xmlparse (get response '%content))
					    'stringtosignbytes)
				    '%content))))))

(define (op-failed ex op bucket path)
  (logwarn |S3/Error|
    "Error on " OP " to s3://" bucket "/" path "\n\t"
    (error-condition ex)
    (when (error-context ex) (printout " @" (error-context ex)))
    (when (error-details ex)
      (printout " (" (error-details ex) ") "))
    (when (error-irritant ex)
      (printout "\n\t" (pprint (error-irritant ex))))))

;;; Getting S3 URIs for the API

(define (s3/uri bucket path (scheme s3scheme) (usepath))
  (when (not scheme) (set! scheme s3scheme))
  (unless (has-suffix scheme "://") (set! scheme (fix-scheme scheme)))
  (default! usepath (if (equal? scheme "http://") #t default-usepath))
  (if usepath
      (stringout scheme s3root "/" bucket path)
      (if (and (hashset-get website-buckets bucket)
	       (equal? scheme "http://"))
	  (stringout scheme bucket path)
	  (stringout scheme bucket "." s3root path))))
(define (s3/pathuri bucket path (scheme s3scheme))
  (unless (has-suffix scheme "://") (set! scheme (fix-scheme scheme)))
  (s3/uri bucket path scheme #t))
(define (s3/hosturi bucket path (scheme s3scheme))
  (unless (has-suffix scheme "://") (set! scheme (fix-scheme scheme)))
  (s3/uri bucket path scheme #f))

(define (fix-scheme scheme)
  (if (has-suffix scheme "://") scheme
      (if (has-suffix scheme ":")
	  (glom scheme "//")
	  (glom scheme "://"))))

;;; Getting signed URIs

(define (signeduri bucket path (opts #f)
		   (expires (* 17 3600))
		   (op "GET") (headers '())
		   (usepath) (scheme))
  (unless (has-prefix path "/") (set! path (glom "/" path)))
  (default! usepath (position #\. bucket))
  (default! scheme
    (getopt opts 'scheme
	    (if (testopt opts '{ssl https}) "https"
		s3scheme)))
  (unless (has-suffix scheme "://") (set! scheme (fix-scheme scheme)))
  (info%watch "SIGNEDURI" op bucket path scheme expires headers usepath)
  (let* ((endpoint (if usepath
		       (glom scheme s3root "/" bucket path)
		       (glom scheme bucket "." s3root path)))
	 (host (if usepath s3root (glom bucket "." s3root)))
	 (seconds (if (number? expires)
		      (if (> expires (time)) 
			  (- expires (time))
			  expires)
		      (if (timestamp? expires)
			  (time-until expires)
			  (* 17 3600))))
	 (req (aws/v4/prepare 
	       `#[%params {"X-Amz-Credential" 
			   "X-Amz-SignedHeaders"
			   "X-Amz-Algorithm"
			   "X-Amz-Security-Token"
			   "X-Amz-Expires"
			   "X-Amz-Date"}
		  %headers host
		  "X-Amz-Expires" ,(max (inexact->exact (floor seconds)) 0)]
	       op endpoint opts #f #f)))
    (scripturl endpoint
	"X-Amz-Credential" (get req "X-Amz-Credential") 
	"X-Amz-SignedHeaders" (get req "X-Amz-SignedHeaders")
	"X-Amz-Algorithm" (get req "X-Amz-Algorithm")
	"X-Amz-Expires" (get req "X-Amz-Expires")
	"X-Amz-Date" (get req "X-Amz-Date")
	"X-Amz-Security-Token" (get req "X-Amz-Security-Token")
	"X-Amz-Signature" 
	(downcase (packet->base16 (get req 'signature))))))
(define (s3/signeduri arg . args)
  (if (or (null? args) (s3loc? arg)
	  (and (string? arg) (has-prefix arg {"s3:" "http:" "https:"}))
	  (number? (car args)) (timestamp? (car args)))
      (let* ((optarg (and (pair? args) (car args)))
	     (opts (tryif (and (table? optarg) (not (timestamp? optarg)))
		     optarg))
	     (expires
	      (cond (opts (getopt opts 'expires (* 48 3600)))
		    ((not optarg) (* 48 3600))
		    ((number? optarg) (timestamp+ (car args)))
		    ((timestamp? optarg) optarg)
		    (else
		     (irritant optarg |BadOptArg| s3/signeduri))))
	     (s3loc (->s3loc arg)))
	(signeduri (s3loc-bucket s3loc) (s3loc-path s3loc) 
		   (try opts #f) expires "GET"
		   (try (getopt opts 'headers 
				(get (s3loc/opts s3loc) 'headers))
			'())
		   ;; Use 'path' syntax for dotted buckets so that you
		   ;; can use https
		   (position #\. (s3loc-bucket s3loc))))
      (apply signeduri arg args)))

;;; Operations

(define (s3/write! loc content (ctype #f) (headers) (opts #f))
  (when (string? loc) (set! loc (->s3loc loc)))
  (unless ctype
    (set! ctype
	  (path->mimetype (s3loc-path loc)
			  (if (packet? content) "application" "text")
			  (getopt opts 'mimetable #f))))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (when (not content) (error |NoContent| "No content to S3/WRITE"))
  (debug%watch
      (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc)
	opts content ctype headers)
    loc ctype headers))

(define (s3/delete! loc (headers) (opts #f))
  (when (string? loc) (set! loc (->s3loc loc)))
  ;; '(("x-amx-acl" . "public-read"))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (s3/op "DELETE" (s3loc-bucket loc)
	 (s3loc-path loc) opts "" #f headers))

(define (s3/bucket? loc (headers))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
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
  (if (position #\. (s3loc-bucket s3loc))
      (stringout s3scheme s3root "/"  (s3loc-bucket s3loc) (s3loc-path s3loc))
      (stringout s3scheme  (s3loc-bucket s3loc) "." s3root (s3loc-path s3loc))))

(define (s3loc/s3uri s3loc)
  (if (string? s3loc) (set! s3loc (->s3loc s3loc)))
  (stringout "s3://" (s3loc-bucket s3loc) (s3loc-path s3loc)))

;;; Basic S3LOC network methods

(define (s3loc/get loc (headers) (opts))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (default! opts
    (if (exists? (get (s3loc/opts loc) 'errs))
	#[errs #t]
	s3opts))
  (when (not (table? opts)) (set! opts `#[errs ,opts]))
  (if headcache
      (let ((req (s3/op "GET" (s3loc-bucket loc) (s3loc-path loc)
		   opts "" "text" headers)))
	(when headcache
	  (let ((copy (deep-copy req)))
	    (drop! copy '%content)
	    (meltcache/store headcache copy s3head (list loc headers))))
	req)
    (s3/op "GET" (s3loc-bucket loc) (s3loc-path loc) opts "" "text" headers)))

(define (s3loc/head loc (headers))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (if headcache
      (meltcache/get headcache s3head loc headers)
      (s3head loc headers)))
(define s3/head s3loc/head)

(define (s3head loc headers)
  (s3/op "HEAD" (s3loc-bucket loc) (s3loc-path loc) #[errs #f] "" ""
	 headers))
(config! 'meltpoint (cons 's3head 300))

(config-def! 's3:headcache
	     (lambda (var (val))
	       (cond ((not (bound? val)) headcache)
		     ((integer? val)
		      (config! 'meltpoint (cons 's3head val))
		      (unless (hashtable? headcache)
			(set! headcache (make-hashtable))))
		     ((hashtable? val) (set! headcache val))
		     ((string? val)
		      (error "Meltcache reloads not yet supported"))
		     (else
		      (set! headcache (make-hashtable))))))

(define (s3loc/get+ loc (text #t) (headers) (opts #f))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (let* ((req (s3/op "GET" (s3loc-bucket loc) (s3loc-path loc)
		opts "" "text" headers))
	 (err (getopt opts 'errs (getopt opts 's3errs s3errs)))
	 (status (get req 'response)))
    (when headcache
      (let ((copy (deep-copy req)))
	(drop! copy '%content)
	(meltcache/store headcache copy s3head (list loc headers))))
    (if (and status (>= 299 status 200))
	`#[content ,(get req '%content)
	   ctype ,(try (get req 'content-type)
		       (getopt opts 'mimetype {})
		       (path->mimetype (s3loc-path loc)
				       (if text "text" "application")
				       (getopt opts 'mimetable #f))
		       (if text "text" "application"))
	   encoding ,(get req 'content-encoding)
	   modified ,(try (get req 'last-modified) (timestamp))
	   etag ,(try (get req 'etag) (md5 (get req '%content)))]
	(and err (irritant req S3FAILURE S3LOC/CONTENT
			   (s3loc->string loc))))))
(define s3/get+ s3loc/get+)

;;; Basic S3 network metadata methods

(define (s3loc/modified loc (headers))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (let ((info (s3loc/head loc headers)))
    (try (get info 'last-modified) #f)))
(define s3/modified s3loc/modified)

(define (s3loc/exists? loc (headers))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (let ((info (s3loc/head loc headers)))
    (response/ok? info)))
(define s3/exists? s3loc/exists?)

(define (s3loc/etag loc (compute #f) (headers))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (let ((req (s3loc/head loc headers)))
    (and (response/ok? req)
	 (try (get req 'etag) (and compute (md5 (s3loc/content loc)))))))
(define s3/etag s3loc/etag)

(define (s3loc/ctype loc)
  (when (string? loc) (set! loc (->s3loc loc)))
  (get (s3loc/head loc) 'content-type))
(define s3/ctype s3loc/ctype)

(define etag-pat #((bos) {"" "\"" "&quot;"} (label hash (isxdigit+))))

(define (s3loc/info loc (headers) (text #f) (opts #f))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers)
			 (getopt opts 'headers '())))
  (let ((req (s3/head loc headers))
	(mimetable (getopt opts 'mimetable #f)))
    (debug%watch "S3LOC/INFO" loc req)
    (and (response/ok? req)
	 (frame-create #f
	   'path (s3loc/s3uri loc)
	   'ctype (try (get req 'content-type)
		       (path->mimetype (s3loc-path loc)
				       (if text "text" "application")
				       mimetable)
		       (if text "text" "application"))
	   'size (get req 'content-length)
	   'encoding (get req 'content-encoding)
	   'modified (try (get req 'last-modified) (timestamp))
	   'etag (try (get req 'etag) 
		      (packet->base16 (md5 (get req '%content))))
	   'hash (try (base16->packet (get req 'x-amz-meta-md5))
		      (base16->packet (get (text->frames etag-pat (get req 'etag)) 'hash))
		      (md5 (get req '%content)))))))
(define s3/info s3loc/info)

;;; Basic S3 network write methods

(define (s3loc/put loc content (ctype) (headers) (opts #f))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! ctype
    (path->mimetype (s3loc-path loc)
		    (if (packet? content) "application" "text")
		    (getopt opts 'mimetable #f)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (when (testopt opts 'headers)
    (set! headers (append (getopt opts 'headers) headers)))
  (when (not content) (error |NoContent| "No content to S3LOC/PUT"))
  (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc) opts
    content ctype headers))
(define s3/put s3loc/put)

(define (s3loc/copy! from to (opts #f) (inheaders #f) (outheaders #f))
  (when (string? to) (set! to (->s3loc to)))
  (when (string? from) (set! from (->s3loc from)))
  (unless inheaders
    (set! inheaders (try (get (s3loc/opts from) 'headers) '())))
  (unless outheaders
    (set! outheaders (try (get (s3loc/opts to) 'headers) '())))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts to) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts to) s3opts)  s3opts)))

  (when (testopt opts 'inheaders)
    (set! inheaders (append (getopt opts 'inheaders) inheaders)))
  (when (testopt opts 'outheaders)
    (set! outheaders (append (getopt opts 'outheaders) outheaders)))

  (let* ((head (s3loc/head from inheaders))
	 (mimetable (getopt opts 'mimetable #f))
	 (ctype (try (get head 'content-type)
		     (path->mimetype
		      (s3loc-path to)
		      (path->mimetype (s3loc-path from) "text" mimetable)
		      mimetable))))
    (loginfo |S3/copy| "Copying " (s3loc->string from)
	     " to " (s3loc->string to))
    (s3/op "PUT" (s3loc-bucket to) (s3loc-path to) opts "" ctype
	   `(("x-amz-copy-source" .
	      ,(stringout "/" (s3loc-bucket from) 
		 (encode-path (s3loc-path from))))
	     ("x-amz-metadata-directive" . "COPY")
	     ,@inheaders
	     ,@outheaders))))
(define s3/copy! s3loc/copy!)

(define (s3loc/link! src loc (opts #f))
  (when (string? loc) (set! loc (->s3loc loc)))
  (when (and (string? src) (not (has-prefix src {"http:" "https:" "ftp:"})))
    (set! src (->s3loc src)))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (let* ((head (tryif (s3loc? src) (s3loc/head src)))
	 (mimetable (getopt opts 'mimetable #f))
	 (ctype (try (get head 'content-type)
		     (path->mimetype
		      (s3loc-path loc)
		      (path->mimetype
		       (if (s3loc? src) (s3loc-path src)
			   (uripath src))
		       "text" mimetable)
		      mimetable))))
    (loginfo |S3/link| "Linking (via redirect) " src " to " loc)
    (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc) opts "" ctype
	   `(("x-amz-website-redirect-location" .
	      ,(if (s3loc? src)
		   (stringout "/" (s3loc-bucket src) (s3loc-path src))
		   src))))))
(define s3/link! s3loc/link!)

(define (s3loc/modify! loc (new #[]) (opts))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! opts
    (if (exists? (get (s3loc/opts loc) 'err))
	(cons #[errs #t] s3opts)
	s3opts))
  (when (not (table? opts)) (set! opts (cons `#[errs ,opts] s3opts)))
  (let* ((inheaders (getopt opts 'inheaders '()))
	 (head (s3loc/head loc inheaders))
	 (mimetable (getopt opts 'mimetable #f))
	 (ctype (try (get new 'content-type)
		     (get new 'mimetype)
		     (get new 'ctype)
		     (get new 'type)
		     (get head 'content-type)
		     (path->mimetype (s3loc-path loc) "text" mimetable)))
	 (disposition (try (get new 'content-disposition)
			   (get new 'disposition)
			   (get head 'content-disposition)))
	 (encoding (try (get new 'content-encoding)
			(get new 'encoding)
			(get head 'content-encoding)))
	 (outheaders '()))
    (when (exists? disposition)
      (set! outheaders 
	    (cons (cons "content-disposition" disposition) outheaders)))
    (when (exists? encoding)
      (set! outheaders 
	    (cons (cons "content-encoding" encoding)
		  outheaders)))
    (do-choices (key (pickstrings (getkeys new)))
      (set! outheaders (cons (cons key (get new key)) outheaders)))
    (lognotice |S3/modify| "Modifying " ctype " " (s3loc->string loc)
	       " metadata to " outheaders)
    (s3/op "PUT" (s3loc-bucket loc) (s3loc-path loc) opts "" ctype
	   `(("x-amz-copy-source" .
	      ,(stringout "/" (s3loc-bucket loc) 
		 (encode-path (s3loc-path loc))))
	     ("x-amz-metadata-directive" . "REPLACE")
	     ,@inheaders
	     ,@outheaders))))
(define s3/modify! s3loc/modify!)

;;; Listing loc

(define (list-chunk loc headers opts (delimiter "/") (next #f))
  (if (and next delimiter)
      (s3/op "GET" (s3loc-bucket loc) "/" 
	opts "" "text" headers
	"delimiter" delimiter
	"prefix" (if (has-prefix (s3loc-path loc) "/")
		     (slice (s3loc-path loc) 1)
		     (s3loc-path loc))
	"marker" next)
      (if next
	  (s3/op "GET" (s3loc-bucket loc) "/" 
	    opts "" "text" headers
	    "prefix" (if (has-prefix (s3loc-path loc) "/")
			 (slice (s3loc-path loc) 1)
			 (s3loc-path loc))
	    "marker" next)
	  (if delimiter
	      (s3/op "GET" (s3loc-bucket loc) "/" 
		opts "" "text" headers
		"delimiter" delimiter
		"prefix" (if (has-prefix (s3loc-path loc) "/")
			     (slice (s3loc-path loc) 1)
			     (s3loc-path loc)))
	      (s3/op "GET" (s3loc-bucket loc) "/" 
		opts "" "text" headers
		"prefix" (if (has-prefix (s3loc-path loc) "/")
			     (slice (s3loc-path loc) 1)
			     (s3loc-path loc)))))))

;;; Working with S3 'dirs'

(define (s3/list loc (headers) (opts #f) (next #f))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (when (testopt opts 'headers)
    (set! headers (append headers (getopt opts 'headers))))
  (let* ((req (list-chunk loc headers opts "/" next))
	 (content (reject (elts (xmlparse (get req '%content) 'data)) string?))
	 (keys (get (get content 'contents) 'key))
	 (selfpath (slice (s3loc-path loc) 1))
	 (next (try (get content 'nextmarker) #f)))
    (choice
     (for-choices (path (get (get content 'commonprefixes) 'prefix))
       (make-s3loc (s3loc-bucket loc) path))
     (for-choices (path (difference keys selfpath))
       (make-s3loc (s3loc-bucket loc) path))
     (tryif next (s3/list loc headers opts next)))))
(module-export! 's3/list)

(define (s3/list+ loc (headers) (opts #f) (next #f))
  (when (string? loc) (set! loc (->s3loc loc)))
  (default! headers (try (get (s3loc/opts loc) 'headers) '()))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (when (testopt opts 'headers)
    (set! headers (append headers (getopt opts 'headers))))
  (let* ((req (list-chunk loc headers opts "/" next))
	 (content (reject (elts (xmlparse (get req '%content) 'data)) string?))
	 (next (try (get content 'nextmarker) #f)))
    (debug%watch "S3/LIST+" content)
    (choice
     (for-choices (elt (get content 'contents))
       `#[key ,(get elt 'key) name ,(basename (get elt 'key))
	  loc ,(make-s3loc (s3loc-bucket loc) (get elt 'key))
	  size ,(string->number (get elt 'size))
	  modified ,(timestamp (get elt 'lastmodified))
	  etag ,(slice (decode-entities (get elt 'etag)) 1 -1)
	  content-type ,(get elt 'content-type)
	  content-encoding ,(get elt 'content-encoding)
	  hash ,(try (base16->packet (get req 'x-amz-meta-md5))
		     (base16->packet
		      (get (text->frames etag-pat (get elt 'etag)) 'hash)))])
     (tryif next (s3/list+ loc headers opts next)))))
(module-export! 's3/list+)

;;; Recursive deletion

(define (s3/axe! loc (headers '()) (opts #f) (pause (config 's3:pause #f)))
  (when (string? loc) (set! loc (->s3loc loc)))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (when (testopt opts 'headers)
    (set! headers (append headers (getopt opts 'headers))))
  (let ((paths (s3/list loc '() opts)))
    (do-choices (path paths)
      (if (has-suffix (s3loc-path path) "/")
	  (s3/axe! path headers)
	  (begin (s3/delete! path headers)
	    (when pause (sleep pause)))))))
(module-export! 's3/axe!)

;;; Recursive copying

(define (get-tail-dir string)
  (slice (gather #("/" (not> "/") "/" (eos)) string) 1))

(define (s3/copy*! from to (opts #f)
		   (inheaders #f) (outheaders #f) 
		   (pause (config 's3:pause #f))
		   (listheaders '()))
  (when (string? from) (set! from (->s3loc from)))
  (when (string? to) (set! to (->s3loc to)))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts to) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts to) s3opts)  s3opts)))
  (unless inheaders
    (set! inheaders (try (getopt (s3loc/opts from) 'headers)
			 (getopt opts 'headers '()))))
  (unless outheaders
    (set! outheaders (try (get (s3loc/opts from) 'headers)
			  (getopt opts 'headers '()))))
  (unless listheaders
    (set! listheaders (try (get (s3loc/opts from) 'headers)
			   (getopt opts 'headers '()))))
  
  (when (testopt opts 'inheaders)
    (set! listheaders (append listheaders (getopt opts 'inheaders))))
  (when (testopt opts 'outheaders)
    (set! listheaders (append outheaders (getopt opts 'outheaders))))
  (when (testopt opts 'listheaders)
    (set! listheaders (append listheaders (getopt opts 'listheaders))))

  (loginfo |S3/copy*| "Copying tree " (s3loc->string from)
	   " to " (s3loc->string to))
  
  (let ((paths (difference (s3/list from listheaders) from)))
    (loginfo |S3/copy*| "Copying " (choice-size paths) " items from " (s3loc->string from) ": "
	     (s3loc->string paths))
    (do-choices (path paths)
      (if (has-suffix (s3loc-path path) "/")
	  (s3/copy*! path
		     (s3/mkpath to (get-tail-dir (s3loc-path path)))
		     opts inheaders outheaders)
	  (begin (s3/copy! path (s3/mkpath to (basename (s3loc-path path)))
		      opts inheaders outheaders)
	    (when pause (sleep pause)))))))

;;; Synchronizing

(define (s3/push! dir s3loc (match #f) (opts #f))
  (if (string? s3loc) (set! s3loc (->s3loc s3loc)))
  (let* ((opts (if opts
		   (cons opts (try (cons (s3loc/opts s3loc) s3opts)  s3opts))
		   (try (cons (s3loc/opts s3loc) s3opts)  s3opts)))
	 (pause (getopt opts 'pause (config 's3:pause #f)))
	 (headers (getopt opts 'headers '()))
	 (nthreads (getopt opts 'threads (config 's3:pushthreads 1)))
	 (forcewrite (getopt opts 'writeall))
	 (mimetable (getopt opts 'mimetable #f))
	 (%loglevel (getopt opts 'loglevel %loglevel)))
    (let* ((s3info (s3/list+ s3loc))
	   (filenames (reject (reject (getfiles dir) has-prefix "#")
			      has-suffix "#"))
	   (nomatch (and match
			 (try (cadr (pick (pick match pair?) car 'not))
			      #f)))
	   (match (and match
		       (choice (reject match pair?)
			       (reject (pick match pair?) car 'not))))
	   (strmatch (tryif match (pickstrings match)))
	   (rxmatch (tryif match (pick match regex?)))
	   (patmatch (tryif match (pick match {vector? pair?})))
	   (strmatch- (tryif nomatch (pickstrings nomatch)))
	   (rxmatch- (tryif nomatch (pick nomatch regex?)))
	   (patmatch- (tryif nomatch (pick nomatch {vector? pair?})))
	   (copynames
	    (filter-choices (file filenames)
	      (and (not (has-suffix file "/"))
		   (or match (not (has-prefix (basename file) ".")))
		   (or match (not (has-suffix (basename file) "~")))
		   (not (has-prefix (basename file) "#"))
		   (not (has-suffix (basename file) "#"))
		   (or (not nomatch)
		       (not (or (has-suffix file strmatch-)
				(has-prefix file
					    (reject strmatch- has-prefix "."))
				(has-prefix (basename file)
					    (reject strmatch has-prefix "."))
				(exists regex/match rxmatch file)
				(exists textmatch patmatch file))))
		   (not (has-prefix (basename file) "."))
		   (not (has-suffix (basename file) "~"))
		   (or (fail? {strmatch rxmatch patmatch})
		       (has-suffix file strmatch)
		       (has-prefix file (reject strmatch has-prefix "."))
		       (has-prefix (basename file)
				   (reject strmatch has-prefix "."))
		       (exists regex/match rxmatch file)
		       (exists textmatch patmatch file)))))
	   (updated {}))
      (if (and (exists? match) match)
	  (lognotice |S3/push|
	    "Syncing " (choice-size copynames) "/" (choice-size filenames)
	    " matching files in " (write dir)
	    "\n to " (if (string? s3loc) s3loc (s3loc->string s3loc)))
	  (lognotice |S3/push|
	    "Syncing " (choice-size copynames) " files in " dir
	    "\n to " (if (string? s3loc) s3loc (s3loc->string s3loc))))
      (debug%watch "S3/PUSH!/select" 
	nomatch match strmatch rxmatch patmatch rxmatch- patmatch-
	"\n" filenames "\n" copynames)
      (do-choices-mt (file copynames nthreads)
	(let* ((info (pick s3info 'name (basename file)))
	       (loc (try (get info 'loc) (s3/mkpath s3loc (basename file))))
	       (encoding (path->encoding file))
	       (mimetype (getopt opts 'mimetype
				 (path->mimetype file #f mimetable))))
	  (logdebug |S3/push|
	    "Syncing " (try mimetype "unknown type") " " (write file)
	    " to " (s3loc->string loc) ":\n info: " info)
	  (if (or (fail? info) forcewrite)
	      (let ((data (if (and (not encoding)
				   (exists? mimetype) mimetype
				   (or (has-prefix mimetype "text")
				       (mimetype/text? mimetype)))
			      (filestring file)
			      (filedata file))))
		(loginfo |S3/push| "Pushing " 
			 (length data) 
			 (if (mimetype/text? mimetype) 
			     " characters "
			     " bytes ")
			 " to " loc)
		(s3/write! loc data (try mimetype "application")
			   (data-headers data encoding headers))
		(set+! updated loc)
		(when pause (sleep pause)))
	      (let ((data (if (and (not encoding)
				   (exists? mimetype) mimetype
				   (or (has-prefix mimetype "text")
				       (mimetype/text? mimetype)))
			      (filestring file)
			      (filedata file))))
		(if (and (= (get info 'size) (file-size file))
			 (or (not (getopt opts 'testhash #t))
			     (not (test info '{hash etag md5}))
			     (overlaps? (stdhash (get info '{hash etag md5}))
					(stdhash (md5 data))))
			 ;; (test info 'content-type mimetype)
			 ;; (or (not encoding) (test info 'content-encoding encoding))
			 )
		    (logdebug |S3/push| "Skipping unchanged " loc)
		    (begin (loginfo |S3/push| "Pushing to " loc)
		      (s3/write! loc data mimetype (data-headers data encoding headers))
		      (set+! updated loc)
		      (when pause (sleep pause))))))))
      (if (fail? updated)
	  (lognotice |S3/push|
	    "No changes among " (choice-size copynames)
	    (when match (printout "/" (choice-size filenames)) " matching")
	    " files in " (write dir))
	  (lognotice |S3/push|
	    "Updated " (choice-size updated) "/" (choice-size copynames)
	    (when match (printout "/" (choice-size filenames) " matching"))
	    " files to " (s3loc->string s3loc)))
      updated)))

(define (data-headers data encoding headers)
  (cons (cons "x-amz-meta-md5" (packet->base16 (md5 data)))
	(if encoding
	    (cons (cons "content-encoding" encoding) headers)
	    headers)))

(define (stdhash x)
  (try
   (if (packet? x) (downcase (packet->base16 x))
       (if (string? x)
	   (downcase (get (text->frames etag-pat x) 'hash)) 
	   (fail)))
   #f))

(define (dowrite loc data mimetype headers)
  (logdebug |S3/writing| "Writing " (length data) " "
	    (if (packet? data) "bytes" "characters")
	    " of " mimetype " to " (s3loc->string loc)
	    (unless (null? headers)
	      (printout "\n\twith " headers)))
  (s3/write! loc data mimetype headers))

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

(define (s3loc/content loc (text #t) (headers '()) (opts #f))
  (when (string? loc) (set! loc (->s3loc loc)))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (try (if text (filestring (s3loc/filename loc))
	   (filedata (s3loc/filename loc)))
       (let* ((req (s3/op "GET" (s3loc-bucket loc) (s3loc-path loc)
		     opts "" "text" headers ))
	      (status (get req 'response)))
	 (if (and status (>= 299 status 200))
	     (get req '%content)
	     (and (if (table? opts) (getopt opts 'errs) opts)
		  (irritant req S3FAILURE S3LOC/CONTENT
				(s3loc->string loc)))))))
(define s3/get s3loc/content)

;;;; Downloads

(define (s3/download! src (dest (getcwd)))
  (info%watch "S3/DOWNLOAD!" src dest)
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

;;; Manipulating policies

(define policy-template (filestring (get-component "s3policy.json")))

(define (generate-policy template loc (account aws:account)
			 (id (uuid->string (getuuid))))
  (when (and (string? loc) (has-prefix loc "s3:"))
    (set! loc (->s3loc loc)))
  (string-subst* template
    "%bucket%" (if (string? loc) loc (s3loc-bucket loc))
    "%path%" (if (s3loc? loc) (s3loc-path loc) "/")
    "%id%" id "%account%" account))

(define (s3/getpolicy bucket)
  (when (and (string? bucket) (has-prefix bucket "s3:"))
    (set! bucket (->s3loc bucket)))
  (let* ((bucketname (if (string? bucket) bucket (s3loc-bucket bucket)))
	 (fetched (s3/op "GET" bucketname "/"
		    #[errs #f ignore 404 usepath #f] #f #f '()
		    "policy")))
    (and (test fetched 'response 200)
	 (get fetched '%content))))
(define (s3/setpolicy! bucket string)
  (notice%watch "S3/SETPOLICY!" bucket string)
  (when (and (string? bucket) (has-prefix bucket "s3:"))
    (set! bucket (->s3loc bucket)))
  (let ((bucketname (if (string? bucket) bucket (s3loc-bucket bucket))))
    (s3/op "PUT" bucketname "/" #[errs #t usepath #f]
	   string "application/json" '() "policy")))

(define (s3/addpolicy! loc add (init-policy policy-template) (opts #f))
  (if opts
      (set! opts (cons opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
      (set! opts (try (cons (s3loc/opts loc) s3opts)  s3opts)))
  (let* ((bucket (s3loc-bucket loc))
	 (path (s3loc-path loc))
	 (policy (s3/getpolicy bucket)))
    (unless (string? policy)
      (set! policy (generate-policy (or init-policy policy-template) loc)))
    (when (has-prefix path "/") (set! path (slice path 1)))
    (when (has-suffix path "/") (set! path (slice path 0 -1)))
    (let* ((statement
	    (string-subst* add
	      "%bucket%" bucket "%path%" path
	      "%id%" (uuid->string (getuuid))
	      "%isotime%" (get (gmtimestamp 'seconds) 'iso)
	      "%account%" aws:account))
	   (insert (get-policy-insert-point policy))
	   (newpolicy (glom (slice policy 0 insert)
			statement "," (slice policy insert))))
      (s3/setpolicy! bucket newpolicy))))

(define (get-policy-insert-point policy)
  (let* ((start (textsearch #("\"Statement\":" (spaces*) "[") policy))
	 (end (and start (position #\[ policy start))))
    (and end (1+ end))))

;;; Some test code

(comment
 (s3/op "GET" "data.beingmeta.com" "/brico/brico.db" #t "")
 (s3/op "PUT" "public.sbooks.net" "/uspto/6285999" content #t "text/html"
	(list "x-amz-acl: public-read")))
