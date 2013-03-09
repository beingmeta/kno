;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'aws/aws4)

(use-module '{aws fdweb texttools mimetable crypto stringfmts
	      ezrecords rulesets logger varconfig})

(define-init %loglevel %info!)
;;(define %loglevel %debug!)

(module-export! '{aws4/prepare aws4/get}) ;; aws4/post
(module-export! '{derive-key})

(define default-region "us-east-1")
(define default-service "iam")

(define debug #t)

(define aws-regions
  {"us-east-1" "us-west-1" "us-west-2" "eu-west-1" "ap-southeast-1"
   "ap-southeast-2" "ap-northeast-1" "sa-east-1"})

;;; Doing a GET with AWS4 authentication

(define (aws4/get req endpoint (args #[]) (headers #[]) (payload #f)
		  (curl #[]) (date (gmtimestamp)))
  (add! req '%date date)
  (add! headers 'date (get date 'isobasic))
  (add! headers 'host (urihost endpoint))
  (add! args "AWSAccessKeyId" (getopt req 'key awskey))
  (add! args "Timestamp" (get date 'isobasic))
  (do-choices (key (getkeys args))
    (add! req key (get args key))
    (add! req '%params key))
  (do-choices (key (getkeys headers))
    (let ((v (get headers key)))
      (add! curl 'header (cons key (qc v)))
      (add! req key
	    (if (and (singleton? v) (string? v)) v
		(stringout (do-choices v
			     (if (timestamp? v) (get v 'isobasic)
				 (printout v)))))))
      (add! req '%headers key))
  ;; (add! args "SignatureMethod" "AWS-HMAC-SHA256")
  (set! req  (aws4/prepare req "GET" endpoint (or payload "")))
  ;; (add! args "Signature" (packet->base64 (getopt req 'signature)))
  (add! curl 'header
	(glom "Authorization: AWS4-HMAC-SHA256 Credential=" (getopt req 'credential) ", "
	  "SignedHeaders=" (getopt req 'signed-headers) ", "
	  "Signature=" (downcase (packet->base16 (getopt req 'signature)))))
  (cons (urlget (scripturl+ endpoint args) curl )
	req))

;;; Doing a post with AWS authentication

;;; Not yet working
(define (aws4/post req endpoint (args #[]) (headers #[]) (payload #f)
		  (curl #[VERBOSE #t]) (date (gmtimestamp)))
  (add! req '%date date)
  (add! headers 'date (get date 'isobasic))
  (add! headers 'host (urihost endpoint))
  (add! headers 'content-type "application/x-www-form-urlencoded; charset=utf8")
  (add! args "AWSAccessKeyId" (getopt req 'key awskey))
  (add! args "Timestamp" (get date 'isobasic))
  (do-choices (key (getkeys args))
    (add! req key (get args key))
    (add! req '%params key))
  (do-choices (key (getkeys headers))
    (let ((v (get headers key)))
      (add! curl 'header (cons key (qc v)))
      (add! req key
	    (if (and (singleton? v) (string? v)) v
		(stringout (do-choices v
			     (if (timestamp? v) (get v 'isobasic)
				 (printout v)))))))
      (add! req '%headers key))
  ;; (add! args "SignatureMethod" "AWS-HMAC-SHA256")
  (set! req  (aws4/prepare req "POST" endpoint (or payload "")))
  ;; (add! args "Signature" (packet->base64 (getopt req 'signature)))
  (add! curl 'header
	(glom "Authorization: AWS4-HMAC-SHA256 Credential=" (getopt req 'credential) ", "
	  "SignedHeaders=" (getopt req 'signed-headers) ", "
	  "Signature=" (downcase (packet->base16 (getopt req 'signature)))))
  (cons (urlpost endpoint (curlopen curl) args) req))

;;; GENERATING KEYS, ETC

(define (aws4/prepare req method uri payload)
  (let* ((cq (canonical-query-string req))
	 (chinfo (canonical-headers req))
	 (ch (car chinfo)) (sh (cdr chinfo))
	 (host (getopt req 'host (urihost uri)))
	 (date (gmtimestamp
		(getopt req '%date
			(getopt req 'date
				(gmtimestamp 'seconds)))))
	 (creq (stringout method "\n"
		 "/" (uripath uri) "\n"
		 cq "\n" ch "\n" sh "\n"
		 (downcase (packet->base16 (sha256 payload)))))
	 (region (try (getopt req '%region {})
		      (getopt req 'region {})
		      (pick aws-regions search host)
		      default-region))
	 (service (try (getopt req '%service {})
		       (getopt req 'service {})
		       (slice host 0 (position #\. host))
		       default-service))
	 (string (get-string-to-sign date region service creq))
	 (signing-key (derive-key (getopt req '%secret (getopt req 'secret secretawskey))
				  date region service))
	 (awskey (getopt req 'key awskey))
	 (signature (hmac-sha256 signing-key string)))
    (cons (frame-create #f
	    'key awskey 'signature signature
	    'region region 'service service 'date date
	    'credential (glom awskey "/" (get date 'isobasicdate) "/"
			  region "/" service "/aws4_request")
	    'date date 'host host
	    'signed-headers sh
	    ;; Debugging info
	    'creq (tryif debug creq)
	    'sigkey (tryif debug signing-key)
	    'string-to-sign (tryif debug string))
	  req)))

;;; Support functions

(define (derive-key secret date region service)
  (hmac-sha256
   (hmac-sha256 (hmac-sha256 (hmac-sha256 (glom "AWS4" secret) (get date 'isobasicdate))
			     region)
		service)
   "aws4_request"))

(define (get-string-to-sign date region service creq)
  (stringout "AWS4-HMAC-SHA256\n"
    (get date 'isobasic) "\n"
    (get date 'isobasicdate) "/" region "/" service "/aws4_request\n"
    (downcase (packet->base16 (sha256 creq)))))

;;; Canonical query

(define (canonical-query-string args (params))
  (default! params (try (getopt args '%params {})  (getopt args 'params {})
			(getkeys args)))
  (let* ((pairs (for-choices (key params)
		  (let ((v ))
		    (cons (uriencode key)
			  (for-choices (v (get args key))
			    (if (string? v) (uriencode v)
				(if (timestamp? v) (get v 'isobasic)
				    (uriencode (stringout v))))))))))
    (stringout
      (doseq (q (sorted pairs car) i)
	(printout (if (> i 0) "&")
	  (car q) "=" (cdr q))))))

;;;; Canonical Headers

(define (merge-spaces s)
  (textsubst s '(isspace+) " "))

(define trim-rule
  `#((subst (not> "\"") ,merge-spaces)
     (* #("\"" (not> "\"") "\""
	  (subst (not> "\"") ,merge-spaces)))))

(define (trim-header-value value)
  (trim-spaces (textsubst value trim-rule)))
  
(define (canonical-headers args (headers))
  (default! headers (try (getopt args '%headers {})
			 (getopt args 'headers {})
			 (getkeys args)))
  (let* ((pairs (for-choices (key headers)
		  (let ((v (get args key)))
		    (cons (trim-spaces (downcase key))
			  (if (string? v)
			      (trim-header-value v)
			      (trim-header-value
			       (if (timestamp? v) (get v 'isobasic)
				   (stringout v))))))))
	 (hkeys (sorted (car pairs))))
    (cons (stringout
	    (doseq (h hkeys i)
	      (let ((v (get pairs h)))
		(if (ambiguous? v)
		    (printout h ":"
		      (doseq (ev (sorted v) i)
			(printout (if (> i 0) ",") ev)))
		    (printout h ":" v "\n")))))
	  (stringout
	    (doseq (h hkeys i)
	      (printout (if (> i 0) ";") h))))))
