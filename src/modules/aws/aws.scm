;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Amazon Web Services
(in-module 'aws)

(use-module '{logger texttools fdweb})

(define-init %loglevel %notice%)

(define %nosubst '{aws/account
		   aws:key aws:secret
		   aws/refresh aws:token})

(module-export! 
 '{aws/account aws:key aws:secret aws:token aws/expires 
   aws/ok? aws/checkok aws/set-creds! aws/creds!
   aws/datesig aws/datesig/head})

;; Default (non-working) values from the online documentation
;;  Helpful for testing requests
(define aws:secret
  (getenv "AWS_SECRET_ACCESS_KEY"))
(define aws:key (getenv "AWS_ACCESS_KEY_ID"))
(define aws/account (getenv "AWS_ACCOUNT_NUMBER"))

(config-def! 'aws:secret
	     (lambda (var (val))
	       (if (bound? val)
		   (set! aws:secret val)
		   aws:secret)))
(config-def! 'aws:key
	     (lambda (var (val))
	       (if (bound? val)
		   (set! aws:key val)
		   aws:key)))
(config-def! 'aws:account
	     (lambda (var (val))
	       (if (bound? val)
		   (set! aws/account val)
		   aws/account)))

(define aws:token #f)
(define aws/expires #f)
(define aws/refresh #f)

(define (aws/datesig (date (timestamp)) (spec #{}))
  (unless date (set! date (timestamp)))
  (default! method (try (get spec 'method) "HmacSHA1"))
  ((if (test spec 'algorithm "HmacSHA256") hmac-sha256 hmac-sha1)
   (try (get spec 'secret) aws:secret)
   (get (timestamp date) 'rfc822)))

(define (aws/datesig/head (date (timestamp)) (spec #{}))
  (stringout "X-Amzn-Authorization: AWS3-HTTPS"
    " AWSAccessKeyId=" (try (get spec 'accesskey) aws:key)
    " Algorithm=" (try (get spec 'algorithm) "HmacSHA1")
    " Signature=" (packet->base64 (aws/datesig date spec))))

(define (aws/ok? (opts #f) (err #f))
  (if (or (not opts) (not (getopt opts 'aws:secret)))
      (if (not aws:secret)
	  (and err (error |NoAWSCredentials| opts))
	  (or (not aws/expires) (> (difftime aws/expires) 3600)
	      (and aws/refresh (aws/refresh #f))
	      (and err (error |ExpiredAWSCredentials| aws:key))))
      (or (not (getopt opts 'aws:expires))
	  (time<? (getopt opts 'aws:expires))
	  (and aws/refresh (aws/refresh opts))
	  (and err (error |ExpiredAWSCredentials| aws:key)))))

(define (aws/checkok (opts #f)) (aws/ok? opts #t))

(define (refresh-creds opts)
  (if opts (aws/refresh opts)
      (let ((refreshed (frame-create #f
			 'aws:account aws/account
			 'aws:key aws:key 'aws:secret aws:secret
			 'aws:token aws:token)))
	(when refreshed
	  (set! aws:key (get refreshed 'aws:key))
	  (set! aws:secret (get refreshed 'aws:secret))
	  (set! aws:token (get refreshed 'aws:token))
	  (set! aws/expires (get refreshed 'aws:expires)))
	refreshed)))

(define (aws/set-creds! key secret (token #f) (expires #f) (refresh #f))
  (info%watch "AWS/SET-CREDS!" key secret token expires refresh)
  (set! aws:key key)
  (set! aws:secret secret)
  (set! aws:token token)
  (set! aws/expires expires)
  (set! aws/refresh refresh))

(define (aws/creds! arg)
  (if (not arg)
      (begin (aws/set-creds! #f #f #f #f) #f)
      (let* ((spec (if (string? arg)
		       (if (has-prefix arg {"https:" "http:"})
			   (urlcontent arg)
			   (if (has-prefix arg { "/" "~/" "./"})
			       (onerror (filestring arg)
				 (lambda (x) (filedata arg)))
			       arg))
		       arg))
	     (creds (if (string? spec) (jsonparse spec)
			(if (packet? spec) (packet->dtype spec)
			    spec))))
	(aws/set-creds! (try (get creds 'aws:key) (get creds 'accesskeyid))
			(try (->secret (get creds 'aws:secret))
			     (->secret (get creds 'secretaccesskey)))
			(try (get creds 'aws:token)
			     (get creds 'token)
			     #f)
			(try (get creds 'aws:expires)
			     (get creds 'expiration)
			     #f)
			#f)
	creds)))




