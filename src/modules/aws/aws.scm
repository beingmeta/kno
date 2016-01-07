;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Amazon Web Services
(in-module 'aws)

(use-module '{logger texttools})

(module-export! 
 '{aws/key aws/secret awsaccount
   aws/token aws/datesig aws/datesig/head})

;; Default (non-working) values from the online documentation
;;  Helpful for testing requests
(define aws/secret
  (or (getenv "AWS_SECRET_ACCESS_KEY")
      #"uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o"))
(define aws/key
  (or (getenv "AWS_ACCESS_KEY_ID")
      "0PN5J17HBGZHT7JJ3X82"))
(define awsaccount
  (or (getenv "AWS_ACCOUNT_NUMBER")
      "0PN5J17HBGZHT7JJ3X82"))

(config-def! 'aws:secret
	     (lambda (var (val))
	       (if (bound? val)
		   (set! aws/secret val)
		   aws/secret)))
(config-def! 'aws:key
	     (lambda (var (val))
	       (if (bound? val)
		   (set! aws/key val)
		   aws/key)))
(config-def! 'aws:account
	     (lambda (var (val))
	       (if (bound? val)
		   (set! awsaccount val)
		   awsaccount)))

(define aws/token #f)

(define (aws/datesig (date (timestamp)) (spec #{}))
  (unless date (set! date (timestamp)))
  (default! method (try (get spec 'method) "HmacSHA1"))
  ((if (test spec 'algorithm "HmacSHA256") hmac-sha256 hmac-sha1)
   (try (get spec 'secret) aws/secret)
   (get (timestamp date) 'rfc822)))

(define (aws/datesig/head (date (timestamp)) (spec #{}))
  (stringout "X-Amzn-Authorization: AWS3-HTTPS"
    " AWSAccessKeyId=" (try (get spec 'accesskey) aws/key)
    " Algorithm=" (try (get spec 'algorithm) "HmacSHA1")
    " Signature=" (packet->base64 (aws/datesig date spec))))

