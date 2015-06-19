;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Amazon Web Services
(in-module 'aws)

(use-module 'crypto)

(module-export! 
 '{awskey secretawskey awsaccount
   aws/datesig aws/datesig/head})

;; Default (non-working) values from the online documentation
;;  Helpful for testing requests
(define secretawskey
  (or (getenv "AWS_SECRET_ACCESS_KEY")
      #"uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o"))
(define awskey
  (or (getenv "AWS_ACCESS_KEY_ID")
      "0PN5J17HBGZHT7JJ3X82"))
(define awsaccount
  (or (getenv "AWS_ACCOUNT_NUMBER")
      "0PN5J17HBGZHT7JJ3X82"))

(config-def! 'aws:secret
	     (lambda (var (val))
	       (if (bound? val)
		   (set! secretawskey val)
		   secretawskey)))
(config-def! 'aws:key
	     (lambda (var (val))
	       (if (bound? val)
		   (set! awskey val)
		   awskey)))
(config-def! 'aws:account
	     (lambda (var (val))
	       (if (bound? val)
		   (set! awsaccount val)
		   awsaccount)))

(define (getit) hmac-sha256)

(define (aws/datesig (date (timestamp)) (spec #{}))
  (unless date (set! date (timestamp)))
  (default! method (try (get spec 'method) "HmacSHA1"))
  ((if (test spec 'algorithm "HmacSHA256") hmac-sha256 hmac-sha1)
   (try (get spec 'secret) secretawskey)
   (get (timestamp date) 'rfc822)))

(define (aws/datesig/head (date (timestamp)) (spec #{}))
  (stringout "X-Amzn-Authorization: AWS3-HTTPS"
    " AWSAccessKeyId=" (try (get spec 'accesskey) awskey)
    " Algorithm=" (try (get spec 'algorithm) "HmacSHA1")
    " Signature=" (packet->base64 (aws/datesig date spec))))

