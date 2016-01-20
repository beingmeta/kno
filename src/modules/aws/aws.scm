;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc.  All rights reserved.

;;; Core file for accessing Amazon Web Services
(in-module 'aws)

(use-module '{logger texttools})

(module-export! 
 '{aws/role aws/creds
   aws/datesig aws/datesig/head
   ec2-instance-data-root})

(define ec2-instance-data-root "http://169.254.169.254/")

(define-init default-creds #[])

(define aws/role (getenv "AWS_ROLE"))
(varconfig! aws:role aws/role)

(config-def! 'aws:secret
	     (lambda (var (val))
	       (if (bound? val)
		   (store! default-creds 'aws:secret val)
		   (test default-creds 'aws:secret))))
(config-def! 'aws:key
	     (lambda (var (val))
	       (if (bound? val)
		   (store! defalt-creds 'aws:key val)
		   (try (get default-creds 'aws:key) #f))))
(config-def! 'aws:account
	     (lambda (var (val))
	       (if (bound? val)
		   (store! default-creds aws:account val)
		   (try (get default-creds 'aws:account) #f))))

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

(define (aws/creds opts)
  (unless (or (getopt opts 'aws:secret) (test default-creds 'aws:secret))
    (error |NoDefaultAWSCredentials|))
  (unless (getopt opts 'aws:secret)
    (if opts 
	(set! opts (cons default-creds opts))
	(set! opts default-creds)))
  (if (getopt opts 'aws:key)
      (if (or (not (testopt opts 'aws:expires))
	      (time<? (timestamp+ 60) (getopt opts 'aws:expires)))
	  opts
	  (if (testopt opts 'aws:refresh)
	      ((getopt opts 'aws:refresh) opts)
	      (if (time<? (timestamp+ 1) (getopt opts 'aws:expires)) opts
		  (error |AWSCredentialsExpired| opts))))
      (error |NoAWSKeyValue| opts)))



