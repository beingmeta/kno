;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc.  All rights reserved.

(in-module 'aws/roles)

(use-module '{aws aws/v4 fdweb texttools mimetable regex logctl
	      ezrecords rulesets logger varconfig})
(define %used_modules '{aws varconfig ezrecords rulesets})

(define %loglevel %warn%)

(module-export! '{ec2/credentials ec2/role!})

(define aws/role #f)

(define-init credentials-cache (make-hashtable))

(define aws/role-root
  "http://169.254.169.254/latest/meta-data/iam/security-credentials/")
(varconfig! aws/roleroot aws/role-root)

(define (get-credentials role (error #f) (version "latest"))
  (if (not version) (set! version "latest"))
  (let* ((url (glom aws/role-root (downcase role)))
	 (response (urlget url))
	 (status (get response 'response))
	 (type (get response 'content-type)))
    (if (= status 200)
	(debug%watch (jsonparse (get response '%content)) response)
	(if error
	    (irritant response |BadEC2DataResponse| ec2/credentials)
	    (begin (logwarn |EC2 Credentials failed| url " status " status)
		  #f)))))

(define (ec2/credentials role (error #f) (cached))
  (set! cached (try (get credentials-cache role) #f))
  (if (and cached 
	   (or (not (test cached 'aws:expires))
	       (time<? (timestamp+ 180)
		       (timestamp (get cached 'aws:expires)))))
      (begin (logdebug |AWS/Credentials| 
	       "Using cached credentials for '" role ",' "
	       "expiring " (get cached 'aws:expires))
	cached)
      (let* ((fresh (get-credentials role error))
	     (result (tryif fresh
		       (frame-create #f
			 'aws:key (get fresh 'accesskeyid)
			 'aws:secret (->secret (get fresh 'secretaccesskey))
			 'aws:expires (timestamp (get fresh 'expiration))
			 'aws:token (get fresh 'token)))))
	(loginfo |AWS/NewCredentials| 
	  "Got new credentials for '" role "'\n "
	  (pprint result))
	(store! credentials-cache role result)
	result)))

(define (ec2/role! role (version "latest") (error #f))
  (if (not version) (set! version "latest"))
  (let* ((creds (ec2/credentials role error)))
    (when creds
      (set! aws/role role)
      (aws/creds! (get creds 'aws:key)
		  (->secret (get creds 'aws:secret))
		  (get creds 'aws:token)
		  (get creds 'aws:expires)
		  (lambda () (ec2/role! role))))
    (and creds aws/key)))

(config-def! 'aws:role
	     (lambda (var (value))
	       (if (bound? value)
		   (and (not (equal? value aws/role))
			(begin (when (ec2/role! value) (set! aws/role value))
			  #t))
		   aws/role)))

