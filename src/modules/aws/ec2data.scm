;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc.  All rights reserved.

(in-module 'aws/ec2data)

(use-module '{aws aws/v4 fdweb texttools mimetable regex logctl
	      ezrecords rulesets logger varconfig})
(define %used_modules '{aws varconfig ezrecords rulesets})

(module-export! '{ec2/data ec2/credentials ec2/role!})

(define ec2-instance-data-root "http://169.254.169.254/")

(define-init prop-ids #[])

(define ec2-data-endpoints
  {"/meta-data/" "/user-data"
   (glom "/meta-data/" {"ami-id" "ami-launch-index" "ami-manifest-path"
			"instance-id" "hsotname" "local-ipv4" "reservation-id"
			"security-groups"})})

(define (ec2/data (prop #f) (version "latest") (error #f))
  (if (not version) (set! version "latest"))
  (let* ((url (glom ec2-instance-data-root version "/"
		(if (string? prop) prop)
		(if (test prop-ids prop)
		    (get prop-ids prop)
		    (irritant prop |UnknownProperty| ec2/data))))
	 (response (urlget url))
	 (status (get response 'status))
	 (type (get response 'content-type)))
    (if (= status 200)
	(cond (else (get reponse 'content)))
	(irritant response |BadEC2DataResponse| ec2/data))))

(define (ec2/credentials role (version "latest") (error #f))
  (if (not version) (set! version "latest"))
  (let* ((url (glom ec2-instance-data-root version "/meta-data/iam/"
		(downcase role)))
	 (response (urlget url))
	 (status (get response 'status))
	 (type (get response 'content-type)))
    (if (= status 200)
	(cond (else (get reponse 'content)))
	(irritant response |BadEC2DataResponse| ec2/data))))

(define (ec2/role! role (version "latest") (error #f))
  (if (not version) (set! version "latest"))
  (let* ((url (glom ec2-instance-data-root version "/meta-data/iam/"
		(downcase role)))
	 (response (urlget url))
	 (status (get response 'status))
	 (type (get response 'content-type)))
    (if (= status 200)
	(let ((parsed (jsonparse (get reponse 'content))))
	  (when (test parsed 'accesskeyid)
	    (config! 'aws:key (get parsed 'accesskeyid))
	    (config! 'aws:secret (->secret (get parsed 'secretaccesskey)))))
	(irritant response |BadEC2DataResponse| ec2/data))))




