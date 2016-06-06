;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'webapi/facebook)

;;; This implements simple access to the Facebook API
;;;  Basically replaced by the directory-based facebook module

(use-module '{ezrecords fdweb texttools})

;; Not currently used
(defrecord facepp appid apikey secretkey)

(define apikey "1ab7fa75d12550c67cf11f3c6be17cb1")
(define secretkey "1932a9c8a3c12044b8bbd11397da16ed")
(define appid "24262579118")

(config-def! 'fb/appid
	     (lambda (var (val))
	       (if (bound? val)
		   (set! appid val)
		   appid)))
(config-def! 'fb/apikey
	     (lambda (var (val))
	       (if (bound? val)
		   (set! apikey val)
		   apikey)))
(config-def! 'fb/apisecret
	     (lambda (var (val))
	       (if (bound? val)
		   (set! secretkey val)
		   secretkey)))

(define facebook-uri "https://api.facebook.com/restserver.php")

(define (pair-args args)
  (let ((results {}))
    (do ((scan args (cddr scan)))
	((null? scan) results)
      (set+! results (cons (car scan) (cadr scan))))))

(define (fb/calluri uri . args)
  (let* ((plist (sorted (pair-args args) car))
	 (sigstring
	  (stringout (doseq (p plist) (printout (car p) "=" (cdr p)))
	    secretkey))
	 (signature (md5 sigstring))
	 (urlpostargs (list "sig"
			    (downcase (packet->base16 signature)))))
    (doseq (p plist)
      (set! urlpostargs (cons* (car p) (cdr p) urlpostargs)))
    (apply urlpost uri (->list urlpostargs))))

(define (fb/call . args)
  (let* ((plist (sorted (pair-args args) car))
	 (sigstring
	  (stringout (doseq (p plist) (printout (car p) "=" (cdr p)))
	    secretkey))
	 (signature (md5 sigstring))
	 (urlpostargs (list "sig"
			    (downcase (packet->base16 signature)))))
    (doseq (p plist)
      (set! urlpostargs (cons* (car p) (cdr p) urlpostargs)))
    (apply urlpost facebook-uri (->list urlpostargs))))

(comment
 (apicall "http://api.facebook.com/restserver.php"
	  "v" "1.0" "api_key" apikey
	  "method" "facebook.auth.getSession"
	  "auth_token" authtoken))

