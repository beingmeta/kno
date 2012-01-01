;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'aws/ses)

(module-export! '{ses/call})

(use-module '{aws fdweb texttools logger rulesets})

(define ses-endpoint "https://email.us-east-1.amazonaws.com/")

(define (ses/call args (opts #[]))
  (let* ((date (gmtimestamp 'seconds))
	 (secret (getopt opts 'aws:secret (config 'AWS:SECRET)))
	 (sig (hmac-sha256 secret (get date 'rfc822)))
	 (authstring
	  (stringout "AWS3-HTTPS AWSAccessKeyId="
	    (getopt opts 'aws:key (config 'AWS:KEY)) ", "
	    "Signature=" (uriencode (packet->base64 sig)) "=, "
	    "Algorithm=HmacSHA256"))
	 (handle (curlopen 'header (cons "Date" (get date 'rfc822))
			   'header (cons "X-Amzn-Authorization" authstring)
			   'verbose #t)))
    (urlpost (getopt opts 'endpoint ses-endpoint) handle args)))



