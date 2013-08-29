;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'aws/ses)

(module-export! '{ses/call})

(use-module '{aws fdweb texttools logger email})
(define %used_modules '{aws})

(define %loglevel %notify!)
;;(set!  %loglevel %debug%)

(define ses-endpoint "https://email.us-east-1.amazonaws.com/")

(define (ses/call args (opts #[]))
  (unless (and (test args 'from)
	       (forall email/ok? (get args 'from)))
    (error 'bademail "Bad FROM: " (reject (get args 'from) email/ok?)
	   " in " args))
  (unless (and (test args 'from)
	       (forall email/ok? (get args 'dest)))
    (error 'bademail "Bad DEST: " (reject (get args 'dest) email/ok?)
	   " in " args))
  (let* ((date (gmtimestamp 'seconds))
	 (datestring
	  (string-subst (string-subst (get date 'rfc822) "+0000" "GMT")
			" 1 Jan" " 01 Jan"))
	 (datestring (get date 'rfc822))
	 (secret (getopt opts 'aws:secret (config 'AWS:SECRET)))
	 (sig (hmac-sha256 secret datestring))
	 (authstring
	  (debug%watch
	      (stringout "AWS3-HTTPS AWSAccessKeyId="
		(getopt opts 'aws:key (config 'AWS:KEY)) ", "
		"Signature=" (packet->base64 sig) ", "
		"Algorithm=HmacSHA256")))
	 (query #[])
	 (handle (curlopen 'header (cons "Date" datestring)
			   'header (cons "X-Amzn-Authorization" authstring)
			   'header (cons "Expect" "")
			   'method 'POST
			   'verbose (getopt opts 'verbose #f))))
    (store! query "Action" (try (get args 'action) "SendEmail"))
    (store! query "Timestamp" (get date 'iso8601))
    (store! query "Source" (get args 'from))
    (when (exists? (get args 'returnpath))
      (store! query "ReturnPath" (get args 'returnpath)))
    (do-choices (dest (get args 'to) i)
      (store! query (stringout "Destination.ToAddresses.member." (1+ i))
	      dest))
    (do-choices (dest (get args 'cc) i)
      (store! query (stringout "Destination.CcAddresses.member." (1+ i))
	      dest))
    (do-choices (dest (get args 'bcc) i)
      (store! query (stringout "Destination.BccAddresses.member." (1+ i))
	      dest))
    (do-choices (dest (get args 'replyto) i)
      (store! query (stringout "ReplyToAddresses.member." (1+ i))
	      dest))
    (when (exists? (get args 'subject))
      (store! query "Message.Subject.Data" (get args 'subject))
      (store! query "Message.Subject.Charset" "UTF-8"))
    (when (exists? (get args 'text))
      (store! query "Message.Body.Text.Data" (get args 'text))
      (store! query "Message.Body.Text.Charset" "UTF-8"))
    (when (exists? (get args 'html))
      (store! query "Message.Body.Html.Data" (get args 'html))
      (store! query "Message.Body.Html.Charset" "UTF-8"))
    
    (debug%watch "SES/CALL" query)
    
    (urlput (getopt opts 'endpoint ses-endpoint)
	    (subseq (scripturl+ "" query) 1)
	    "application/x-www-form-urlencoded"
	    handle)))







