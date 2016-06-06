;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'aws/ses)

(module-export! '{ses/call ses/rawcall ses/sendmail ses/send})

(use-module '{aws fdweb texttools mimeout logger email varconfig})
(define %used_modules '{aws})

(define-init %loglevel %notice%)

(define ses:key #f)
(define ses:secret #f)
(varconfig! ses:key ses:key)
(varconfig! ses:secret ses:secret)

(define default-from #f)
(varconfig! ses:from default-from)

(define ses-endpoint "https://email.us-east-1.amazonaws.com/")

(define (ses/call args (opts #[]) (from))
  (default! from (try (get args 'from) (getopt opts 'from {})
		      default-from))
  (if from
      (unless (forall email/ok? from)
	(error 'bademail "Bad FROM origin: " (reject from email/ok?)
	       " in " args))
      (error 'bademail "No email origin (FROM) specified in: " args))
  (unless (and (test args 'to) (forall email/ok? (get args 'to)))
    (error 'bademail "Bad TO: " (reject (get args 'to) email/ok?)
	   " in " args))
  (let* ((date (gmtimestamp 'seconds))
	 (datestring
	  (string-subst (string-subst (get date 'rfc822) "+0000" "GMT")
			" 1 Jan" " 01 Jan"))
	 (datestring (get date 'rfc822))
	 (secret (getopt opts 'aws:secret (or ses:secret aws:secret)))
	 (sig (hmac-sha256 secret datestring))
	 (authstring
	  (debug%watch
	      (stringout "AWS3-HTTPS AWSAccessKeyId="
		(getopt opts 'aws:key (or ses:key aws:key)) ", "
		"Signature=" (packet->base64 sig) ", "
		"Algorithm=HmacSHA256")))
	 (query #[])
	 (handle (curlopen 'header (cons "Date" datestring)
			   'header (cons "X-Amzn-Authorization" authstring)
			   'header (cons "Expect" "")
			   'header (getopt opts 'aws:token aws:token)
			   'method 'POST
			   'verbose (getopt opts 'verbose #f))))
    (store! query "Action" (try (get args 'action) "SendEmail"))
    (store! query "Timestamp" (get date 'iso8601))
    (store! query "Source" from)
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
    
    (let ((response
	   (urlput (getopt opts 'endpoint ses-endpoint)
		   (subseq (scripturl+ "" query) 1)
		   "application/x-www-form-urlencoded"
		   handle)))
      (debug%watch "SES/RESPONSE" response)
      response)))

(define (ses/sendmail to text)
  (if (string? to)
      (ses/call `#[TO ,to
		   SUBJECT ,(stringout "Message from " (config 'sessionid))
		   TEXT ,text])
      (ses/call (frame-create to
		  'text text
		  'subject
		  (tryif (not (test to 'subject))
		    (stringout "Message from " (config 'sessionid)))))))

(define ses/send
  (macro expr
    `(,ses/sendmail ,(cadr expr) (,stringout ,@(cddr expr)))))

(define (ses/rawcall args (opts #[]) (from))
  (default! from (try (get args 'from) (getopt opts 'from {})
		      default-from))
  (if from
      (unless (forall email/ok? from)
	(error 'bademail "Bad FROM origin: " (reject from email/ok?)
	       " in " args))
      (error 'bademail "No email origin (FROM) specified in: " args))
  (unless (and (test args 'to) (forall email/ok? (get args 'to)))
    (error 'bademail "Bad TO: " (reject (get args 'to) email/ok?)
	   " in " args))
  (let* ((date (gmtimestamp 'seconds))
	 (datestring
	  (string-subst (string-subst (get date 'rfc822) "+0000" "GMT")
			" 1 Jan" " 01 Jan"))
	 (datestring (get date 'rfc822))
	 (secret (getopt opts 'aws:secret (or ses:secret aws:secret)))
	 (sig (hmac-sha256 secret datestring))
	 (authstring
	  (debug%watch
	      (stringout "AWS3-HTTPS AWSAccessKeyId="
		(getopt opts 'aws:key (or ses:key aws:key)) ", "
		"Signature=" (packet->base64 sig) ", "
		"Algorithm=HmacSHA256")))
	 (query #[])
	 (handle (curlopen 'header (cons "Date" datestring)
			   'header (cons "X-Amzn-Authorization" authstring)
			   'header (cons "Expect" "")
			   'method 'POST
			   'verbose (getopt opts 'verbose #f)))
	 (message
	  (stringout (mimeout (get-mime-head args from)
			      (try (get args 'content)
				   (get args 'text)
				   "")))))
    (store! query "Action" (try (get args 'action) "SendRawEmail"))
    (store! query "Timestamp" (get date 'iso8601))
    (store! query "Source" from)
    (when (exists? (get args 'returnpath))
      (store! query "ReturnPath" (get args 'returnpath)))

    (do-choices (dest (get args 'to) i)
      (store! query (stringout "Destinations.member." (1+ i))
	      dest))

    (store! query "RawMessage.Data" (->base64 message))
    
    (debug%watch "SES/RAWCALL" query)
    
    (let ((response
	   (urlput (getopt opts 'endpoint ses-endpoint)
		   (subseq (scripturl+ "" query) 1)
		   "application/x-www-form-urlencoded"
		   handle)))
      (debug%watch "SES/RESPONSE" response)
      ;; (store! response 'messagedata message)
      response)))

(define (get-mime-head args from)
  (frame-create #f
    'from from
    'to (stringout (do-choices (to (get args 'to) i)
		     (printout (if (> i 0) ", ") to)))
    'cc (tryif (test args 'cc)
	  (stringout (do-choices (to (get args 'cc) i)
		       (printout (if (> i 0) ", "))
		       to)))
    '%ordered #(TO FROM CC)
    'replyto
    (tryif (test args 'bcc)
	  (stringout (do-choices (to (get args 'bcc) i)
		       (printout (if (> i 0) ", "))
		       to)))
    'subject (try (get args 'subject) "")))




