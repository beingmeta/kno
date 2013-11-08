;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved.

(in-module 'twilio)

(use-module '{fdweb texttools varconfig logger})

(module-export! '{twilio/send smsout})

(define %loglevel %debug%)

(define default-sid
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ01234567")
(varconfig! twilio:sid default-sid)

(define default-from "+16175551212")
(varconfig! twilio:from default-from)

(define default-auth "abcdefghijklmnopqrstuvwxyz012345")
(varconfig! twilio:auth default-auth)

(define (twilio/send string opts)
  (logdebug "Sending " (write string) " to " opts)
  (when (string? opts) (set! opts `#[to ,opts]))
  (urlpost (glom "https://"
	     (getopt opts 'sid default-sid) ":"
	     (getopt opts 'auth default-auth)
	     "@api.twilio.com"
	     "/2010-04-01/Accounts/"
	     (getopt opts 'sid default-sid)
	     "/Messages")
	   "From" (getopt opts 'from default-from)
	   "To" (getopt opts 'to)
	   "Body" string))

(define smsout
  (macro expr
    `(,twilio/send (stringout ,@(cddr expr)) ,(cadr expr))))

