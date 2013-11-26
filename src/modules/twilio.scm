;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved.

(in-module 'twilio)

(use-module '{fdweb texttools varconfig logger})

(module-export! '{twilio/send smsout sms/norm sms/norm})

(define-init %loglevel %notify%)

(define-init default-sid
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ01234567")
(varconfig! twilio:sid default-sid)

(define-init default-from "+16175551212")
(varconfig! twilio:from default-from)

(define-init default-auth "abcdefghijklmnopqrstuvwxyz012345")
(varconfig! twilio:auth default-auth)

(define (twilio/send string opts)
  (loginfo "Sending " (write string) " to " opts)
  (when (string? opts) (set! opts `#[to ,opts]))
  (detail%watch
   (urlpost (glom "https://api.twilio.com/2010-04-01/Accounts/"
	      (getopt opts 'sid default-sid)
	      "/Messages")
	    `#[header ,(glom "Authorization: Basic "
			 (->base64 (glom (getopt opts 'sid default-sid) ":"
				     (getopt opts 'auth default-auth))))]
	    "From" (getopt opts 'from default-from)
	    "To" (getopt opts 'to)
	    "Body" string)
   opts string))

(define smsout
  (macro expr
    `(,twilio/send (stringout ,@(cddr expr)) ,(cadr expr))))

;;; Phone number functions

(define number-pat
  `(GREEDY
    (PREF
     #("+" (label cc (isdigit+)) (spaces*)
       "(" (label areacode #((isdigit) (isdigit) (isdigit))) ")"
       (spaces*)
       (label number
	      #((isdigit) (isdigit) (isdigit) (opt "-")
		(isdigit) (isdigit) (isdigit) (isdigit))))
     #("+" (label cc (isdigit+)) (spaces*)
       (label areacode (isdigit+)) 
       {(spaces*) "/" "-" "."}
       (label number
	      #((isdigit+) (* #({"." (spaces) "/" "-"} (isdigit+))))))
     #((opt "+") (opt (label cc "1"))
       (label areacode
	      #({"2" "3" "4" "5" "6" "7" "8" "9"} (isdigit) (isdigit)))
       {"-" "/" "." ""} (spaces*)
       (label number
	      #((isdigit) (isdigit) (isdigit)
		{(spaces) "-" "/" "." ""}
		(isdigit) (isdigit) (isdigit) (isdigit))))
     #("(" (label areacode #((isdigit) (isdigit) (isdigit))) ")"
       (spaces*)
       (label number
	      #((isdigit) (isdigit) (isdigit)
		{(spaces) "-" "/" "." ""}
		(isdigit) (isdigit) (isdigit) (isdigit)))))))

(define (sms/display string)
  (and (string? string)
       (let* ((match (text->frame number-pat string))
	      (cc (try (get match 'cc) 1))
	      (areacode (get match 'areacode))
	      (number (get match 'number)))
	 (and (exists? match)
	      (if (exists? areacode)
		  (stringout "+" cc "(" areacode ")" number)
		  (stringout "+" cc " "
		    (if (exists? areacode) (printout areacode " "))
		    (textsubst number #{(spaces) "." "/" "-"} "")))))))
(define (sms/norm string)
  (and (string? string)
       (let* ((match (text->frame number-pat string))
	      (cc (get match 'cc))
	      (areacode (get match 'areacode))
	      (number (get match 'number)))
	 (when (and (or (fail? cc) (equal? cc "1") (eq? cc 1))
		    (equal? (length number) 7))
	   (set! number
		 (glom (slice number 0 3) "-" (slice number 3))))
	 (and (exists? match)
	      (if (or (fail? cc) (equal? cc "1") (eq? cc 1))
		  (glom "+1(" areacode ")" number)
		  string)))))

