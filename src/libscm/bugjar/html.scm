;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved
Copyright (C) 2020-2021 beingmeta, LLC

(in-module 'bugjar/html)

(use-module '{webtools xhtml bugjar kno/condense io/getcontent logger})

(define-init %loglevel %notice%)

(module-export! '{exception/preamble
		  exception/header 
		  exception/body
		  exception->html
		  exception/page
		  exception.html})

(define inline-css-file (get-component "resources/inline.css"))
(define inline-js-file (get-component "resources/inline.js"))
(define inline-css (filestring inline-css-file))
(define inline-js (filestring inline-js-file))

(define (exception/preamble exception)
  (htmlheader
   (xmlblock STYLE ((type "text/css"))
     (getcontent inline-css-file)))
  (htmlheader
   (xmlblock SCRIPT ((language "javascript"))
     (getcontent inline-js-file)))
  (title! (exception-condition exception) " "
	  (get (timestamp+ (exception-timebase exception) (exception-moment exception))
	       'iso) " "
	  (when (exception-caller exception)
	    (printout "- " (exception-caller exception))))
  (bodyclass! "errorpage"))

(define (exception/header exception)
  (div ((class "exception_header"))
    (when (exception-caller exception)
      (span ((class "floatright caller")) (->string (exception-caller exception))))
    (span ((class "floatright sessionid")) (exception-sessionid exception))
    (span ((class "floatright timestamp"))
      (get (timestamp+ (exception-timebase exception) (exception-moment exception))
	   'iso))
    (h1 (tt (exception-condition exception)))
    (when (exception-details exception)
      (p* ((class "details")) 
	(xmlout (exception-details exception))))
    (div ((style "clear: both;"))))
  (when (req/get 'messages)
    (div ((class "exception_messages"))
      (do-choices (msg (req/get 'messages {}))
	(cond ((and (string? msg) (position #\< msg)) (xhtml msg))
	      ((string? msg) (p* ((class "message")) msg))
	      ((applicable? msg) (msg))
	      (else (p* ((class "message")) msg)))))))

(define (exception/body exception)
  (div ((class "exception_body") (onclick "toggleExpand(event);"))
    (when (exception-irritant? exception)
      (h2 "Irritant")
      (div ((class "irritant aspect expands"))
	(xmlblock "PRE" ()
	  (stringout (void (pprint (exception-irritant exception)))))))
    (h2 "Backtrace")
    (div ((class "exception_backtrace aspect expands"))
      (doseq (stack (exception-stack exception))
	(div ((class "stack"))
	  (void (stack-source stack))
	  (span ((class "info"))
	    (span ((class "depth")) "@" (stack-depth stack)))
	  (when (string? (stack-filename stack))
	    (span ((class "filename")) "\&ldquo;" (stack-filename stack) "\&rdquo;"))
	  (when (stack-crumb stack)
	    (span ((class "crumb")) (stack-crumb stack)))
	  (when (or (stack-origin stack) (stack-label stack))
	    (span ((class "label"))
	      (if (and (stack-origin stack) (stack-label stack))
		  (printout (stack-origin stack) ":" (stack-label stack))
		  (printout (or (stack-origin stack) (stack-label stack)))) ))
	  (if (pair? (stack-op stack))
	      (xmlblock PRE ((class "expr"))
		(stringout (void (pprint (stack-op stack)))))
	      (span ((class "op")) (stack-op stack)))
	  (let ((args (stack-args stack))
		(env (and (stack-env stack)
			  (if (table? (stack-env stack))
			      (stack-env stack)
			      (if (compound? (stack-env stack) '%BOUND)
				  (compound-ref (stack-env stack) 0)
				  #f)))))
	    (when  (and args (sequence? args)  (> (length args) 0))
	      (xmlblock ol ((class "args")) 
		(doseq (arg args i) 
		  (if (bound? arg)
		      (li (output-vals arg))
		      (li "unbound")))))
	    (when env
	      (div ((class "bindings expands"))
		(do-choices (key (getkeys env) k)

		  (when (> k 0) (xmlout " "))
		  (if (void? (get env key))
		      (span ((class "binding"))
			(span ((class "name")) key)
			"\&nbsp;=\&nbsp;"
			(span ((class "status")) "unbound"))
		      (let* ((v (get env key))
			     (big (or (> (choice-size v) 7)
				      (and (singleton? v) (sequence? v)
					   (> (length v) (if (string? v) 42 7)))
				      (and (singleton? v) (string? v)
					   (multiline-string? v)))))
			(span ((class (if big "binding big" "binding")))
			  (span ((class "name")) key) "\&nbsp;=\&nbsp;"
			  (span ((class "values expands"))
			    (output-vals v))))))))))))
    (do-choices (context (pick (exception-context exception) {slotmap? schemap?}))
      (do-choices (key (getkeys context))
	(unless (void? key)
	  (h2 (if (string? key) key (capitalize (downcase key))))
	  (let ((value (get context key)))
	    (if (string? value)
		(div ((class "string aspect expands")) value)
		(div ((class (glom (downcase key) " aspect expands")))
		  (xmlblock "PRE" () (stringout (listdata value #t)))))))))
    (do-choices (context (reject (exception-context exception) {slotmap? schemap?}))
      (h2 "Context")
      (div ((class "context aspect expands"))
	(xmlblock "PRE" () (stringout (listdata context #t)))))))

(define (exception->html exception)
  (exception/preamble exception)
  (exception/header exception)
  (exception/body exception))

(define (exception/page exception)
  (req/set! 'content-type "text/html; charset=utf8")
  (req/set! 'doctype "<!doctype html>")
  (bodyclass! "errorpage")
  (exception->html exception))

(define (exception.html exception)
  (lineout "<!doctype html>\n<html>\n<head>")
  (xmlblock STYLE ((type "text/css"))
    (printout (getcontent inline-css-file)))
  (xmlblock SCRIPT ((language "javascript"))
    (printout (getcontent inline-js-file)))
  (xmlblock TITLE ()
    (exception-condition exception) " "
    (get (timestamp+ (exception-timebase exception) (exception-moment exception))
	 'iso) " "
	 (when (exception-caller exception)
	   (printout "- " (exception-caller exception))))
  (lineout "\n</head>\n<body class='errorpage'>")
  (exception/header exception)
  (exception/body exception))

(defambda (output-vals vals)
  (cond ((fail? vals) 
	 (span ((class "status")) "empty choice"))
	((ambiguous? vals)
	 (span ((class "status")) (choice-size vals) "\&nbsp;values" " ")))
  (do-choices (val vals i)
    (when (> i 0) (xmlout " "))
    (cond ((symbol? val) (span ((class "value symbol")) val))
	  ((string? val)
	   (if (multiline-string? val)
	       (if (position #\< val)
		   (xmlblock PRE ((class "value string")) val)
		   (div ((class "value string")) val))
	       (span ((class "value string")) val)))
	  ((number? val) (span ((class "value number")) val))
	  ((oid? val) (span ((class "value oid")) val))
	  (else (xmlblock PRE ((class "value"))
		  (stringout (listdata val #t)))))))
