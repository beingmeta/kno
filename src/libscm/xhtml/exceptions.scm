;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved
Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'xhtml/exceptions)

(use-module '{webtools xhtml})

(module-export! '{exception->html})

(define (exception->html exception)
  (htmlheader
   (xmlblock STYLE ((type "text/css"))
     "\n"
     "body {font-family: arial,sans; margin: 0px;}\n"
     "div.exception_header {padding-left: 3px; padding-right: 3px; margin-bottom: 1ex; margin-right: 0px; padding-top: 0px; background-color: silver; border-bottom: solid black 2px;}\n"
     "div.exception_header span.floatright { float: right; font-size: 80%; clear: right;}\n"
     "div.exception_header h1 { margin-top: 0px; margin-bottom: 0px; }\n"
     "div.exception_header p { margin-left: 2em;  }\n"
     "div#BODY {margin-left: 5vw;}\n"
     "div#BODY div {margin-right: 2em;}\n"
     "h2 { text-align: left; margin-left: -3vw; margin-right: 2em; border-top: double black 2px; font-variant: small-caps; margin-bottom: 1px;}\n"
     "h2:first-child { border-top: none; }\n"
     "pre { margin-top: 1px; }\n"
     "div#BODY div.stack { margin-left: -2vw; }\n"
     "div.stack { border-top: solid black 1px; margin-bottom: 1em; }\n"
     "div.stack:first-child { border-top: none; }\n"
     "div.stack pre { display: inline-block; }\n"
     "div.stack span.info { font-size: 70%; float: right; margin-left: 2em; }\n"
     "div.stack span.info span.type { font-weight: bold; ;}\n"
     "div.stack span.label { font-size: 150%; font-weight: bold; padding-right: 1em; ;}\n"
     "div.stack span.op { white-space: nowrap; font-size: 80%; }\n"
     "div.stack div.bindings { margin-left: 2vw; }\n"
     "div.stack div.bindings .binding { white-space: nowrap; padding-right: 1ex; }\n"
     "div.stack div.bindings .binding .name { font-weight: bold; }\n"
     "div.stack div.bindings .binding .value { display: inline-block; }\n"
     "div.stack div.bindings .binding .status { font-style: italic; }\n"
     "p,li,ul,ol {text-align: left; margin-right: 2em; margin-top: 1px; margin-bottom: 1px;}\n"))
  (title! (exception-condition exception) " "
	  (get (timestamp+ (exception-timebase exception) (exception-moment exception))
	       'iso) " "
	       (when (exception-caller exception)
		 (printout "- " (exception-caller exception))))
  (div ((class "exception_header"))
    (when (exception-caller exception)
      (span ((class "floatright caller")) (->string (exception-caller exception))))
    (span ((class "floatright sessionid")) (exception-sessionid exception))
    (span ((class "floatright timestamp"))
      (get (timestamp+ (exception-timebase exception) (exception-moment exception))
	   'iso))
    (h1 (tt (exception-condition exception)))
    (when (exception-details exception)
      (p* ((class "details")) (xmlout (exception-details exception)))))
  (div ((id "BODY"))
    (when (exception-irritant? exception)
      (h2 "Irritant")
      (xmlblock "PRE" ((class "irritant"))
	(stringout (void (pprint (exception-irritant exception))))))
    (do-choices (context (pick (exception-context exception) {slotmap? schemap?}))
      (do-choices (key (getkeys context))
	(h2 (if (string? key) key (capitalize (downcase key))))
	(xmlblock "PRE" ((class key))
	  (stringout (void (pprint (get context key)))))))
    (do-choices (context (reject (exception-context exception) {slotmap? schemap?}))
      (h2 "Context")
      (xmlblock "PRE" ((class "context"))
	(stringout (void (pprint context)))))
    (h2 "Backtrace")
    (div ((class "backtrace"))
      (doseq (stack (exception-stack exception))
	(div ((class "stack"))
	  (span ((class "info"))
	    (span ((class "depth")) "@" (stack-depth stack)))
	  (if (and (stack-origin stack) (stack-label stack)
		   (not (equal? (stack-origin stack) (stack-label stack))))
	      (span ((class "label")) (stack-origin stack) "/" (stack-label stack))
	      (span ((class "label")) (stack-label stack)))
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
	      (xmlblock ul ((class "args")) 
		(doseq (arg args i) (li (output-vals arg)))))
	    (when env
	      (div ((class "bindings"))
		(do-choices (key (getkeys env) k)
		  (when (> k 0) (xmlout " "))
		  (span ((class "binding"))
		    (span ((class "name")) key) "\&nbsp;=\&nbsp;"
		    (if (void? (get env key))
			(span ((class "status")) "unbound")
			(output-vals (get env key))))))))
	  ;;(if (stack-env stack))
	  )))))

(defambda (output-vals vals)
  (cond ((fail? vals) 
	 (span ((class "status")) "empty choice"))
	((ambiguous? vals)
	 (span ((class "status")) (choice-size vals) "\&nbsp;values" " ")))
  (do-choices (val vals i)
    (when (> i 0) (xmlout " "))
    (cond ((symbol? val) (span ((class "value symbol")) val))
	  ((string? val) (span ((class "value string")) val))
	  ((number? val) (span ((class "value number")) val))
	  ((oid? val) (span ((class "value oid")) val))
	  (else (xmlblock PRE ((class "value"))
		  (stringout (void (pprint val))))))))
