;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc. All rights reserved

(in-module 'urltest)

(use-module '{fdweb xhtml reflection texttools})
(use-module '{varconfig stringfmts logger})
(use-module '{twilio aws/ses})
(define %used_modules '{varconfig})

(module-export! 'urltest)

(define-init workdir (config 'workdir (tempdir)))
(varconfig! workdir workdir)

(define cookiejar #f)
(varconfig! urltest:cookiejar cookiejar)

(define email #f)
(varconfig! urltest:email email)
(define sms #f)
(varconfig! urltest:sms email)
(define email-from "admin@beingmeta.com")
(varconfig! urltest:from email-from)

(define maxtime 60)
(varconfig! testurl:maxtime maxtime)

(define (getopts (opts #f))
  (if opts
      (if (and (test opts 'cookiejar) (test opts 'maxtime))
	  opts
	  (let ((copy (deep-copy opts)))
	    (unless (test opts 'cookiejar)
	      (store! copy 'cookiejar
		      (or cookiejar (mkpath workdir "cookies"))))
	    (unless (test opts 'maxtime)
	      (store! copy 'maxtime maxtime))
	    copy))
      `#[cookiejar ,(or cookiejar (mkpath workdir "cookies"))
	 maxtime ,maxtime]))

(define (report-trouble testid url req testfn)
  (logwarn (stringout testid) " accessing " url ":\n\t" (pprint req))
  (when email
    (ses/call `#[to ,email from ,email-from
		 subject ,(stringout "Failed " testid " @ " url)
		 text ,(stringout (pprint req))]))
  (when sms
    (twilio/send (stringout "Failed " testid " @ " url) sms))
  #f)

(define (test-response req expect)
  (cond ((number? expect) (test req 'response expect))
	((applicable? expect) (expect (get req 'response)))
	((eq? expect 'ok) (>= 299 (get req 'response) 200))
	((eq? expect 'redirect) (>= 399 (get req 'response) 300))
	((eq? expect 'err) (>= 499 (get req 'response) 400))
	(else #t)))

(define (test-location req expect)
  (if (string? expect)
      (test req 'response expect)
      (if (applicable? expect)
	  (expect (get req 'location))
	  (textsearch expect (get req 'location)))))

(define (test-content req expect)
  (if (number? expect)
      (> (length (get req '%content)) expect)
      (if (string? expect)
	  (test req '%content expect)
	  (if (applicable? expect)
	      (expect (get req '%content))
	      (textsearch expect (get req '%content))))))

(define (urlget+ url opts (max-redirects 7))
  (let* ((handle (curlopen opts))
	 (req (urlget url handle))
	 (urls url)
	 (count 0))
    (while (and (< count max-redirects)
		(test req 'response)
		(< 300 (get req 'response) 399)
		(test req 'location))
      (let ((nexturl (get req 'location)))
	(unless (string-starts-with? nexturl #((isalpha+) ":") )
	  (set! nexturl (glom (urischeme url) "//" (urihost url) nexturl)))
	(set! req (urlget nexturl handle))
	(set! url nexturl)))
    req))

(define (urltest-inner testid url (opts #f) (testfn #f))
  (set! opts (getopts opts))
  (let ((req (onerror (if (and (table? testfn) (test testfn '{location response}))
			  (urlget url opts)
			  (urlget+ url opts))
	       (lambda (ex) (report-trouble testid url ex #f)))))
    (cond ((not req) #f)
	  ((and testfn (applicable? testfn))
	   (or (testfn req) (report-trouble testid url req testfn) #t))
	  ((and testfn (slotmap? testfn))
	   (and
	    (or (not (test testfn 'response))
		(test-response req (get testfn 'response))
		(report-trouble testid url req
				`#[test response
				   expect ,(get testfn 'response)
				   got ,(get req 'response)]))
	    (or (not (test testfn 'location))
		(test-location req (get testfn 'location))
		(report-trouble testid url req
				`#[test location
				   expect ,(get testfn 'location)
				   got ,(get req 'location)]))
	    (or (not (test testfn 'content))
		(test-content req (get testfn 'content))
		(report-trouble testid url req
				`#[test content
				   expect ,(get testfn 'content)
				   got ,(get req '%content)]))))
	  ((number? testfn)
	   (or (identical? testfn (get req 'response))
	       (report-trouble testid url req testfn)))
	  ((and (test req 'response) (>= 399 (get req 'response) 200)) #t)
	  (else (report-trouble testid url req testfn)))))

(define (urltest testid url (opts (getopts)) (testfn #f))
  (let ((result (urltest-inner testid url opts testfn)))
    (if result
	(lognotice "Successful test " testid " @ " url)
	(logwarn "Failed test " testid " @ " url))
    result))



