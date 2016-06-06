;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc. All rights reserved

(in-module 'checkurl)

(use-module '{fdweb xhtml reflection texttools})
(use-module '{varconfig stringfmts logger})
(use-module '{twilio aws/ses})
(define %used_modules '{varconfig})

(define-init %loglevel %warn%)

(module-export! 'checkurl)

(define-init workdir #f)
(varconfig! workdir workdir)

(define-init statefile "/tmp/checkurl.state")
(define-init troubles (make-hashtable))
(define (config-state var (val))
  (cond ((not (bound? val)) troubles)
	((and (string? statefile) (file-exists? statefile))
	 (set! statefile statefile)
	 (set! troubles (file->dtype statefile)))
	((string? statefile)
	 (set! statefile statefile)
	 (set! troubles (make-hashtable))
	 (dtype->file troubles statefile))
	((and (pair? val) (table? (car val)) (string? (cdr val)))
	 (set! statefile (cdr statefile))
	 (set! troubles (car val))
	 (dtype->file troubles statefile))
	((and (pair? val) (string? (car val)) (table? (cdr val)))
	 (set! statefile (car statefile))
	 (set! troubles (cdr val))
	 (dtype->file troubles statefile))
	((table? val)
	 (set! statefile #f)
	 (set! troubles val))
	(else (error "Bad value for CHECKURL:STATE"))))
(config-def! 'checkurl:state config-state)

(define (default-cookiejar)
  (when (and workdir (not (file-directory? workdir)))
    (unless (file-directory? (dirname workdir))
      (mkdirs (dirname workdir)))
    (unless (file-directory? workdir) (mkdir workdir)))
  (unless workdir (set! workdir (tempdir)))
  (set! cookiejar (mkpath workdir "cookies"))
  cookiejar)

(define cookiejar #f)
(varconfig! checkurl:cookiejar cookiejar)

(define verbose #f)
(varconfig! checkurl:verbose verbose)

(define email #f)
(varconfig! checkurl:email email)
(define sms #f)
(varconfig! checkurl:sms sms)
(define email-from "admin@beingmeta.com")
(varconfig! checkurl:from email-from)

(define maxtime 60)
(varconfig! checkurl:maxtime maxtime)

(define nagtime (* 60 30))
(varconfig! checkurl:nagtime nagtime)
(set! nagtime 60)

(define (getopts (opts #f))
  (if opts
      (if (and (test opts 'cookiejar) (test opts 'maxtime))
	  opts
	  (let ((copy (deep-copy opts)))
	    (unless (test opts 'cookiejar)
	      (store! copy 'cookiejar
		      (or cookiejar (default-cookiejar))))
	    (unless (test opts 'maxtime)
	      (store! copy 'maxtime maxtime))
	    copy))
      `#[cookiejar ,(or cookiejar (default-cookiejar))
	 maxtime ,maxtime]))

(define (testok testid url req testfn (opts #f))
  (when (and troubles (test troubles test))
    (drop! troubles test)
    (when statefile (dtype->file troubles statefile))
    (report-resolution testid url req testfn opts))
  (logdebug "Passed test " testid " accessing " url ":\n" (pprint req))
  #t)

(define (report-trouble testid url req ex testfn (opts #f))
  (if req
      (logwarn (stringout testid) " accessing " url ":\n\t"
	       (pprint req))
      (logwarn (stringout testid) " accessing " url ":\n\t" ex))
  (when (or (fail? (get troubles testid))
	     (> (- (time-since (get troubles testid))) nagtime))
    (logwarn |InformAuthorities|
      "Informing the authorities about the failure of " testid " on " url)
    (when email
      (ses/call `#[to ,email from ,email-from
		   subject ,(stringout "Failed " testid " @ " url)
		   text ,(stringout
			   "The test " testid " on " url " has failed.\n"
			   (when ex
			     (if (error? ex)
				 (printout "The error was "
				   (error-condition ex) " in "
				   (error-context ex)
				   (when (error-details ex)
				     (printout
				       "\n\treporting " (write (error-details ex))))
				   (when (error-irritant? ex)
				     (printout "\n\twith irritant: "
				       (pprint (error-irritant ex)))))
				 (printout "The surprising condition was\n\t"
				   (pprint ex #t 80 "\t"))))
			   (when req
			     (printout "The network response was:\n")
			     (pprint req)))]))
    (when sms
      (twilio/send (stringout "Failed " testid " @ " url) sms))
    (when troubles (store! troubles testid (timestamp 'seconds)))
    (when statefile (dtype->file troubles statefile)))
  #f)

(define (report-resolution testid url req testfn (opts #f))
  (when (test troubles testid)
    (logwarn (stringout testid) " resolved for " url ":\n\t" (pprint req))
    (drop! troubles testid)
    (when statefile (dtype->file troubles statefile))
    (when email
      (ses/call `#[to ,email from ,email-from
		   subject ,(stringout "Success on " testid " @ " url)
		   text ,(stringout (pprint req))]))
    (when sms
      (twilio/send (stringout "Success on " testid " @ " url) sms)))
  #t)

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

(define (checkurl-inner testid url (opts #f) (testfn #f))
  (set! opts (getopts opts))
  (let ((req (onerror (if (and (table? testfn)
			       (test testfn '{location response}))
			  (urlget url opts)
			  (urlget+ url opts))
	       (lambda (ex)
		 (report-trouble testid url #f ex testfn opts)))))
    (cond ((not req) #f)
	  ((and testfn (applicable? testfn))
	   (if (testfn req)
	       (testok testid url req testfn opts)
	       (report-trouble testid url req #f testfn opts)))
	  ((and testfn (slotmap? testfn))
	   (and
	    (or (not (test testfn 'response))
		(test-response req (get testfn 'response))
		(report-trouble testid url req
				`#[test response
				   expect ,(get testfn 'response)
				   got ,(get req 'response)]
				testfn opts))
	    (or (not (test testfn 'location))
		(test-location req (get testfn 'location))
		(report-trouble testid url req
				`#[test location
				   expect ,(get testfn 'location)
				   got ,(get req 'location)]
				testfn opts))
	    (or (not (test testfn 'content))
		(test-content req (get testfn 'content))
		(report-trouble testid url req
				`#[test content
				   expect ,(get testfn 'content)
				   got ,(get req '%content)]
				testfn opts))
	    (testok testid url req testfn opts)))
	  ((number? testfn)
	   (if (identical? testfn (get req 'response))
	       (testok testid url req testfn opts)
	       (report-trouble testid url req
			       `#[test response expect ,testfn
				  got ,(get req 'response)]
			       testfn opts)))
	  ((and (test req 'response) (>= 399 (get req 'response) 200))
	   (testok testid url req testfn opts))
	  (else (report-trouble testid url req #f testfn opts)))))

(define (checkurl testid url (opts (getopts)) (testfn #f))
  (let ((result (checkurl-inner testid url opts testfn)))
    (if result
	(if verbose
	    (logwarn "Successful test " testid " @ " url)
	    (lognotice "Successful test " testid " @ " url))
	(logwarn "Failed test " testid " @ " url))
    result))
