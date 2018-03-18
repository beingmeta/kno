;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc. All rights reserved

(in-module 'bugjar/servlet)

(use-module '{fdweb xhtml texttools})
(use-module '{varconfig logger})
(use-module '{gpath condense})
(use-module '{bugjar bugjar/html})
(use-module '{aws aws/s3})

(define %loglevel %notice%)

(config! 'aws:config 'bugjar_creds)

(module-export! '{main webmain get-bugroot errorpage})

(define-init bug-buckets #[])
(config-def! 'bugjar:buckets
	     (lambda (var (val))
	       (cond ((not (bound? val)) bug-buckets)
		     ((table? val)
		      (do-choices (key (getkeys val))
			(store! bug-buckets key (get val key))))
		     ((string? val)
		      (doseq (defs (textslice val '(+ {";" (isspace) (vspace)}) #f))
			(cond ((position def "=")
			       (store! bug-buckets
				 (decode-entities (slice def 0 (position def "=")))
				 (decode-entities (slice def (1+ (position def "="))))))
			      ((position def ":")
			       (store! bug-buckets
				 (decode-entities (slice def 0 (position def ":")))
				 (decode-entities (slice def (1+ (position def ":"))))))
			      (else (logwarn |BadBucketDef| "Definition " (write def) " from " val)))))
		     (else (logwarn |BadBucketDef| "Definition " (write def) " from " val)))))

(define-init default-bug-bucket 
  "s3://beingmeta-us-east/bugjar/")
(varconfig! bugjar:bucket default-bug-bucket)

(define (get-bugroot hostname)
  (if (string-starts-with? 
       hostname {#({"local" "site"} ".bugs.")
		 #("bugs." {"local" "site"} ".")})
      bugjar/saveroot
      (let ((region (text->frames
		     #{#((bos) (label region (not> ".")) ".bugs.")
		       #((bos) "bugs." (label region (not> ".")) ".")}
		     hostname)))
	(->gpath (try (get bug-buckets (get region 'region)) 
		      default-bug-bucket)))))

(define (main (http_host (req/get 'http_host))
	      (request_uri (req/get 'request_uri)))
  (debug%watch "MAIN" REQUEST_URI)
  (let* ((root (get-bugroot http_host))
	 (bugpath (gp/mkpath root (strip-prefix request_uri "/")))
	 (data (gp/fetch bugpath))
	 (bugdata (and data (onerror (packet->dtype data) #f)))
	 (expanded (and bugdata (condense/expand bugdata))))
    (debug%watch "MAIN/OUTPUT" bugpath root bugdata expanded request_uri)
    (if expanded (exception/page expanded)
	(begin
	  (htmlheader (xmlblock STYLE () "body { font-family: sans,sans-serif;}"))
	  (cond ((not data)
		 (req/set! 'status 404)
		 (title! "Error details were not found")
		 (h1 "No details for error " (strong (tt (basename request_uri))))
		 (p "We looked at the path '" (tt (gpath->string bugpath)) "'"))
		((not bugdata)
		 (req/set! 'status 504)
		 (title! "The stored error details were invalid")
		 (h1 "Corrupted error details for " (strong (tt (basename request_uri))))
		 (p "Invalid error details stored at '" (tt (gpath->string bugpath)) "'"))
		(else
		 (req/set! 'status 504)
		 (title! "Error getting and converting error dump")
		 (p "An internal error was encountered while attempting "
		   "to display error details from '" (tt (gpath->string bugpath)) "'")))))))

(define (errorpage reqerror)
  (exception/page reqerror))

(define webmain main)