;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc. All rights reserved

(in-module 'bugjar/servlet)

(use-module '{fdweb xhtml texttools})
(use-module '{varconfig logger})
(use-module '{gpath condense})
(use-module '{bugjar bugjar/html})
(use-module '{aws aws/s3})

(define %loglevel %debug%)

(loginfo |Bugjar/Servlet| "Loading")

(module-export! '{main webmain get-bugroot})

(define bug-buckets
  #["us-east" "beingmeta-us-east"
    "us-east-1" "beingmeta-us-east"
    "us-east-2" "beingmeta-us-east-2"	
    "US-west" "beingmeta-us-west"
    "us-northwest" "beingmeta-us-northwest"
    "eu-east" "beingmeta-eu-east"
    "eu-west" "beingmeta-eu-west"
    "asia-south" "beingmeta-asia-south"
    "asia-japan" "beingmeta-asia-japan"])

(define default-bug-bucket
  "beingmeta-us-east")

(define (get-bugroot hostname)
  (if (string-starts-with? 
       hostname {#({"local" "site"} ".bugs.")
		 #("bugs." {"local" "site"} ".")})
      bugjar/saveroot
      (let ((region (text->frames
		     #{#((bos) (label (not> ".") region) ".bugs.")
		       #((bos) "bugs." (label (not> ".") region) ".")})))
	(s3/loc (try (get bug-buckets (get region 'region)) 
		     default-bug-bucket)))))

(define (main (http_host (req/get 'http_host))
	      (request_uri (req/get 'request_uri)))
  (debug%watch "MAIN" REQUEST_URI)
  (let* ((root (get-bugroot http_host))
	 (bugpath (gp/mkpath root (strip-prefix request_uri "/")))
	 (bugdata (gpath->dtype bugpath))
	 (expanded (condense/expand bugdata)))
    (debug%watch "MAIN/OUTPUT" bugpath root bugdata expanded request_uri)
    (exception/page expanded)))

(define webmain main)
