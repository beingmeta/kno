;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc. All rights reserved.

(in-module 'testmodules)

(use-module '{logger optimize})

(module-export! '{test-module
		  test-root-modules test-other-modules test-beingmeta-modules
		  test-all-modules})

(define root-modules
  '{audit cachequeue calltrack checkurl codewalker
    couchdb ctt curlcache delicious dopool dropbox ellipsize email
    extoids ezrecords fakezip fifo fillin findcycles getcontent
    gpath gravatar gutdb hashfs hashstats histogram hostinfo i18n
    ice jsonout lexml logctl logger meltcache mergeutils mimeout
    mimetable mttools myweb oauth openlibrary optimize opts
    packetfns parsetime pump rdf readcsv recycle rss rulesets
    samplefns sandbox savecontent saveopt signature soap speling
    stringfmts tighten tinygis tracer trackrefs twilio updatefile
    usedb varconfig whocalls xtags})

(define other-modules
  '{(AWS AWS/S3 AWS/V4 AWS/AWS AWS/SES AWS/SQS AWS/DYNAMODB
	 AWS/SIMPLEDB AWS/ASSOCIATES)
    (DOMUTILS DOMUTILS/CSS DOMUTILS/INDEX DOMUTILS/ADJUST DOMUTILS/STYLES
	      DOMUTILS/ANALYZE DOMUTILS/CLEANUP DOMUTILS/DOMUTILS DOMUTILS/LOCALIZE
	      DOMUTILS/HYPHENATE DOMUTILS/DOMANALYZE)
    (BUGJAR)
    (KNODULES KNODULES/HTML KNODULES/DRULES KNODULES/DEFTERM
	      KNODULES/KNODULES KNODULES/USEBRICO KNODULES/PLAINTEXT)
    (PAYPAL PAYPAL/PAYPAL PAYPAL/EXPRESS PAYPAL/ADAPTIVE
	    PAYPAL/CHECKOUT)
    (FACEBOOK FACEBOOK/FBML FACEBOOK/FBCALL FACEBOOK/FACEBOOK)
    (TWITTER TWITTER/TWITTER)
    (GOOGLE GOOGLE/DRIVE GOOGLE/GOOGLE)
    (TEXTINDEX TEXTINDEX/DOMTEXT TEXTINDEX/TEXTINDEX)
    (BRICO BRICO/BRICO BRICO/DTERMS BRICO/LOOKUP BRICO/XDTERMS
	   BRICO/INDEXING BRICO/MAPRULES BRICO/ANALYTICS BRICO/WIKIPEDIA
	   BRICO/DTERMCACHE)
    (MISC/OIDSHIFT)
    (TESTS/TEST-MTTOOLS)
    (WEBAPI/FACEBOOK)})

(define beingmeta-modules {})

(define (test-module name)
  (if (get-module name)
      (optimize-module! name)
      (logwarn |LoadFailed| "Couldn't load module " name)))

(define (test-root-modules)
  (do-choices (module root-modules) (test-module module)))

(define (test-other-modules)
  (do-choices (module-list other-modules)
    (test-module (car module-list))
    (test-module (elts (cdr module-list)))))

(define (test-beingmeta-modules)
  (do-choices (module-list beingmeta-modules)
    (test-module (car module-list))
    (test-module (elts (cdr module-list)))))

(define (test-all-modules)
  (test-root-modules)
  (test-other-modules)
  ;; (test-beingmeta-modules)
  )
