;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc. All rights reserved.

(in-module 'testmodules)

(use-module '{logger optimize})

(module-export! '{test-module
		  test-root-modules test-other-modules test-beingmeta-modules
		  test-all-modules})

(define root-modules
  '{audit cachequeue calltrack checkurl codewalker
    couchdb ctt curlcache dopool dropbox ellipsize email
    extoids ezrecords fakezip fifo fillin findcycles getcontent
    gpath gravatar gutdb hashfs hashstats histogram hostinfo i18n
    ice jsonout logctl logger meltcache mergeutils mimeout ;;  lexml
    mimetable mttools oauth openlibrary optimize opts
    packetfns parsetime pump rdf readcsv recycle rss rulesets
    samplefns savecontent saveopt signature speling ;; soap
    stringfmts tinygis tracer trackrefs twilio updatefile ;; tighten
    usedb varconfig whocalls xtags})

(define other-modules
  '{(AWS AWS/S3 AWS/V4 AWS/SES AWS/SQS AWS/DYNAMODB
	 AWS/SIMPLEDB AWS/ASSOCIATES)
    (DOMUTILS DOMUTILS/CSS DOMUTILS/INDEX DOMUTILS/ADJUST DOMUTILS/STYLES
	      DOMUTILS/ANALYZE DOMUTILS/CLEANUP DOMUTILS/LOCALIZE
	      DOMUTILS/HYPHENATE)
    (BUGJAR)
    (KNODULES KNODULES/HTML KNODULES/DRULES KNODULES/DEFTERM
	      KNODULES/USEBRICO KNODULES/PLAINTEXT)
    (PAYPAL PAYPAL/EXPRESS PAYPAL/ADAPTIVE
	    PAYPAL/CHECKOUT)
    (FACEBOOK FACEBOOK/FBML FACEBOOK/FBCALL)
    (TWITTER)
    (GOOGLE GOOGLE/DRIVE)
    (TEXTINDEX TEXTINDEX/DOMTEXT)
    (BRICO BRICO/DTERMS BRICO/LOOKUP BRICO/XDTERMS
	   BRICO/INDEXING BRICO/MAPRULES BRICO/ANALYTICS BRICO/WIKIPEDIA
	   BRICO/DTERMCACHE)
    (MISC/OIDSHIFT)
    (TESTS/MTTOOLS)
    (WEBAPI/FACEBOOK)})

(define beingmeta-modules {})

(define (test-module name)
  (logwarn |Testing Module| "Module " name)
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
