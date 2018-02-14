(use-module '{logger optimize})
(load-component "common.scm")
(config! 'optalltest #t)
(config! 'traceload #t)
(config! 'mysql:lazyprocs #f)
(define %loglevel %info%)

(define check-modules
  (macro expr `(begin (use-module ,(cadr expr)))))

(check-modules '{cachequeue calltrack checkurl codewalker
		 couchdb ctt curlcache dopool dropbox ellipsize
		 email extoids ezrecords fakezip fifo fillin
		 findcycles getcontent gpath gravatar gutdb
		 hashfs hashstats histogram hostinfo i18n
		 ice isbn jsonout logctl logger
		 meltcache mimeout mimetable
		 mttools oauth openlibrary optimize
		 opts packetfns parsetime bugjar
		 pump readcsv rulesets samplefns
		 savecontent saveopt signature speling ;; soap
		 stringfmts tinygis tracer trackrefs twilio
		 updatefile usedb varconfig whocalls ximage})

(check-modules '{aws aws/s3 aws/ses aws/simpledb aws/sqs aws/v4
		 aws/associates aws/dynamodb})

(check-modules '{domutils domutils/index domutils/localize
		 domutils/styles domutils/css domutils/cleanup
		 domutils/adjust domutils/analyze
		 domutils/hyphenate})

(check-modules '{facebook facebook/fbcall facebook/fbml})

(check-modules '{google google/drive})

(check-modules '{knodules knodules/drules
		 knodules/html knodules/plaintext})

(check-modules '{misc/oidshift})

(check-modules '{paypal paypal/checkout paypal/express paypal/adaptive})

(check-modules '{textindex textindex/domtext})

(check-modules '{twitter})

(check-modules '{morph morph/en morph/es})

(define (have-brico)
  (and (config 'bricosource)
       (onerror (begin (use-module 'brico) #t) #f)))

(when (have-brico)
  (check-modules '{brico brico/dterms brico/indexing brico/lookup
		   brico/analytics brico/maprules brico/xdterms
		   brico/wikipedia
		   knodules/usebrico knodules/defterm
		   xtags rdf audit}))
