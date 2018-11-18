(check-modules '{cachequeue calltrack checkurl codewalker
		 couchdb curlcache dopool dropbox ellipsize ;; ctt
		 email extoids ezrecords fakezip fifo fillin
		 findcycles getcontent gpath gravatar gutdb
		 hashfs hashstats histogram hostinfo i18n
		 ice isbn jsonout logctl logger
		 meltcache mimeout mimetable
		 mttools oauth openlibrary ;; optimize
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
       (or (position #\@ (config 'bricosource) )
	   (position #\: (config 'bricosource) )
	   (file-exists? (config 'bricosource)))
       (onerror (begin (get-module 'brico) #t) #f)))

(when (have-brico)
  (check-modules '{brico brico/dterms brico/indexing brico/lookup
		   brico/analytics brico/maprules brico/xdterms
		   brico/build/wikipedia brico/build/wordnet brico/build/wikidata
		   knodules/usebrico knodules/defterm
		   xtags rdf audit}))

