(check-modules '{condense dopool engine sqloids ezrecords fifo
		 gpath jsonout logctl logger mimetable
		 mttools packetfns parsetime readfile
		 stringfmts usedb varconfig
		 bench})

(check-modules '{flexdb flexdb/flexpool flexdb/flexindex
		 flexdb/adjuncts flexdb/branches flexdb/registry
		 flexdb/typeindex 
		 ;; Kind of legacy
		 flexdb/slotindex flexdb/splitpool})

(check-modules '{bugjar bugjar/html bugjar/servlet})

(check-modules '{xhtml/auth xhtml/buglog xhtml/clickit 
		 xhtml/datetime xhtml/download xhtml/entities
		 xhtml/exceptions xhtml/include
		 xhtml/pagedate xhtml/tableout})

(when (get-module 'mongodb)
  (check-modules '{mongodb/utils
		   mongodb/pools mongodb/indexes 
		   mongodb/slots mongodb/orm}))

(check-modules
 '{opts hashfs rulesets meltcache curlcache saveopt signature cachequeue 
   calltrack checkurl codewalker couchdb dropbox ellipsize email fakezip fillin 
   findcycles getcontent gravatar gutdb hashstats histogram hostinfo i18n ice 
   isbn mimeout oauth openlibrary bugjar pump readcsv samplefns savecontent 
   speling tinygis tracer trackrefs twilio updatefile whocalls ximage})

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

;;(check-modules '{textindex textindex/domtext})

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
		   brico/build/wordnet
		   knodules/usebrico knodules/defterm
		   xtags rdf audit}))

