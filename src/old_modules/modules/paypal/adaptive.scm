;;; -*- Mode: Scheme; -*-

(in-module 'paypal/adaptive)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools ezrecords
	      parsetime varconfig logger opts})
(use-module 'paypal)

(define-init %loglevel %notify%)

(define (getmemo x)
  (if (uuid? x) (uuid->string x)
      (if (string? x) x (->string x))))

(module-export! '{paypal/adaptive/start paypal/adaptive/details})

(define (paypal/adaptive/start spec)
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://svcs.paypal.com/AdaptivePayments/Pay"
		     "https://svcs.sandbox.paypal.com/AdaptivePayments/Pay"))
	 (url (getopt spec 'url payurl))
	 (invoice (getopt spec 'invoice (getuuid)))
	 (args `#["actionType" "PAY"
		  "currencyCode" ,(getopt spec 'currency "USD")
		  "requestEnvelope.errorLanguage"
		  ,(getopt spec 'errlang (getopt spec 'language  "en_US"))
		  "feesPayer"
		  ,(getopt spec 'fees "EACHRECEIVER")
		  "returnUrl" ,(getopt spec 'return paypal/return-url)
		  "cancelUrl" ,(getopt spec 'cancel paypal/cancel-url)
		  "no_shipping" ,(if (getopt spec 'shipping #f) 0 1)
		  "memo" ,(getopt spec 'memo
				  (paypal/uuid (getopt spec 'invoice (getuuid))))
		  "reverseAllParallelPaymentsOnError" "true"
		  "trackingId" ,(paypal/uuid invoice)]))
    (when (getopt spec 'notify #f)
      (store! args "ipnNotificationUrl" (getopt spec 'ipnurl #f)))
    (when (getopt spec 'detail #f)
      (store! args "detailLevel"
	      (if (eq? #t (getopt spec 'detail #f))
		  "returnAll"
		  (getopt spec 'detail #f))))
    (do-choices (receiver (getopt spec 'receivers) i)
      (add-receiver! args receiver
		     (if (getopt spec 'invoice #f) spec
			 (cons `#[invoice ,invoice]  spec))
		     i))
    (let* ((curlargs `#[HEADER
			#["X-PAYPAL-REQUEST-DATA-FORMAT" "NV"
			  "X-PAYPAL-SECURITY-USERID"
			  ,(getopt spec 'pp:user pp:user)
			  "X-PAYPAL-SECURITY-PASSWORD"
			  ,(getopt spec 'pp:pass pp:pass)
			  "X-PAYPAL-SECURITY-SIGNATURE"
			  ,(getopt spec 'pp:sig pp:sig)
			  "X-PAYPAL-APPLICATION-ID"
			  ,(getopt spec 'pp:appid pp:appid)
			  "X-PAYPAL-RESPONSE-DATA-FORMAT" "JSON"]
			CONTENT-TYPE "application/x-www-form-urlencoded"])
	   (argdata (scripturl+ #f args))
	   (response (urlpost url curlargs argdata)))
      (if (test response 'type {"text/xml"  "application/json"})
	  (let* ((parsed (if (test response 'type "text/xml")
			     (xmlparse (get response '%content))
			     (jsonparse (get response '%content))))
		 (trouble (try (if (test response 'type "text/xml")
				   (xmlget parsed 'faultmessage)
				   (first (get parsed 'error)))
			       #f))
		 (paykey (get parsed 'paykey))
		 (formurl
		  (stringout
		    "https://" (if (getopt spec 'live pp:live)
				   "www.paypal.com"
				   "www.sandbox.paypal.com")
		    "/cgi-bin/webscr")))
	    (when trouble
	      (irritant (cons trouble response)
			|PayPalFail| PAYPAL/ADAPTIVE/START
			(try (get trouble 'message)
			     "PayPal call returned error")))
	    (store! parsed 'invoice invoice)
	    (store! parsed 'payid paykey)
	    (store! parsed 'payurl
		    (scripturl formurl 
			"cmd" "_ap-payment"
			"paykey" paykey))
	    (store! parsed 'invoice invoice)
	    (store! parsed 'api 'ppadaptive)
	    parsed)))))

(define (add-receiver! args receiver opts i)
  (cond ((pair? receiver)
	 (store! args
		 (glom "receiverList.receiver(" i ").email")
		 (car receiver))
	 (store! args
		 (glom "receiverList.receiver(" i ").amount")
		 (paypal/amount (cdr receiver)))
	 (when (or (equal? (getopt opts 'primary #f) receiver)
		   (equal? (getopt opts 'primary #f) (car receiver)))
	   (store! args
		   (glom "receiverList.receiver(" i ").primary")
		   "true")))
	((slotmap? receiver)
	 (store! args
		 (glom "receiverList.receiver(" i ").email")
		 (get receiver 'email))
	 (store! args
		 (glom "receiverList.receiver(" i ").amount")
		 (paypal/amount (get receiver 'amount)))
	 (when (test receiver 'primary)
	   (store! args
		   (glom "receiverList.receiver(" i ").primary")
		   "true"))
	 (if (test receiver 'digital)
	     (store! args
		     (glom "receiverList.receiver(" i ").paymentType")
		     "DIGITALGOODS")
	     (if (test receiver 'type)
		 (store! args
			 (glom "receiverList.receiver(" i ").paymentType")
			 (get receiver 'type))
		 (if (getopt opts 'paytype #f)
		     (store! args
			     (glom "receiverList.receiver(" i ").paymentType")
			     (getopt opts 'paytype #f)))))
	 (when (getopt opts 'invoice #f)
	   (store! args
		   (glom "receiverList.receiver(" i ").invoiceId")
		   (glom (getopt opts 'invoice #f) "-" i))))))

;;; Getting details

(define (paypal/adaptive/details spec)
  (if (string? spec) (set! spec `#[paykey ,spec])
      (if (uuid? spec) (set! spec `#[invoice ,spec])))
  (if (not (table? spec)) (set! spec `#[id ,spec]))
  (let* ((ppurl
	  (if (getopt spec 'live pp:live)
	      "https://svcs.paypal.com/AdaptivePayments/PaymentDetails"
	      "https://svcs.sandbox.paypal.com/AdaptivePayments/PaymentDetails"))
	 (url (getopt spec 'endpoint ppurl))
	 (args (if (or (testopt spec 'paykey) (testopt spec 'payid))
		   `#["payKey" ,(getopt spec 'paykey (getopt spec 'payid))
		      "requestEnvelope.errorLanguage"
		      ,(getopt spec 'language  "en_US")]
		   (if (testopt spec 'invoice)
		       `#["trackingId" ,(paypal/uuid (getopt spec 'invoice))
			  "requestEnvelope.errorLanguage"
			  ,(getopt spec 'language  "en_US")]
		       (error |MissingID| paypal/details
			      "Need invoice (UUID) or paykey to get transaction details")))))
    (when (getopt spec 'detail #f)
      (store! args "detailLevel"
	      (if (eq? #t (getopt spec 'detail #f))
		  "returnAll"
		  (getopt spec 'detail #f))))
    (let* ((curlargs `#[HEADER
			#["X-PAYPAL-REQUEST-DATA-FORMAT" "NV"
			  "X-PAYPAL-SECURITY-USERID"
			  ,(getopt spec 'pp:user pp:user)
			  "X-PAYPAL-SECURITY-PASSWORD"
			  ,(getopt spec 'pp:pass pp:pass)
			  "X-PAYPAL-SECURITY-SIGNATURE"
			  ,(getopt spec 'pp:sig pp:sig)
			  "X-PAYPAL-APPLICATION-ID"
			  ,(getopt spec 'pp:appid pp:appid)
			  "X-PAYPAL-RESPONSE-DATA-FORMAT" "JSON"]
			CONTENT-TYPE "application/x-www-form-urlencoded"])
	   (response (urlpost url curlargs (scripturl+ #f args))))
      (if (test response 'type {"text/xml"  "application/json"})
	  (let* ((parsed (if (test response 'type "text/xml")
			     (xmlparse (get response '%content))
			     (jsonparse (get response '%content))))
		 (trouble (if (test response 'type "text/xml")
			      (exists? (xmlget parsed 'faultmessage))
			      (exists? (get parsed 'error)))))
	    (when trouble
	      (error "PayPal call returned error" response))
	    (modify-frame parsed
	      'api (getopt spec 'api)
	      'payid (getopt spec 'payid)
	      'completed (not trouble)))))))



