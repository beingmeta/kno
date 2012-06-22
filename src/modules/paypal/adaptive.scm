;;; -*- Mode: Scheme; -*-

(in-module 'paypal/adaptive)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools ezrecords parsetime varconfig})
(use-module 'paypal)

(define return-url "https://www.sbooks.net/completed")
(define cancel-url "https://www.sbooks.net/cancelled")
(define (getmemo x)
  (if (uuid? x) (uuid->string x)
      (if (string? x) x (->string x))))

(module-export! '{paypal/start paypal/details})

(define (paypal/start spec (raw #f))
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
		  "returnUrl" ,(getopt spec 'return return-url)
		  "cancelUrl" ,(getopt spec 'cancel cancel-url)
		  "memo" ,(getmemo (getopt spec 'invoice (getuuid)))
		  "reverseAllParallelPaymentsOnError" "true"
		  "trackingId"
		  ,(if (uuid? invoice) (uuid->string invoice)
		       invoice)]))
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
	   (response (urlpost url curlargs (scripturl+ #f args))))
      (if raw response
	  (if (test response 'type {"text/xml"  "application/json"})
	      (let* ((parsed (if (test response 'type "text/xml")
				 (xmlparse (get response '%content))
				 (jsonparse (get response '%content))))
		     (paykey (get parsed 'paykey))
		     (formurl
		      (stringout
			"https://" (if (getopt spec 'live pp:live)
				       "www.paypal.com"
				       "www.sandbox.paypal.com")
			"/cgi-bin/webscr")))
		(when (if (test response 'type "text/xml")
			  (exists? (xmlget parsed 'faultmessage))
			  (exists? (get parsed 'error)))
		  (error "PayPal call returned error" response))
		(store! parsed 'invoice invoice)
		(unless (getopt spec 'action #f)
		  (store! parsed 'action formurl))
		(store! parsed 'invoice invoice)
		(cons parsed spec)))))))

(define (add-receiver! args receiver opts i)
  (cond ((pair? receiver)
	 (store! args
		 (glom "receiverList.receiver(" i ").email")
		 (car receiver))
	 (store! args
		 (glom "receiverList.receiver(" i ").amount")
		 (cdr receiver))
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
		 (get receiver 'amount))
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

(define (paypal/details spec (raw #f))
  (if (string? spec) (set! spec `#[paykey ,spec]))
  (let* ((paykey (getopt spec 'paykey))
	 (url (getopt spec 'url
		      (if (getopt spec 'live pp:live)
			  "https://svcs.paypal.com/AdaptivePayments/PaymentDetails"
			  "https://svcs.sandbox.paypal.com/AdaptivePayments/PaymentDetails")))
	 (args `#["payKey" ,paykey
		  "requestEnvelope.errorLanguage"
		  ,(getopt spec 'language  "en_US")]))
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
      (if raw response
	  (if (test response 'type {"text/xml"  "application/json"})
	      (let ((parsed (if (test response 'type "text/xml")
				(xmlparse (get response '%content))
				(jsonparse (get response '%content)))))
		(when (if (test response 'type "text/xml")
			  (exists? (xmlget parsed 'faultmessage))
			  (exists? (get parsed 'error)))
		  (error "PayPal call returned error" response))
		(cons parsed spec)))))))


