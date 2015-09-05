;;; -*- Mode: Scheme; -*-

(in-module 'paypal/checkout)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools jsonout ezrecords oauth
	      parsetime varconfig logger opts})
(use-module 'paypal)

(module-export! '{paypal/checkout/start 
		  paypal/checkout/details
		  paypal/checkout/finish})

(define-init %loglevel %notify%)

(define paypal-creds #f)
(define get-paypal-creds
  (slambda ()
    (or paypal-creds
	(let ((client-creds
	       (oauth/getclient (if pp:live 'paypalapi 'paypalapitest))))
	  (set! paypal-creds client-creds)
	  client-creds))))

(define (paypal/checkout/start spec (raw #f))
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://api.paypal.com/v1/payments/payment"
		     "https://api.sandbox.paypal.com/v1/payments/payment"))
	 (url (getopt spec 'endpoint payurl))
	 (invoice (getopt spec 'invoice (getuuid)))
	 (creds (or paypal-creds (get-paypal-creds)))
	 (body `#["intent" "sale"
		  "payer" #["payment_method" "paypal"]
		  "transactions"
		  #(#["amount"
		      #["total" ,(paypal/amount (getopt spec 'amount))
			"currency" ,(getopt spec 'currency "USD")]
		      "description" 
		      ,(try (getopt spec 'description) "an item of value")
		      "invoice_number" 
		      ,(if (uuid? invoice) (uuid->string invoice) invoice)])
		  "redirect_urls"
		  #["return_url" ,(getopt spec 'return paypal/return-url)
		    "cancel_url" ,(getopt spec 'cancel paypal/cancel-url)]]))
    (let ((response
	   (oauth/call creds 'post url '() (stringout (jsonout body))
		       "application/json")))
      (modify-frame response
	'payid (get response 'id) 'api 'ppcheckout
	'payurl (get (pick (elts (get response 'links))
			   'rel "approval_url")
		     'href)))))

(define (paypal/checkout/details spec)
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://api.paypal.com/v1/payments/payment"
		     "https://api.sandbox.paypal.com/v1/payments/payment"))
	 (url (getopt spec 'endpoint payurl))
	 (creds (or paypal-creds (get-paypal-creds))))
    (let* ((requrl (glom url "/" (get spec 'payid)))
	   (response (oauth/call creds 'GET requrl))
	   (parsed (jsonparse (get response '%content))))
      (debug%watch "PAYPAL/CHECKOUT/FINISHED" spec requrl response parsed)
      (modify-frame parsed
	'completed (test parsed 'state "approved")
	'payid (get response 'id) 'api 'ppcheckout
	'payurl (get (pick (elts (get response 'links))
			   'rel "approval_url")
		     'href)))))

(define (paypal/checkout/finish spec)
  (let* ((ppurl (if (getopt spec 'live pp:live)
		    "https://api.paypal.com/v1/payments/payment"
		    "https://api.sandbox.paypal.com/v1/payments/payment"))
	 (url (getopt spec 'endpoint ppurl))
	 (creds (or paypal-creds (get-paypal-creds))))
    (let* ((requrl (glom url "/" (get spec 'payid) "/execute"))
	   (body (stringout (jsonout #["payer_id" (getopt spec 'payer_id (getopt spec 'payerid))])))
	   (response (oauth/call creds requrl '() body "application/json"))
	   (ok (response/ok? response))
	   (parsed (and ok (jsonparse (get response '%content)))))
      (debug%watch "PAYPAL/CHECKOUT/FINISHED" spec requrl response parsed)
      (if (and ok (test parsed 'state "approved"))
	  (modify-frame parsed
	    'api 'ppcheckout 'payid (getopt spec 'payid)
	    'completed (test parsed 'state "approved"))
	  (irritant (cons parsed response)
		    |PayPalFail| PAYPAL/CHECKOUT/DETAILS)))))


