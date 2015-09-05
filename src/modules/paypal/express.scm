;;; -*- Mode: Scheme; -*-

(in-module 'paypal/express)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools ezrecords
	      parsetime varconfig logger opts})
(use-module 'paypal)

(define-init %loglevel %notify%)

(define express-api-version "121.0")

(module-export!
 '{paypal/express/start paypal/express/details
   paypal/express/finish})

(define (paypal/express/start spec (raw #f))
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://api-3t.paypal.com/nvp"
		     "https://api-3t.sandbox.paypal.com/nvp"))
	 (url (getopt spec 'endpoint payurl))
	 (invoice (getopt spec 'invoice (getuuid)))
	 (args `#["METHOD" "SetExpressCheckout"
		  "PAYMENTREQUEST_0_AMT"
		  ,(paypal/amount (getopt spec 'amount 10.00))
		  "PAYMENTREQUEST_0_PAYMENTACTION"
		  (getopt spec 'action "Sale")
		  "PAYMENTREQUEST_0_CURRENCYCODE"
		  ,(getopt spec 'currency "USD")
		  "PAYMENTREQUEST_0_INVNUM"
		  ,(paypal/uuid (getopt spec 'invoice (getuuid)))
		  "PAYMENTREQUEST_0_DESC"
		  ,(getopt spec 'description "an item of great value")
		  "NOSHIPPING" 1 "ALLOWNOTE" 1
		  "TOTALTYPE" ,(getopt spec 'totaltype "Total")
		  "BUYEREMAILOPTINENABLE"
		  ,(if (getopt spec 'askemail #t) 1 0)
		  "no_shipping" ,(if (getopt spec 'shipping #f) 0 1)
		  "returnUrl" ,(getopt spec 'return paypal/return-url)
		  "cancelUrl" ,(getopt spec 'cancel paypal/cancel-url)
		  "USER" ,(getopt spec 'pp:user
				  (if (getopt spec 'live pp:live)
				      pp:user pp:testuser))
		  "PWD" ,(getopt spec 'pp:pass
				 (if (getopt spec 'live pp:live)
				     pp:pass pp:testpass))
		  "SIGNATURE" ,(getopt spec 'pp:sig
				       (if (getopt spec 'live pp:live)
					   pp:sig pp:testsig))
		  "VERSION" 78]))
    (when (and (getopt spec 'digitalgoods)
	       (not (getopt spec 'items)))
      (store! args "L_PAYMENT_REQUEST_0_QTY0" 1)
      (store! args "L_PAYMENT_REQUEST_0_QTY0" 1)
      (store! args "L_PAYMENT_REQUEST_0_ITEMAMT"
	      (paypal/amount (getopt spec 'amount 10.00)))
      (store! args "L_PAYMENT_REQUEST_0_AMT0"
	      (paypal/amount (getopt spec 'amount 10.00)))
      (store! args "L_PAYMENT_REQUEST_0_NAME0"
	      (getopt spec 'description "an item of great value"))
      (store! args "L_PAYMENT_REQUEST_0_ITEMCATEGORY0" "Digital"))
    (do-choices (item (getopt spec 'items {}) i)
      (store! args (glom "L_PAYMENTREQUEST_0_QTY" i)
	      (getopt item 'quanity 1))
      (store! args (glom "L_PAYMENTREQUEST_0_AMT" i)
	      (getopt item 'amount {}))
      (store! args (glom "L_PAYMENTREQUEST_0_NAME" i)
	      (getopt item 'name {}))
      (store! args (glom "L_PAYMENTREQUEST_0_ITEMCATEGORY" i)
	      (getopt item 'category {})))
    (when (getopt spec 'memo)
      (store! args "PAYMENTREQUEST_0_CUSTOM" (getopt spec 'memo)))
    (let* ((requrl (scripturl+ url args))
	   (response (urlget requrl))
	   (ok (response/ok? response))
	   (parsed (and ok (urldata/parse (get response '%content)))))
      (debug%watch "PAYPAL/EXPRESS/START" spec requrl response parsed)
      (if (and parsed (test parsed 'ack "Success"))
	  (modify-frame parsed
	    'payurl
	    (scripturl 
		(if pp:live
		    "https://www.paypal.com/cgi-bin/webscr"
		    "https://www.sandbox.paypal.com/cgi-bin/webscr")
		"cmd" "_express-checkout"
		"token" (get parsed 'token))
	    'payid (get parsed 'token)
	    'api 'ppexpress)
	  (irritant (cons parsed response) |PayPalFail|)))))

(define (paypal/express/details spec)
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://api-3t.paypal.com/nvp"
		     "https://api-3t.sandbox.paypal.com/nvp"))
	 (url (getopt spec 'endpoint payurl))
	 (invoice (getopt spec 'invoice (getuuid)))
	 (args `#["METHOD" "GetExpressCheckoutDetails"
		  "TOKEN" ,(getopt spec 'token (getopt spec 'payid))
		  "USER" ,(getopt spec 'pp:user
				  (if (getopt spec 'live pp:live)
				      pp:user pp:testuser))
		  "PWD" ,(getopt spec 'pp:pass
				 (if (getopt spec 'live pp:live)
				     pp:pass pp:testpass))
		  "SIGNATURE" ,(getopt spec 'pp:sig
				       (if (getopt spec 'live pp:live)
					   pp:sig pp:testsig))
		  "VERSION" ,(getopt spec 'version express-api-version)]))
    (let* ((requrl (scripturl+ url args))
	   (response (urlget requrl))
	   (ok (response/ok? response))
	   (parsed (and ok (urldata/parse (get response '%content)))))
      (debug%watch "PAYPAL/EXPRESS/DETAILS" spec requrl response parsed)
      (if (and parsed (test parsed 'ack {"Success" "SuccessWithWarning"}))
	  (modify-frame parsed
	    'completed (test parsed 'PAYMENTINFO_0_PAYMENTSTATUS
			     "Completed"))
	  (irritant (cons parsed response) |PayPalFail|)))))

(define (paypal/express/finish spec)
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://api-3t.paypal.com/nvp"
		     "https://api-3t.sandbox.paypal.com/nvp"))
	 (url (getopt spec 'endpoint payurl))
	 (amount (getopt spec 'PAYMENTREQUEST_0_AMT 0))
	 (currency (getopt spec 'PAYMENTREQUEST_0_CURRENCYCODE 0))
	 (args `#["METHOD" "DoExpressCheckoutPayment"
		  "TOKEN" ,(getopt spec 'token)
		  "PAYERID" ,(getopt spec 'payerid)
		  "PAYMENTREQUEST_0_AMT" ,amount
		  "PAYMENTREQUEST_0_CURRENCYCODE" ,currency
		  "PAYMENTREQUEST_0_PAYMENTACTION" "Sale"
		  "USER" ,(getopt spec 'pp:user
				  (if (getopt spec 'live pp:live)
				      pp:user pp:testuser))
		  "PWD" ,(getopt spec 'pp:pass
				 (if (getopt spec 'live pp:live)
				     pp:pass pp:testpass))
		  "SIGNATURE" ,(getopt spec 'pp:sig
				       (if (getopt spec 'live pp:live)
					   pp:sig pp:testsig))
		  "VERSION" ,(getopt spec 'version express-api-version)]))
    (let* ((requrl (scripturl+ url args))
	   (response (urlget requrl))
	   (ok (response/ok? response))
	   (parsed (and ok (urldata/parse (get response '%content)))))
      (debug%watch "PAYPAL/EXPRESS/FINISHED" spec requrl response parsed)
      (if (and parsed (test parsed 'ack {"Success" "SuccessWithWarning"}))
	  (modify-frame parsed
	    'api (getopt spec 'api)
	    'payid (try (get parsed 'payid)
			(getopt spec 'payid (getopt spec 'token)))
	    'completed (test parsed 'PAYMENTINFO_0_PAYMENTSTATUS
			     "Completed"))
	  (irritant (cons parsed response)
		    |PayPalFail| PAYPAL/EXPRESS/DETAILS)))))

