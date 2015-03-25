;;; -*- Mode: Scheme; -*-

(in-module 'paypal/express)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools ezrecords
	      parsetime varconfig logger opts})
(use-module 'paypal)

(define-init %loglevel %notify!)
;;(define %loglevel %debug!)

(define express-api-version 78)

(module-export! '{paypal/express/start paypal/express/details})

(define (paypal/express/start spec (raw #f))
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://api-3t.paypal.com/nvp"
		     "https://api-3t.sandbox.paypal.com/nvp"))
	 (url (getopt spec 'url payurl))
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
		  "returnUrl" ,(getopt spec 'return paypal/return-url)
		  "cancelUrl" ,(getopt spec 'cancel paypal/cancel-url)
		  "USER" ,(getopt spec 'pp:user pp:user)
		  "PWD" ,(getopt spec 'pp:pass pp:pass)
		  "SIGNATURE" ,(getopt spec 'pp:sig pp:sig)
		  "VERSION" 78]))
    (when (getopt spec 'memo)
      (store! args "PAYMENTREQUEST_0_CUSTOM" (getopt spec 'memo)))
    (let* ((requrl (scripturl+ url args))
	   (response (urlget requrl))
	   (parsed (cgiparse (get response '%content))))
      (if raw response
	  (if (test parsed 'ack "Success")
	      (cons parsed response)
	      (irritant (cons parsed response) |PayPalFail|))))))

(define (paypal/express/details spec (raw #f))
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://api-3t.paypal.com/nvp"
		     "https://api-3t.sandbox.paypal.com/nvp"))
	 (url (getopt spec 'url payurl))
	 (invoice (getopt spec 'invoice (getuuid)))
	 (args `#["METHOD" "GetExpressCheckoutDetails"
		  "TOKEN" ,(getopt spec 'token)
		  "USER" ,(getopt spec 'pp:user pp:user)
		  "PWD" ,(getopt spec 'pp:pass pp:pass)
		  "SIGNATURE" ,(getopt spec 'pp:sig pp:sig)
		  "VERSION" ,(getopt spec 'version express-api-version)]))
    (let* ((requrl (scripturl+ url args))
	   (response (urlget requrl))
	   (parsed (cgiparse (get response '%content))))
      (if raw response
	  (if (test parsed 'ack "Success")
	      (cons parsed response)
	      (irritant (cons parsed response) |PayPalFail|))))))

(define (paypal/express/finish spec (raw #f))
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://api-3t.paypal.com/nvp"
		     "https://api-3t.sandbox.paypal.com/nvp"))
	 (url (getopt spec 'url payurl))
	 (invoice (getopt spec 'invoice (getuuid)))
	 (args `#["METHOD" "DoExpressCheckoutPayment"
		  "TOKEN" ,(getopt spec 'token)
		  "PAYERID" ,(getopt spec 'payer)
		  "USER" ,(getopt spec 'pp:user pp:user)
		  "PWD" ,(getopt spec 'pp:pass pp:pass)
		  "SIGNATURE" ,(getopt spec 'pp:sig pp:sig)
		  "VERSION" ,(getopt spec 'version express-api-version)]))
    (let* ((requrl (scripturl+ url args))
	   (response (urlget requrl))
	   (parsed (cgiparse (get response '%content))))
      (if raw response
	  (if (test parsed 'ack "Success")
	      (cons parsed response)
	      (irritant (cons parsed response) |PayPalFail|))))))





