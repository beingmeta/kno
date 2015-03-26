;;; -*- Mode: Scheme; -*-

(in-module 'paypal/express)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools ezrecords
	      parsetime varconfig logger opts})
(use-module 'paypal)

(define-init %loglevel %notify!)
;;(define %loglevel %debug!)

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
	   (ok (response/ok? response))
	   (parsed (and ok (cgiparse (get response '%content)))))
      (debug%watch "PAYPAL/EXPRESS/START" spec requrl response parsed)
      (if (and parsed (test parsed 'ack "Success"))
	  parsed
	  (irritant (cons parsed response) |PayPalFail|)))))

(define (paypal/express/details spec)
  (let* ((payurl (if (getopt spec 'live pp:live)
		     "https://api-3t.paypal.com/nvp"
		     "https://api-3t.sandbox.paypal.com/nvp"))
	 (url (getopt spec 'endpoint payurl))
	 (invoice (getopt spec 'invoice (getuuid)))
	 (args `#["METHOD" "GetExpressCheckoutDetails"
		  "TOKEN" ,(getopt spec 'token (getopt spec 'payid))
		  "USER" ,(getopt spec 'pp:user pp:user)
		  "PWD" ,(getopt spec 'pp:pass pp:pass)
		  "SIGNATURE" ,(getopt spec 'pp:sig pp:sig)
		  "VERSION" ,(getopt spec 'version express-api-version)]))
    (let* ((requrl (scripturl+ url args))
	   (response (urlget requrl))
	   (ok (response/ok? response))
	   (parsed (and ok (cgiparse (get response '%content)))))
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
		  "USER" ,(getopt spec 'pp:user pp:user)
		  "PWD" ,(getopt spec 'pp:pass pp:pass)
		  "SIGNATURE" ,(getopt spec 'pp:sig pp:sig)
		  "VERSION" ,(getopt spec 'version express-api-version)]))
    (let* ((requrl (scripturl+ url args))
	   (response (urlget requrl))
	   (ok (response/ok? response))
	   (parsed (and ok (cgiparse (get response '%content)))))
      (debug%watch "PAYPAL/EXPRESS/FINISHED" spec requrl response parsed)
      (if (and parsed (test parsed 'ack {"Success" "SuccessWithWarning"}))
	  (modify-frame parsed
	    'completed (test parsed 'PAYMENTINFO_0_PAYMENTSTATUS
			     "Completed"))
	  (irritant (cons parsed response)
		    |PayPalFail| PAYPAL/EXPRESS/DETAILS)))))
