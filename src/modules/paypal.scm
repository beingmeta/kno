;;; -*- Mode: Scheme; -*-

(in-module 'paypal)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools ezrecords parsetime varconfig})

(define live #f)
(define business "anonymous@company.com")
(define default-options #[])
(define default-button
  "<input type='image' name='submit' border='0' src='https://www.paypal.com/en_US/i/btn/btn_buynow_LG.gif' alt='PayPal - The safer, easier way to pay online'/>")

(varconfig! pp:live live)
(varconfig! pp:business business)
(varconfig! pp:options default-options)
(varconfig! pp:button default-button)

(define (paypal/fields (options default-options))
  (input TYPE "HIDDEN" NAME "cmd" VALUE (getopt options 'cmd "_xclick"))
  (if (getopt options 'invoice)
      (input TYPE "HIDDEN" NAME "invoice" VALUE (getopt options 'invoice))
      (input TYPE "HIDDEN" NAME "invoice" VALUE (uuid->string (getuuid))))
  (when (getopt options 'amount) (input TYPE "HIDDEN" NAME "amount" VALUE (getopt options 'amount)))
  (input TYPE "HIDDEN" NAME "amount" VALUE (getopt options 'amount))
  (input TYPE "HIDDEN" NAME "business" VALUE (getopt options 'business business))
  (when (getopt options 'item_number)
    (input TYPE "HIDDEN" NAME "item_number" VALUE (getopt options 'item_number)))
  (input TYPE "HIDDEN" NAME "charset" VALUE (getopt options 'charset "utf-8"))
  (when (getopt options 'return)
    (input TYPE "HIDDEN" NAME "return" VALUE (getopt options 'return)))
  (when (getopt options 'item_name)
    (input TYPE "HIDDEN" NAME "item_name" VALUE (getopt options 'item_name)))
  (when (getopt options 'notify_url)
    (input TYPE "HIDDEN" NAME "notify_url" VALUE (getopt options 'notify_url)))
  (when (getopt options 'cancel)
    (input TYPE "HIDDEN" NAME "cancel_return" VALUE (getopt options 'cancel))))

(define paypal/form
  (macro expr
    `(let* ((pp:options ,(second expr))
	    (pp:needbutton #t)
	    (pp:buttonopt (getopt pp:options 'button (config 'pp:button)))
	    (pp:button (lambda ()
			 (set! pp:needbutton #f)
			 (cond ((string? pp:buttonopt) (xhtml pp:buttonopt))
			       ((applicable? pp:buttonopt) (pp:buttonopt))
			       ((and (table? pp:buttonopt) (test pp:buttonopt '%xmltag))
				(xmleval pp:buttonopt))
			       (else (xhtml ,default-button))))))
       (,xmlblock "FORM"
	   ((action (getopt pp:options 'action
			    (if (getopt pp:options 'live (config 'pp:live))
				"https://www.paypal.com/cgi-bin/webscr"
				"https://www.sandbox.paypal.com/cgi-bin/webscr")))
	    (method "POST"))
	 (,paypal/fields pp:options)
	 ,@(cddr expr)
	 (when pp:needbutton (pp:button))))))

(module-export! '{paypal/form paypal/fields paypal/opts})
