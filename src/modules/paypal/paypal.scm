;;; -*- Mode: Scheme; -*-

(in-module 'paypal)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools ezrecords parsetime logger varconfig})

(define-init %loglevel %notify%)

(define pp:live #f)
(define pp:business "anonymous@company.com")
(define pp:testbusiness "anonymous@company.com")
(define pp:appid "APP-80W284485P519543T")
(define pp:user "store_1288290819_biz_api1.beingmeta.com")
(define pp:testuser "store_1288290819_biz_api1.beingmeta.com")
(define pp:pass "1288290827")
(define pp:testpass "1288290827")
(define pp:sig "Ahtmaqpduek-SZn7-BlOIE3N9iHpA9wBdjV93rW33IVJiSTp20qT5Pjd")
(define pp:testsig "Ahtmaqpduek-SZn7-BlOIE3N9iHpA9wBdjV93rW33IVJiSTp20qT5Pjd")
(module-export! '{pp:live pp:appid
		  pp:business pp:user pp:pass pp:sig
		  pp:testbusiness pp:testuser pp:testpass pp:testsig})

(define ppid #f)
(define default-options #[])
;;(define default-button "https://www.paypal.com/en_US/i/btn/btn_buynow_LG.gif")
;;(define default-button "https://www.paypal.com/en_US/i/btn/btn_buynow_SM.gif")
(define pp:button 
  "https://www.paypalobjects.com/webstatic/en_US/btn/btn_buynow_pp_142x27.png")
(varconfig! pp:live pp:live)
(varconfig! pp:business pp:business)
(varconfig! pp:testbusiness pp:testbusiness)
(varconfig! pp:id ppid)
(varconfig! pp:options default-options)
(varconfig! pp:button pp:button)

(varconfig! pp:appid pp:appid)

(varconfig! pp:user pp:user)
(varconfig! pp:pass pp:pass)
(varconfig! pp:sig pp:sig)

(varconfig! pp:testuser pp:testuser)
(varconfig! pp:testpass pp:testpass)
(varconfig! pp:testsig pp:testsig)

(define paypal/return-url "https://www.example.com/completed")
(varconfig! pp:return paypal/return-url)
(define paypal/cancel-url "https://www.example.com/cancelled")
(varconfig! pp:cancel paypal/cancel-url)

(define pp:ccbutton
  "https://www.paypalobjects.com/webstatic/en_US/btn/btn_buynow_cc_171x47.png")

(define (paypal/amount amount)
  (if (number? amount)
      (inexact->string amount 2)
      amount))

(define (paypal/uuid uuid)
  (if (uuid? uuid) (uuid->string uuid)
      (if (string? uuid) uuid
	  (stringout uuid))))

(define (paypal/fields (options default-options))
  (unless (getopt options 'paykey #f)
    (input TYPE "HIDDEN" NAME "cmd" VALUE (getopt options 'cmd "_xclick"))
    (if (getopt options 'invoice)
	(input TYPE "HIDDEN" NAME "invoice" VALUE (getopt options 'invoice))
	(input TYPE "HIDDEN" NAME "invoice" VALUE (uuid->string (getuuid))))
    (when (getopt options 'amount)
      (input TYPE "HIDDEN" NAME "amount"
	     VALUE (paypal/amount (getopt options 'amount))))
    (input TYPE "HIDDEN" NAME "business"
	   VALUE (getopt options 'business
			 (if pp:live pp:business pp:testbusiness)))
    (when (getopt options 'item_number)
      (input TYPE "HIDDEN" NAME "item_number"
	     VALUE (getopt options 'item_number)))
    (input TYPE "HIDDEN" NAME "charset"
	   VALUE (getopt options 'charset "utf-8"))
    (when (getopt options 'return)
      (input TYPE "HIDDEN" NAME "return"
	     VALUE (getopt options 'return)))
    (when (getopt options 'item_name)
      (input TYPE "HIDDEN" NAME "item_name" VALUE (getopt options 'item_name)))
    (when (getopt options 'notify_url)
      (input TYPE "HIDDEN" NAME "notify_url" VALUE
	     (getopt options 'notify (getopt options 'notify_url))))
    (when (getopt options 'cancel)
      (input TYPE "HIDDEN" NAME "cancel_return" VALUE (getopt options 'cancel)))
    (when (getopt options 'no_note)
      (input TYPE "HIDDEN" NAME "no_note" VALUE (getopt options 'no_note)))
    (when (getopt options 'no_shipping)
      (input TYPE "HIDDEN" NAME "no_shipping"
	     VALUE (getopt options 'no_shipping)))
    (when (getopt options 'cbt)
      (input TYPE "HIDDEN" NAME "cbt" VALUE (getopt options 'cbt)))
    (when (getopt options 'page_style)
      (input TYPE "HIDDEN" NAME "page_style"
	     VALUE (getopt options 'page_style)))
    (when (getopt options 'image_url)
      (input TYPE "HIDDEN" NAME "image_url"
	     VALUE (getopt options 'image_url)))
    (when (getopt options 'cpp_header_image)
      (input TYPE "HIDDEN" NAME "cpp_header_image"
	     VALUE (getopt options 'cpp_header_image))))
  (when (getopt options 'paykey #f)
    (input TYPE "HIDDEN" NAME "payKey"
	   VALUE (getopt options 'paykey #f))))

(define (buttonout text)
  (if (has-prefix text "https://www.paypal.com/")
      (input TYPE "IMAGE" name "submit" border 0
	     src text alt "Use PayPal")
      (xhtml text)))

(define paypal/form
  (macro expr
    `(let* ((pp:options ,(second expr))
	    (pp:needbutton #t)
	    (pp:buttonopt (getopt pp:options 'button (config 'pp:button)))
	    (pp:button (lambda ()
			 (set! pp:needbutton #f)
			 (cond ((string? pp:buttonopt)
				(,buttonout pp:buttonopt))
			       ((applicable? pp:buttonopt) (pp:buttonopt))
			       ((and (table? pp:buttonopt) (test pp:buttonopt '%xmltag))
				(xmleval pp:buttonopt))
			       (else (,buttonout ,pp:button))))))
       (,xmlblock "FORM"
	   ((action (getopt pp:options 'action
			    (if (getopt pp:options 'live pp:live)
				"https://www.paypal.com/webscr"
				"https://www.sandbox.paypal.com/webscr")))
	    (target (ifexists (getopt pp:options 'target {})))
	    (method (getopt pp:options 'method "POST")))
	 (,paypal/fields pp:options)
	 ,@(cddr expr)
	 (when pp:needbutton (pp:button))))))

(module-export! '{paypal/form paypal/fields paypal/amount paypal/uuid
		  paypal/opts paypal/return-url paypal/cancel-url})
(module-export! '{paypal/form paypal/fields paypal/amount paypal/uuid
		  paypal/opts paypal/return-url paypal/cancel-url
		  pp:button})
