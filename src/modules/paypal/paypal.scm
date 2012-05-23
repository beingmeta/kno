;;; -*- Mode: Scheme; -*-

(in-module 'paypal)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools ezrecords parsetime varconfig})

(define pp:live #f)
(define pp:business "anonymous@company.com")
(define pp:appid "APP-80W284485P519543T")
(define pp:user "store_1288290819_biz_api1.beingmeta.com")
(define pp:pass "1288290827")
(define pp:sig "Ahtmaqpduek-SZn7-BlOIE3N9iHpA9wBdjV93rW33IVJiSTp20qT5Pjd")
(module-export! '{pp:live pp:appid pp:business pp:user pp:pass pp:sig })

(define ppid #f)
(define default-options #[])
(define default-button "https://www.paypal.com/en_US/i/btn/btn_buynow_LG.gif")

(varconfig! pp:live pp:live)
(varconfig! pp:business pp:business)
(varconfig! pp:id ppid)
(varconfig! pp:options default-options)
(varconfig! pp:button default-button)

(varconfig! pp:appid pp:appid)
(varconfig! pp:user pp:user)
(varconfig! pp:pass pp:pass)
(varconfig! pp:sig pp:sig)

(define (format-amount amount)
  (if (number? amount)
      (inexact->string amount 2)
      amount))

(define (paypal/fields (options default-options))
  (input TYPE "HIDDEN" NAME "cmd" VALUE (getopt options 'cmd "_xclick"))
  (if (getopt options 'invoice)
      (input TYPE "HIDDEN" NAME "invoice" VALUE (getopt options 'invoice))
      (input TYPE "HIDDEN" NAME "invoice" VALUE (uuid->string (getuuid))))
  (when (getopt options 'amount)
    (input TYPE "HIDDEN" NAME "amount" VALUE (format-amount (getopt options 'amount))))
  (input TYPE "HIDDEN" NAME "business"
	 VALUE (getopt options 'business pp:business))
  (when (getopt options 'item_number)
    (input TYPE "HIDDEN" NAME "item_number"
	   VALUE (getopt options 'item_number)))
  (input TYPE "HIDDEN" NAME "charset" VALUE (getopt options 'charset "utf-8"))
  (when (getopt options 'return)
    (input TYPE "HIDDEN" NAME "return" VALUE (getopt options 'return)))
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
    (input TYPE "HIDDEN" NAME "no_shipping" VALUE (getopt options 'no_shipping)))
  (when (getopt options 'cbt)
    (input TYPE "HIDDEN" NAME "cbt" VALUE (getopt options 'cbt)))
  (when (getopt options 'page_style)
    (input TYPE "HIDDEN" NAME "page_style" VALUE (getopt options 'page_style)))
  (when (getopt options 'image_url)
    (input TYPE "HIDDEN" NAME "image_url" VALUE (getopt options 'image_url)))
  (when (getopt options 'cpp_header_image)
    (input TYPE "HIDDEN" NAME "cpp_header_image"
	   VALUE (getopt options 'cpp_header_image))))

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
			       (else (,buttonout ,default-button))))))
       (,xmlblock "FORM"
	   ((action (getopt pp:options 'action
			    (if (getopt pp:options 'live pp:live)
				"https://www.paypal.com/cgi-bin/webscr"
				"https://www.sandbox.paypal.com/cgi-bin/webscr")))
	    (target (ifexists (getopt pp:options 'target {})))
	    (method "POST"))
	 (,paypal/fields pp:options)
	 ,@(cddr expr)
	 (when pp:needbutton (pp:button))))))

(module-export! '{paypal/form paypal/fields paypal/opts})
