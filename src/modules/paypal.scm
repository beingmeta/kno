;;; -*- Mode: Scheme; -*-

(in-module 'paypal)

;;; Connects with Paypal

(use-module '{fdweb xhtml texttools ezrecords parsetime varconfig})

(define live #f)
(define business "anonymous@company.com")

(varconfig! 'pp:live live)
(varconfig! 'pp:business business)

(define (paypalform cmd invoice amount (options #[]))
  (form ((action="https://www.paypal.com/cgi-bin/webscr") (method "POST"))
    (input TYPE "HIDDEN" NAME "cmd" VALUE cmd)
    (input TYPE "HIDDEN" NAME "invoice" VALUE invoice)
    (input TYPE "HIDDEN" NAME "amount" VALUE amount)
    (input TYPE "HIDDEN" NAME "business" VALUE (getopt options 'business business))
    (when (getopt options 'item_number)
      (input TYPE "HIDDEN" NAME "item_number" VALUE (getopt options 'item_number)))
    (input TYPE "HIDDEN" NAME "charset" VALUE (getopt options 'charset "utf-8"))
    (when (getopt options 'return)
      (input TYPE "HIDDEN" NAME "return" VALUE (getopt options 'return)))
    (when (getopt options 'cancel)
      (input TYPE "HIDDEN" NAME "cancel_return" VALUE (getopt options 'cancel)))))