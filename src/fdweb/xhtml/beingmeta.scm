(in-module 'xhtml/beingmeta)

(use-module '{fdweb xhtml})

;;; Various beingmeta related logotypes, etc.

(define (bm/logotype (anchor #f))
  (if anchor
      (anchor* (if (string? anchor) anchor
		   "http://www.beingmeta.com")
	  ((target "_blank"))
	(bm/logotype #f))
      (xmlout 
	"being" (span ((class "bmm"))
		  "m" (span ((class "bme")) "e")
		  (span ((class "bmt")) "t"
			(span ((class "bma")) "a"))))))

(module-export! 'bm/logotype)
