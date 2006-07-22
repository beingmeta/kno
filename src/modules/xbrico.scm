(in-module 'xbrico)

(module-export!
 '{xbrico-pool
   xbrico-index
   names-pool
   places-pool})

(define xbricosource #f)

(define xbrico-pool #f)
(define names-pool #f)
(define places-pool #f)

(define xbrico-index #f)
(define names-index #f)
(define places-index #f)

(define xbricosource-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) xbricosource)
	  ((equal? val xbricosource)
	   xbricosource)
	  ((exists? xbrico-pool)
	   (message "Redundant configuration of XBRICOSOURCE "
		    "which is already provided from " xbricosource)
	   #f)
	  (else
	   (unless (exists? (name->pool "xbrico.beingmeta.com"))
	     (set! xbricosource val)
	     (use-pool val))
	   (set! xbrico-index (use-index val))
	   (set! xbrico-pool (name->pool "xbrico.beingmeta.com"))
	   (set! names-pool (name->pool "namedb.beingmeta.com"))
	   (set! places-pool (name->pool "placedb.beingmeta.com"))))))
(config-def! 'xbricosource xbricosource-config)

