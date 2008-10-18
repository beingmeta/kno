(in-module 'facebook/fbcall)

(use-module '{fdweb xhtml texttools facebook})

(module-export! '{fbcall fbcall/open fbcalluri})

(define (fbcalluri method args (w/session #t) (w/callid #t))
  (when w/callid
    (set! args (cons* "call_id" (get (timestamp) 'tick) args)))
  (set! args (cons* "api_key" apikey "v" "1.0" "method" method
		    args))
  (when (and w/session (cgitest 'fb_sig_session_key)
	     (exists? (cgiget 'fb_sig_session_key))
	     (string? (cgiget 'fb_sig_session_key)))
    (set! args (cons* "session_key" (cgiget 'fb_sig_session_key) args)))
  (stringout "http://api.facebook.com/restserver.php?"
    (do ((args args (cddr args)))
	((null? args) (printout))
      (printout (car args) "=" (cadr args) "&"))
    (printout "sig=" (downcase (packet->base16 (get-signature args))))))

(define (fbcall method . args)
  (jsonparse
   (get (urlget (fbcalluri method (cons* "format" "JSON" args)))
	'%content)))
;; This calls a Facebook method without a session_id or call_id
(define (fbcall/open method . args)
  (jsonparse
   (get (urlget (fbcalluri method (cons* "format" "JSON" args) #f #f))
	'%content)))

(define (get-signature args)
  (let ((table (frame-create #f)))
    (do ((scan args (cddr scan)))
	((null? scan))
      (add! table (car scan) (cadr scan)))
    (md5 (stringout (doseq (key (lexsorted (getkeys table)))
		      (printout key "=" (get table key)))
	   (stringout apisecretkey)))))

;;; Custom methods

(module-export!
 '{fb/getuserinfo
   fb/getmyid fb/getmyfriends fb/getmygroups fb/getmypages})

(define (fb/getmyid)
  (string->number (fbcall "users.getLoggedInUser")))

(defambda (fb/getuserinfo users (fields "name"))
  (let ((info (fbcall "users.getInfo" "uids"
		      (stringout (do-choices (u users i)
				   (printout (if (> i 0) ",") u)))
		      "fields"
		      (stringout (do-choices (field fields i)
				   (printout (if (> i 0) ",") field))))))
    (for-choices (friendinfo (elts info))
      (let ((f (frame-create #f 'fb/uid (get friendinfo "uid"))))
	(do-choices (field fields)
	  (add! f (string->lisp field) (get friendinfo field)))
	f))))

(define (fb/getmyfriends) (fbcall "facebook.friends.get"))
(define (fb/getmygroups) (fbcall "groups.get"))
(define (fb/getmypages)
  (fbcall "pages.getInfo"
	  "fields"
	  "page_id,name,website,genre,pic,pic_small,pic_big,pic_square,pic_large"))