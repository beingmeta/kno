(in-module 'facebook/fbcall)

(use-module '{fdweb xhtml texttools facebook})

(module-export! '{fbcall fbcalluri})

(define (fbcalluri method args)
  (set! args (cons* "api_key" apikey "call_id" (get (timestamp) 'tick)
		    "v" "1.0" "method" method
		    args))
  (if (cgitest 'fb_sig_session_key)
      (set! args (cons* "session_key" (cgiget 'fb_sig_session_key) args))
      (if (cgitest 'fbsession)
	  (set! args (cons* "session_key" (cgiget 'fbsession) args))))
  (stringout "http://api.facebook.com/restserver.php?"
    (do ((args args (cddr args)))
	((null? args) (printout))
      (printout (car args) "=" (cadr args) "&"))
    (printout "sig=" (downcase (packet->base16 (get-signature args))))))

(define (fbcall method . args)
  (jsonparse
   (get (urlget (fbcalluri method (cons* "format" "json" args)))
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