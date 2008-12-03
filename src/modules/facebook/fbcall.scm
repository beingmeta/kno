(in-module 'facebook/fbcall)

(define version "$Id$")

(use-module '{fdweb xhtml texttools facebook logger})

(module-export! '{fbcall fbcall/raw fbcall/open fbcall/vec})

(define %loglevel %notice!)

;;; Main functions

(define (fbcall method . args)
  (%debug "Calling " method " on"
	  (doseq (arg args) (printout " " (write arg))))
  (let* ((req (urlget (fbcalluri method (cons* "format" "JSON" args))))
	 (content (get req '%content))
	 (result (fbcallparse content)))
    (%debug "Content for " method " is " (write content))
    (%debug "Results for " method " is " (write result))
    result))
;; This returns the direct result of URLGET
(define (fbcall/raw method . args)
  (%debug "Calling " method " on"
	  (doseq (arg args) (printout " " (write arg))))
  (urlget (fbcalluri method (cons* "format" "JSON" args))))

;; This calls a Facebook method without a session_id or call_id
(define (fbcall/open method . args)
  (jsonparse
   (get (urlget (fbcalluri method (cons* "format" "JSON" args) #f #f))
	'%content)))

;;; Support functions

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

(define (fbcallparse content (err #t))
  ;; This handles the various kinds of responses the Facebook API may
  ;;  generate.  Note that we always request JSON, but sometimes we
  ;;  get XML and sometimes we get either XML or JSON without a
  ;;  content type.  We have to handle all of those cases as well as
  ;;  converting error results to Scheme errors.
  (if (or (equal? content "") (equal? content #""))
      ;; If we get this, something is quite wrong, usually with the
      ;; underlying connection and not Facebook itself.
      (error "Invalid FBCALL results" content)
      (if (or (string? content) (packet? content))
	  (if (if (string? content)
		  (eqv? (elt content 0) #\<)
		  (eqv? (elt content 0) 60))
	      (error "FBCALL/JSON returned XML" content)
	      (if err
		  (let ((parsed (jsonparse content)))
		    (if (and (table? parsed) (test parsed "error_code"))
			(error "FBCALL API Error" parsed)
			parsed))
		  (jsonparse content)))
	  (error "Invalid FBCALL result content" content))))

(define (get-signature args)
  (let ((table (frame-create #f)))
    (do ((scan args (cddr scan)))
	((null? scan))
      (add! table (car scan) (cadr scan)))
    (md5 (stringout (doseq (key (lexsorted (getkeys table)))
		      (printout key "=" (get table key)))
	   (stringout apisecretkey)))))

;;; Custom methods

;;; Sometimes Facebook API calls which normally return vectors of
;;;  results return an empty table/map when there are no results.
;;;  This handles that case and also the (possible) case where a
;;;  single non-empty table/map is returned.
(define (fbcall/vec method . args)
  "Calls the Facebook API and ensures that the results are a vector"
  (%debug "Calling " method " on"
	  (doseq (arg args) (printout " " (write arg))))
  (let* ((req (urlget (fbcalluri method (cons* "format" "JSON" args))))
	 (content (get req '%content))
	 (result (fbcallparse content)))
    (%debug "Content for " method " is " (write content))
    (%debug "Results for " method " is " (write result))
    (if (vector? result) result
	(if (and (table? result) (zero? (getkeys result)))
	    #()
	    (vector result)))))

(module-export!
 '{fb/getuserinfo
   fb/getmyid fb/getmyfriends fb/getmygroups fb/getmypages})

(define (fb/getmyid)
  (string->number (fbcall "users.getLoggedInUser")))

(define default-fields
  {"name" "pic" "pic_small" "pic_big" "pic_square" "status" "uid"
   "has_added_app"})

(defambda (fb/getuserinfo users (fields default-fields))
  (let ((info (fbcall/vec "users.getInfo" "uids"
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

(define (fb/getmyfriends) (fbcall/vec "facebook.friends.get"))
(define (fb/getmygroups) (fbcall/vec "groups.get"))
(define (fb/getmypages)
  (fbcall/vec "pages.getInfo"
	      "fields"
	      "page_id,name,website,genre,pic,pic_small,pic_big,pic_square,pic_large"))


