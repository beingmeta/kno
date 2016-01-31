;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'facebook/fbcall)

;;; Calling the Facebook API and various standard calls

(use-module '{fdweb xhtml texttools facebook logger})

(define-init %loglevel %notice%)

(module-export!
 '{fbcall
   fbcalluri fbcalluri*
   fbcall/raw fbcall/open fbcall/vec fbcall/open/vec fbcall/xml
   fb/false? fb/true?})

(define (getsecret sessionid)
  (get (get fb/sessions sessionid) "secret"))

(define (fb/false? arg)
  (or (not arg)
      (zero? arg)
      (and (string? arg) (or (equal? arg "0") (equal? arg "false")))))
(define (fb/true? arg) (not (fb/false? arg)))

;;; Main functions

(define (fbcall method . args)
  (%debug "Calling " method " on"
	  (doseq (arg args) (printout " " (write arg))))
  (let* ((uri (fbcalluri method (cons* "format" "JSON" args)))
	 (ign1 (begin (%debug "Request URI is " uri) #t))
	 (req (urlget uri))
	 (content (get req '%content))
	 (ign2 (begin (%debug "Request content is " content) #t))
	 (result (fbcallparse content)))
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

(define (fbcall/xml method . args)
  (get (urlget (fbcalluri method (cons* "format" "XML" args)))
       '%content))

;;; Support functions

(define (fbcalluri method args (w/session #t) (w/callid #t) (sessionkey (cgiget 'fb_sig_session_key)))
  ;; (when (and w/session (not (cgitest 'fb_sig_session_key)) (cgitest 'fbinfo)) (fb/useinfo))
  (when w/callid (set! args (cons* "call_id" (time) args)))
  (set! args (cons* "api_key" apikey "v" "1.0" "method" method
		    args))
  ;; (debug%watch "FBCALLURI" method w/session (cgiget 'fb_sig_session_key))
  (when (and w/session sessionkey (string? sessionkey))
    (set! args (cons* "session_key" sessionkey args)))
  (stringout "http://api.facebook.com/restserver.php?"
    (do ((args args (cddr args)))
	((null? args) (printout))
      (if (string? (cadr args))
	  (printout (car args) "=" (uriencode (cadr args)) "&")
	  (printout (car args) "="
		    (uriencode (lisp->string (cadr args))) "&")))
    (printout "sig="
	      (downcase (packet->base16
			 (get-signature args
					(if w/session
					    (try (cgiget 'fbsecret)
						 (getsecret sessionkey)
						 "toomanysecrets")
					    apisecretkey)))))))
(define (fbcalluri* method . args) (fbcalluri method args))

(define (encode-args args)
  (if (null? args) '()
      (cons* (car args) (uriencode (cadr args))
	     (encode-args (cddr args)))))

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

(define (get-signature args (secretkey apisecretkey))
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
  (let* ((uri (fbcalluri method (cons* "format" "JSON" args)))
	 (ign1 (begin (%debug "Request URI is " uri) #t))
	 (req (urlget uri))
	 (content (get req '%content))
	 (ign2 (begin (%debug "Request content is " content) #t))
	 (result (fbcallparse content)))
    (%debug "Results for " method " is " (write result))
    (if (vector? result) result
	(if (and (table? result) (zero? (getkeys result)))
	    #()
	    (vector result)))))
(define (fbcall/open/vec method . args)
  "Calls the Facebook API and ensures that the results are a vector"
  (%debug "Calling " method " on"
	  (doseq (arg args) (printout " " (write arg))))
  (let* ((uri (fbcalluri method (cons* "format" "JSON" args) #f #f))
	 (ign1 (begin (%debug "Request URI is " uri) #t))
	 (req (urlget uri))
	 (content (get req '%content))
	 (ign2 (begin (%debug "Request content is " content) #t))
	 (result (fbcallparse content)))
    (%debug "Results for " method " is " (write result))
    (if (vector? result) result
	(if (and (table? result) (zero? (getkeys result)))
	    #()
	    (vector result)))))

(module-export!
 '{fb/getuserinfo
   fb/getmyid fb/getmyfriends fb/getmygroups fb/getmypages})

(define (fb/getmyid)
  (->number (fbcall "users.getLoggedInUser")))

(define default-fields
  {"name" "pic" "pic_small" "pic_big" "pic_square" "status" "uid"
   "has_added_app" "profile_url" "about_me"})

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

(define page-fields
  "page_id,name,website,genre,pic,pic_small,pic_big,pic_square,pic_large,website,bio,band_members,company_overview,plot_outline")
(define (fb/getmypages)
  (fbcall/vec "pages.getInfo" "fields" page-fields))

;;; Getting info for particular groups or pages

(define all-page-fields
  "page_id,name,pic_small,pic_square,pic_big,pic,pic_large,type,website,location,hours,band_members,bio,hometown,genre,record_label,influences,founded,company_overview,mission,products,release_date,starring,written_by,directed_by,produced_by,studio,awards,plot_outline,network,season,schedule")

(define (fb/getpageinfo pageid (fields #f))
  (elts
   (fbcall/vec "pages.getInfo"
	       "page_ids" pageid
	       "fields"
	       (cond ((not fields) page-fields)
		     ((string? fields) fields)
		     ((ambiguous? fields)
		      (stringout (do-choices (field fields i)
				   (printout (if (> i 0) ",") field))))
		     ((sequence? fields)
		      (stringout (doseq (field fields i)
				   (printout (if (> i 0) ",") field))))
		     (else all-page-fields)))))

(define (fb/getgroupinfo gid)
  (elts (fbcall/vec "groups.get" "gids" gid)))

(module-export! '{fb/getgroupinfo fb/getpageinfo})

;; We redefine this while pages.getInfo is broken at Facebook

(define mypages-query-base
  (append "select " page-fields " from page where page_id IN "
	  "(select page_id from page_fan where uid="))

(define (fb/getmypages)
  (fbcall/vec "fql.query"
	      "query"
	      (stringout mypages-query-base (fb/getmyid) ")")))

