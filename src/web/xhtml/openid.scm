;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; This file implements OpenID login for FramerD
;;; Copyright (C) 2005-2017 beingmeta, inc. All rights reserved

(in-module 'xhtml/openid)

(use-module '{texttools fdweb xhtml xhtml/clickit domutils varconfig logger})
(define %used_modules '{varconfig})

(define-init %loglevel %notify%)

(define-init openid-servers (make-hashtable))
(define-init openid/optinfo (make-hashtable))
(define-init default-opts #[])

(define reqopts ;; requestopts
  #["openid.ns.ui" "http://openid.net/extensions/ui/1.0"
    "openid.ns.sreg" "http://openid.net/extensions/sreg/1.1"
    "openid.ns.ax" "http://openid.net/srv/ax/1.0"
    "openid.ui.icon" "true"
    "openid.ax.mode" "fetch_request"
    "openid.sreg.optional" "nickname,email,fullname,country,postcode"
    "openid.ax.type.email" "http://axschema.org/contact/email"
    "openid.ax.type.language" "http://axschema.org/pref/language"
    "openid.ax.type.firstname" "http://axschema.org/namePerson/first"
    "openid.ax.type.lastname" "http://axschema.org/namePerson/last"
    "openid.ax.type.fullname" "http://axschema.org/namePerson"      
    "openid.ax.type.nickname" "http://axschema.org/namePerson/friendly"
    "openid.ax.type.homepage" "http://axschema.org/contact/web/default"
    "openid.ax.type.pic" "http://axschema.org/media/image/default"
    "openid.ax.type.sqpic" "http://axschema.org/media/image/aspect11"
    "openid.ax.required" "fullname,nickname,firstname,lastname,homepage,pic,sqpic"])
(define realm #f)
(define callback #f)

(varconfig! openid:opts default-opts)
(varconfig! openid:callback callback)
(varconfig! openid:realm realm)

(define (openid-url string)
  (if (has-prefix string "//") (set! string (string-append "http://" string))
      (unless (or (has-prefix string "http:") (has-prefix string "https:"))
	(set! string (string-append "http://" string))))
  (let ((parsed (parseuri string)))
    (stringout (try (get parsed 'scheme) "http") "://"
	       (get parsed 'hostname) 
	       (when (test parsed 'path)
		 (dolist (elt (get parsed 'path)) (printout "/" elt))
		 (when (test parsed 'name) (printout "/" (get parsed 'name)))))))

(define (get-openid-server uri)
  (try (get openid-servers uri)
       (let ((server (fetch-openid-server uri)))
	 (when (exists? server) (store! openid-servers uri server))
	 server)))

(define (fetch-openid-server uri)
  (let* ((req (urlget uri #[HEADER "Accept: text/html, application/xrds+xml"]))
	 (content (tryif (<= 200 (get req 'response) 299)
		    (get req '%content)))
	 (xml (tryif (exists? content) (xmlparse content 'sloppy)))
	 (links (dom/find xml "link")))
    (try (cons (try-choices (link links)
		 (tryif (textsearch '{(WORD "openid.server")
				      (WORD "openid2.provider")}
				    (get link 'rel))
		   (get link 'href)))
	       (try (try-choices (link links)
		      (tryif (textsearch '{(WORD "openid.delegate")
					   (WORD "openid2.local_id")}
					 (get link 'rel))
			(get link 'href)))
		    #f))
	 ;; RDS, possibly (should probably check)
	 (cons (xmlcontent (xmlget xml 'uri)) #f)
	 (cons uri uri))))

(define (guess-realm uri)
  (let ((parsed (parseuri uri)))
    (stringout (get parsed 'scheme) "://" (get parsed 'hostname) "/")))

(define (guess-callback) (req/get 'callback (geturl)))

(define (openid-redirect url (opts #f))
  (let* ((server (if (pair? url) url (get-openid-server url)))
	 (opts (or opts (try (get openid/optinfo url)
			     (get openid/optinfo server)
			     default-opts)))
	 (callback (getopt opts 'callback
			   (or callback (guess-callback))))
	 (realm (getopt opts 'realm (or realm (guess-realm callback)))))
    (scripturl+ (car server)
      (try (getopt opts 'reqopts {}) reqopts)
      "openid.mode" "checkid_setup"
      "openid.ns" (getopt opts 'ns "http://specs.openid.net/auth/2.0")
      "openid.identity"
      (if (and (pair? server) (cdr server)) (cdr server)
	  "http://specs.openid.net/auth/2.0/identifier_select")
      "openid.claimed_id"
      (if (and (pair? server) (cdr server)) (cdr server)
	  "http://specs.openid.net/auth/2.0/identifier_select")
      "openid.return_to" callback "openid.realm" realm
      "openid.trust_root" realm)))

(define (dedotit s) (string-subst s "." "_"))
(define (redotit s) (string-subst s "_" "."))
(define (add-openid-prefix s) (string-append "openid." s))

(define (validate openid.identity openid.assoc_handle openid.signed openid.sig)
  (let* ((provider (car (get-openid-server openid.identity)))
	 (postdata `#["openid.mode" "check_authentication"
		      "openid.assoc_handle" ,openid.assoc_handle
		      "openid.signed" ,openid.signed
		      "openid.sig" ,openid.sig])
	 (keys (map add-openid-prefix (segment openid.signed ","))))
    (doseq (key keys)
      (unless (equal? key "openid.mode")
	(store! postdata key (req/get (string->lisp key)))))
    (let ((result (urlpost provider #[] postdata)))
      (and (textsearch #((bol) "is_valid:" (spaces*) "true" (eol))
		       (get result '%content))
	   result))))

(define (doredirect uri)
  (debug%watch "REDIRECT" uri)
  (req/set! 'doctype #f)
  (req/set! 'status 303)
  (httpheader "Location: " uri)
  (h1 "Redirecting to " (anchor uri uri)))

(define (openid/auth (openid.endpoint #f) (openid.mode #f) 
		     (openid.identifier #f) (openid.claimed_identifier #f)
		     (openid.identity #f))
  (debug%watch "OPENID/AUTH" openid.endpoint openid.mode)
  (cond ((equal? openid.mode "id_res")
	 (and (req/call validate) (openid-return)))
	((equal? openid.mode "cancel") #f)
	((or openid.identifier openid.endpoint)
	 (doredirect
	  (debug%watch
	      (openid-redirect (or openid.identifier openid.endpoint))
	    openid.identifier openid.endpoint))
	 #f)
	(else #f)))

(define (openid-return)
  (let ((signed (segment (req/get 'openid.signed) ","))
	(result (frame-create #f)))
    (debug%watch "OPENID-RETURN" signed)
    (dolist (key signed)
      (let* ((osym (string->symbol (append "OPENID." (upcase key))))
	     (sym  (string->symbol (upcase key)))
	     (ssym (tryif (and (not (has-prefix key "ax.type."))
			       (rposition #\. key))
		     (string->symbol
		      (upcase (subseq key (1+ (rposition #\. key)))))))
	     (v (or (req/get osym #f) (req/get sym #f) {})))
	(debug%watch "OPENID-RETURN" key osym sym ssym v)
	(when (exists? v)
	  (store! result osym v)
	  (store! result sym v)
	  (add! result ssym v))))
    (req/set! 'results result)
    result))

(define (openidauthvalidate)
  (tryif (req/test 'openid.mode "id_res")
    (and (req/call validate) (openid-return))))

(config! 'auth:validate (cons 'openid openidauthvalidate))

(define (openid/login site (opts default-opts))
  (doredirect (openid-redirect (cons site #f)))
  (fail))

;;; Lists of providers

(define openid/providers/path
  ;; These have OpenID URLs of the form http://host/username
  {"openid.aol.com" "www.flickr.com" "www.myspace.com" "me.yahoo.com"})
(define openid/providers/domain
  ;; These have OpenID URLs of the form http://username.host/
  {"blogspot.com" "livejournal.com" "wordpress.com" "myopenid.com" "myid.net"
   "pip.verisignlabs.com"})
(define openid/providers
  (choice openid/providers/path openid/providers/domain))

(define (openid/authurl provider username)
  (if (overlaps? provider openid/providers/path)
      (stringout "https://" provider "/" username)
      (if (overlaps? provider openid/providers/domain)
	  (stringout "https://" username "." provider "/")
	  (fail))))

;;; Exports

(module-export!
 '{get-openid-server
   openid-url
   openid/authurl openid/auth openid/optinfo
   openid/login
   openid/providers
   openid/providers/path openid/providers/domain})


