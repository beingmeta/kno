;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'delicious)

;;; Provides access to del.icio.us through both their API and RSS urls

(define version "$Id:$")

(use-module '{texttools fdweb meltcache})
;(use-module '{gnosys/webapp/userinfo gnosys/urldb gnosys/metakeys/tagmaps})

(module-export!
   '{delicious/geturls delicious/getbookmarks delicious/gettags
		       delicious/istag})

(define trace-delicious #f)
(define trace-delicious-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) trace-delicious)
	  (else (set! trace-delicious val)))))
(config-def! 'tracedelicious trace-delicious-config)

(define xgeturl
  (ambda (urls flags (handle #f))
    (for-choices (url urls)
      (when trace-delicious (message "delicious: " url))
      (if handle
	  (urlxml handle url (qc flags))
	  (urlxml url (qc flags))))))

;; This is configurable and can be an index
(define delicious-cache (make-hashtable))

(define delicious-cache-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) delicious-cache)
	  ((equal? delicious-cache val))
	  ((string? val)
	   (let ((ix (open-index val)))
	     (unless (eq? delicious-cache ix)
	       (message "Setting Delicious cache to " ix)
	       (set! delicious-cache ix))))
	  ((not val)
	   (message "Resetting Delicious cache")
	   (set! delicious-cache (make-hashtable)))
	  ((table? val)
	   (message "Setting Delicious cache to " val)
	   (set! delicious-cache val))
	  (else (error 'typeerror 'delicious-cache-config
		       "Not a valid cache value" val)))))
(config-def! 'deliciouscache delicious-cache-config)

(define urlfns-table (make-hashtable))

(define makeurlfn
  (slambda (user password)
    (try (get urlfns-table (cons user password))
	 (let* ((handle (curlopen 'authinfo (stringout user ":" password)))
		(fn (slambda (url) (xgeturl url 'data handle))))
	   (store! urlfns-table (cons user password) fn)
	   fn))))

;;; Find tags for a particular URL

(define (rssurlentries url)
  (let* ((url2 (scripturl "http://del.icio.us/rss/url/" "url" url))
	 (result (urlget url2))
	 (redirect (get result 'location)))
    (if (exists? redirect)
	(let ((result (xgeturl redirect 'data)))
	  (get (xmlget result 'rdf) 'item))
	(let ((result (xmlparse (get result 'content) 'data)))
	  (get (xmlget result 'rdf) 'item)))))

(set-meltcache-threshold! rssurlentries (* 40 60))

(define (rssgettags url user)
  (let ((entries (meltcache/accumulate delicious-cache rssurlentries url)))
    (for-choices (entry (if user (pick entries 'creator user) entries))
      (elts (segment (get entry 'subject) " ")))))

(define (apigettags url user password)
  (let* ((handle (makeurlfn user password)))
    (let* ((url (scripturl "http://del.icio.us/api/posts/get" "url" url))
	   (result (handle url)))
      (segment (get (get (get result 'posts) 'post) 'subject) " "))))

(define (gettags url user)
  (cond ((pair? user)
	 (meltcache/get delicious-cache apigettags url (car user) (cdr user)))
	((string? user) (rssgettags url user))
	((and (oid? user) (test user 'deliciousid) (test user 'deliciouspassword))
	 (meltcache/get delicious-cache apigettags url (car user) (cdr user)))
	((and (oid? user) (test user 'deliciousid))
	 (meltcache/accumulate delicious-cache
			       rssgettags url (get user 'deliciousid)))
	(else (meltcache/accumulate delicious-cache rssgettags url #f))))
(define (probetags url user)
  (cond ((pair? user)
	 (meltcache/probe delicious-cache
			  apigettags url (car user) (cdr user)))
	((string? user) (rssgettags url user))
	((and (oid? user) (test user 'deliciousid) (test user 'deliciouspassword))
	 (meltcache/probe delicious-cache
			  apigettags url (car user) (cdr user)))
	((and (oid? user) (test user 'deliciousid))
	 (rssgettags url (get user 'deliciousid)))
	(else (rssgettags url #f))))

(define (delicious/gettags url (users #f))
  (if (and (exists? users) users)
      (for-choices (user users) (gettags url user))
      (gettags url #f)))
(define (delicious/probetags url (users #f))
  (if (and (exists? users) users)
      (for-choices (user users) (probetags url user))
      (probetags url #f)))

;;; Find URLs with particular tags
;;;  This selectively uses the API or RSS depending on what is available

(define (rssgeturls tags userid)
  (let* ((url (if userid
		  (stringout "http://del.icio.us/rss/"
		    userid "/"
		    (do-choices (tag tags i)
		      (printout (if (> i 0) "+") (remove #\Space tag))))
		  (stringout "http://del.icio.us/rss/tag/" 
		    (do-choices (tag tags i)
		      (printout (if (> i 0) "+") (remove #\Space tag))))))
	 (result (xgeturl url 'data)))
    (get (get (xmlget result 'rdf) 'item) 'link)))

(set-meltcache-threshold! rssgeturls (* 40 60))

(define (apigeturls tags user password)
  (let* ((handle (makeurlfn user password)))
    (if (singleton? tags)
	(let* ((url (scripturl "http://del.icio.us/api/posts/get"
			       "tag" (qc (remove #\Space tags))))
	       (result (handle url)))
	  (get (get (get result 'posts) 'post) 'href))
	(let* ((taglist (choice->list (remove #\Space tags)))
	       (first-results (apigeturls (car taglist) user password)))
	  (do ((taglist (cdr taglist) (cdr taglist))
	       (results first-results))
	      ((or (null? taglist) (fail? results))
	       results)
	    (set! results
		  (intersection (apigeturls (car taglist) user password)
				results)))))))

(define (geturls tags user)
  (cond ((pair? user)
	 (meltcache/get delicious-cache
			apigeturls (qc tags) (car user) (cdr user)))
	((string? user)
	 (meltcache/accumulate delicious-cache rssgeturls (qc tags) user))
	((and (oid? user) (test user 'deliciousid) (test user 'deliciouspassword))
	 (meltcache/get delicious-cache
			apigeturls (qc tags) (car user) (cdr user)))
	((and (oid? user) (test user 'deliciousid))
	 (meltcache/accumulate delicious-cache
			       rssgeturls (qc tags) (get user 'deliciousid)))
	(else (meltcache/accumulate delicious-cache
				    rssgeturls (qc tags) #f))))

(define (probeurls tags user)
  (cond ((pair? user)
	 (meltcache/probe delicious-cache
			  apigeturls (qc tags) (car user) (cdr user)))
	((string? user)
	 (meltcache/probe delicious-cache rssgeturls (qc tags) user))
	((and (oid? user) (test user 'deliciousid) (test user 'deliciouspassword))
	 (meltcache/probe delicious-cache
			  apigeturls (qc tags) (car user) (cdr user)))
	((and (oid? user) (test user 'deliciousid))
	 (meltcache/probe delicious-cache
			  rssgeturls (qc tags) (get user 'deliciousid)))
	(else (meltcache/probe delicious-cache
			       rssgeturls (qc tags) #f))))

(define (delicious/geturls tags (users #f))
  (if (and (exists? users) users)
      (for-choices (user users) (geturls (qc tags) user))
      (geturls (qc tags) #f)))
(define (delicious/probeurls tag (user #f))
  (if (and (exists? users) users)
      (for-choices (user users) (probeurls (qc tags) user))
      (probeurls (qc tags) #f)))

;;; Get the whole bookmark rather than just the URL

(define (apibookmark result)
  (let* ((url (get result 'href))
	 (tags (elts (segment (get result 'tag) " "))))
    (frame-create #f
      'about url 'title (get result 'description)
      'provider 'delicious 'providerid 'deliciousid
      'user (get result 'user)
      'description (get result 'extended)
      'keywords tags)))

(define (rssbookmark result)
  (let* ((url (get result 'link))
	 (tags (elts (segment (get result 'subject) " "))))
    (frame-create #f
      'about url 'title (get result 'title)
      'provider 'delicious 'providerid 'deliciousid
      'user (get result 'creator)
      'description (get result 'description)
      'keywords tags)))

(define (rssgetbookmarks tags userid)
  (let* ((url (if userid
		  (stringout "http://del.icio.us/rss/"
		    userid "/"
		    (do-choices (tag tags i)
		      (printout (if (> i 0) "+") (remove #\Space tag))))
		  (stringout "http://del.icio.us/rss/tag/" 
		    (do-choices (tag tags i)
		      (printout (if (> i 0) "+") (remove #\Space tag))))))
	 (result (xgeturl url 'data)))
    (rssbookmark (get (get result 'rdf) 'item))))
(set-meltcache-threshold! rssgetbookmarks (* 40 60))

(define (apigetbookmarks tags user password)
  (let* ((handle (makeurlfn user password)))
    (if (singleton? tags)
	(let* ((url (scripturl "http://del.icio.us/api/posts/get"
			       "tag" (remove #\Space tags)))
	       (result (handle url)))
	  (apibookmark (get (get result 'posts) 'post)))
	(let* ((taglist (choice->list (remove #\Space tags)))
	       (first-results (apigetbookmarks (car taglist) user password)))
	  (do ((taglist (cdr taglist) (cdr taglist))
	       (results first-results))
	      ((or (null? taglist) (fail? results))
	       results)
	    (set! results
		  (intersection (apigetbookmarks (car taglist) user password)
				results)))))))

(define (getbookmarks tags user)
  (cond ((empty? tags) {})
	((pair? user)
	 (meltcache/get delicious-cache
			apigetbookmarks (qc tags) (car user) (cdr user)))
	((string? user)
	 (meltcache/accumulate delicious-cache
			       rssgetbookmarks (qc tags) user))
	((and (oid? user) (test user 'deliciousid)
	      (test user 'deliciouspassword))
	 (meltcache/get delicious-cache
			apigetbookmarks (qc tags)
			(get user 'deliciousid)
			(get user 'deliciouspassword)))
	((and (oid? user) (test user 'deliciousid))
	 (meltcache/accumulate
	  delicious-cache rssgetbookmarks (qc tags) (get user 'deliciousid)))
	(else (meltcache/accumulate
	       delicious-cache rssgetbookmarks (qc tags) #f))))

(define (delicious/getbookmarks tags users)
  (getbookmarks (qc tags) users))

(define (delicious/istag tag)
  (exists?  (meltcache/get delicious-cache rssgeturls tag #f)))


;;; The lookup method

(define lookupmethod
  (ambda (tags user)
    (delicious/getbookmarks (qc tags) (qc user))))
