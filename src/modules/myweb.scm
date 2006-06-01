(in-module 'myweb)

(use-module '{texttools fdweb meltcache})

(module-export!
 '{myweb/gettags
   myweb/geturls
   myweb/topurls
   myweb/probetags
   myweb/probeurls
   myweb/getbookmarks
   myweb/topbookmarks
   myweb/tagfreq
   myweb/cotags})

(define trace-myweb #f)
(define trace-myweb-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) trace-myweb)
	  (else (set! trace-myweb val)))))
(config-def! 'tracemyweb trace-myweb-config)

;; This is configurable and can be an index
(define myweb-cache (make-hashtable))

(define myweb-cache-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) myweb-cache)
	  ((equal? myweb-cache val))
	  ((string? val)
	   (let ((ix (open-index val)))
	     (unless (eq? myweb-cache ix)
	       (message "Setting MyWeb cache to " ix)
	       (set! myweb-cache ix))))
	  ((not val)
	   (message "Resetting MyWeb cache")
	   (set! myweb-cache (make-hashtable)))
	  ((table? val)
	   (message "Setting MyWeb cache to " val)
	   (set! myweb-cache val))
	  (else (error 'typeerror 'myweb-cache-config
		       "Not a valid cache value" val)))))
(config-def! 'mywebcache myweb-cache-config)

(define (parse-arg x)
  (if (string? x) (string->lisp x) x))

(define (getmywebid arg)
  (cond ((not arg) #f)
	((string? arg) arg)
	((pair? arg) (car arg))
	((oid? arg) (difference (get arg 'mywebid) ""))
	(else #f)))

(define reqwait 1)

(define gettags-base
  "http://api.search.yahoo.com/MyWebService/V1/tagSearch")
(define geturls-base
  "http://api.search.yahoo.com/MyWebService/V1/urlSearch")
(define cotags-base
  "http://api.search.yahoo.com/MyWebService/V1/relatedTags")

;;;; Getting the tags on a URL

(define (gettags-url url (user #f) (start 1))
  (if user
      (scripturl gettags-base
		 "appid" "beingmeta"
		 "start" start
		 "results" 50
		 "url" url
		 "yahooid" user)
      (scripturl gettags-base
		 "appid" "beingmeta"
		 "start" start
		 "results" 50
		 "url" url)))

(define (gettags url (user #f) (start 1))
  (let* ((requrl (gettags-url url user start))
	 (gotten (begin (when trace-myweb (message "url2tags: " requrl))
			(urlxml requrl 'data)))
	 (resultset (xmlget gotten 'resultset)))
    (if (exists? resultset)
	(choice (get (get resultset 'result) 'tag)
		(let* ((available (parse-arg (get resultset 'totalresultsavailable)))
		       (returned (parse-arg (get resultset 'totalresultsreturned)))
		       (last (+ returned start)))
		  (tryif (> available last)
			 (begin (if reqwait (sleep reqwait))
				(gettags url user (1+ last))))))
	(error mywebfailure gettags requrl gotten))))

;;;; Getting the URLs with a tag

;;; This gets all the results.  To just get the top (most recent) results,
;;;  use toptags2urls or myweb/toptags.

(define (geturls-url tags (user #f) (start 1))
  (if user
      (scripturl geturls-base
		 "appid" "beingmeta"
		 "start" start
		 "results" 50
		 "tag" (qc tags)
		 "yahooid" user)
      (scripturl geturls-base
		 "appid" "beingmeta"
		 "start" start
		 "results" 50
		 "tag" (qc tags))))

(define (geturls tags (user #f) (start 1))
  (let* ((requrl (geturls-url (qc tags) user start))
	 (gotten (begin (when trace-myweb (message "tags2urls: " requrl))
			(urlxml requrl 'data)))
	 (resultset (xmlget gotten 'resultset)))
    (if (exists? resultset)
	(choice (get (get resultset 'result) 'url)
		(let* ((available (parse-arg (get resultset 'totalresultsavailable)))
		       (returned (parse-arg (get resultset 'totalresultsreturned)))
		       (last (+ returned start)))
		  (tryif (> available last)
			 (begin (if reqwait (sleep reqwait))
				(geturls (qc tags) user (1+ last))))))
	(error mywebfailure geturls requrl gotten))))

;;;; Going from tags to tags
;;;;  This gets tags which co-occur with a particular set of tags

(define (cotags-url tags (user #f) (start 1))
  (if user
      (scripturl cotags-base
		 "appid" "beingmeta"
		 "start" start
		 "results" 50
		 "tag" (qc tags)
		 "yahooid" user)
      (scripturl cotags-base
		 "appid" "beingmeta"
		 "start" start
		 "results" 50
		 "tag" (qc tags))))

(define (convert-cotag-result result)
  (cons (get result 'tag) (parse-arg (get result 'frequency))))

(define (cotags tags (user #f) (start 1))
  (let* ((requrl (cotags-url (qc tags) user start))
	 (gotten (begin (when trace-myweb (message "cotags: " requrl))
			(urlxml requrl 'data)))
	 (resultset (xmlget gotten 'resultset)))
    (if (exists? resultset)
	(choice (convert-cotag-result (get resultset 'result))
		(let* ((available (parse-arg (get resultset
						  'totalresultsavailable)))
		       (returned (parse-arg (get resultset 'totalresultsreturned)))
		       (last (+ returned start)))
		  (tryif (> available last)
			 (begin (if reqwait (sleep reqwait))
				(cotags (qc tags) user (1+ last))))))
	(error mywebfailure cotags requrl gotten))))

;;;; Tags to urls which just returns the top 50
;;;    (This is often preferable because we wait between requests to avoid being
;;;     throttled by Yahoo!)

(define (gettopurls tags (user #f))
  (let* ((requrl (geturls-url (qc tags) user 1))
	 (gotten (begin (when trace-myweb (message "gettopurls: " requrl))
			(urlxml requrl 'data)))
	 (resultset (xmlget gotten 'resultset)))
    (if (exists? resultset)
	(get (get resultset 'result) 'url)
	(error mywebfailure gettopurls requrl gotten))))

;;;; Basic URL fetching functions

(define (myweb/gettags url (user #f))
  (meltcache/get myweb-cache gettags url (getmywebid user)))
(define (myweb/geturls tags (user #f))
  (meltcache/get myweb-cache geturls (qc tags) (getmywebid user)))
(define (myweb/topurls tags (user #f))
  (meltcache/get myweb-cache geturlstop (qc tags) (getmywebid user)))

(define (myweb/probetags url (user #f))
  (meltcache/probe myweb-cache gettags url (getmywebid user)))
(define (myweb/probeurls tag (user #f))
  (meltcache/probe myweb-cache geturls tag (getmywebid user)))

(define (myweb/cotags tags (user #f))
  (meltcache/get myweb-cache cotags (qc tags) (getmywebid user)))

;;;; Getting tag frequency

(define (gettagfreq tag)
  (let* ((url (scripturl geturls-url-base
			 "appid" "beingmeta"
			 "start" 1
			 "results" 2
			 "tag" tag))
	 (data (begin (when trace-myweb (message "gettagfreq: " requrl))
		      (urlxml url 'data)))
	 (resultset (xmlget data 'resultset)))
    (parse-arg (get resultset 'totalresultsavailable))))

(define (myweb/tagfreq tag)
  (meltcache/get myweb-cache gettagfreq tag))

;;;; Functions which return slotmaps and urlframes describing bookmarks

(define (convert2bookmark result)
  (let* ((url (get result 'url))
	 (tags (get (get result 'tags) 'tag))
	 (source (get result 'user)))
    (frame-create #f
      'about url 'title (get result 'title)
      'provider 'myweb 'providerid 'mywebid
      'user (get result 'user)
      'description (get result 'note)
      'keywords tags)))

(define (getbookmarks tags (user #f) (start 1))
  (let* ((requrl (geturls-url (qc tags) user start))
	 (gotten (begin (when trace-myweb (message "getbookmarks: " requrl))
			(urlxml requrl 'data)))
	 (resultset (xmlget gotten 'resultset)))
    (if (exists? resultset)
	(choice (convert2bookmark (get resultset 'result))
		(let* ((available
			(parse-arg (get resultset'totalresultsavailable)))
		       (returned (parse-arg (get resultset 'totalresultsreturned)))
		       (last (+ returned start)))
		  (tryif (> available last)
			 (begin (if reqwait (sleep reqwait))
				(getbookmarks (qc tags) user (1+ last))))))
	(error mywebfailure getbookmarks requrl gotten))))

(define (gettopbookmarks tags (user #f))
  (let* ((requrl (geturls-url (qc tags) user 1))
	 (gotten (begin(when trace-myweb (message "gettopbookmarks: " requrl))
		       (urlxml requrl 'data)))
	 (resultset (xmlget gotten 'resultset)))
    (if (exists? resultset)
	(conver2bookmark (get resultset 'result))
	(error mywebfailure convert2bookmark requrl gotten))))

(define (myweb/getbookmarks tags (user #f))
  (when trace-myweb
    (message "myweb/getbookmarks " tags " as " user
	     " user=" (write (getmywebid user))))
  (meltcache/get myweb-cache getbookmarks (qc tags) (getmywebid user)))
(define (myweb/topbookmarks tags (user #f))
  (meltcache/get myweb-cache topbookmarks (qc tags) (getmywebid user)))

;;;; lookupmethod
;;;;  used by tagmaps

(define lookupmethod
  (ambda (tags userid)
    (if (exists? userid)
	(myweb/getbookmarks (qc tags) (getmywebid userid))
	(meltcache/accumulate myweb-cache toptags2bookmarks (qc tags) #f))))


