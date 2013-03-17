(in-module 'gnosys/tagsites)

;;; This file contains functions for accessing and using different tagging sites
;;;  Copyright (C) 2007-2013 beingmeta, inc.

(define version "$Id$")

(use-module '{fdweb texttools ezrecords logger meltcache})
(use-module '{gnosys})

(define %loglevel %warning!)

(define (getfeed url)
  (logdebug "Fetching data from " url)
  (prog1 (urlxml url 'data)
	 (logdebug "Fetched data from " url)))

(define (xget entry slotid)
  (let ((d (get entry slotid)))
    (choice (pickstrings d) (xmlcontent (reject d string?)))))


;;;; Tag records

(define site-tagify (make-hashtable))

;; A tag record contains a siteid and a vector of terms
(defrecord tag siteid terms (user #f))

(define (make-tag siteid term . terms)
  (if (vector? term)
      (if (null? terms)
	  (cons-tag siteid term)
	  (error 'syntax "MAKE-TAG takes either one vector or N strings"))
      (cons-tag siteid (sorted (choice term (elts terms))))))
(define (make-user-tag user siteid term . terms)
  (if (vector? term)
      (if (null? terms)
	  (cons-tag siteid term user)
	  (error 'syntax "MAKE-TAG takes either one vector or N strings"))
      (cons-tag siteid (sorted (choice term (elts terms))) user)))

(define (tag->string tag (withsite #f))
  (stringout (doseq (term (tag-terms tag) i)
	       (printout (if (> i 0) "+") term))
	     (if withsite
		 (if (tag-user tag)
		     (printout "/by/" (tag-user tag) "@" (tag-siteid tag))
		     (printout "@" (tag-siteid tag))))))
(define (string->tag string (site #f))
  (let* ((bypos (search "/by/" string))
	 (atpos (search "@" string))
	 (etcpos (search "/etc/" string))
	 (tagend (or bypos atpos))
	 (usrend (or atpos etcpos)))
    (cons-tag (if atpos (subseq string (1+ atpos) etcpos) site)
	      (sorted (elts (segment (subseq string 0 tagend) "+")))
	      (and bypos (subseq string (+ bypos 4) usrend)))))

(define (tagify term site (user #f))
  (let ((normalize (try (get site-tagify site) downcase)))
    (for-choices (vary (if (vector? term)
			   (sortvec (apply vector (map normalize (->list term))))
			   (vector (normalize term))))
      (cons-tag site vary))))

(module-export!
 '{tag? tag-siteid tag-terms tag-user
	cons-tag make-tag tagify
	tag->string string->tag})


;;;; Tag caches

;;; This is where RSS requests for tags are cached using meltcaches.
;;;  The values stored here will be meltentries referring to sets of items.
;;;  This is configurable to be an external index. 
(define tag-cache (make-hashtable))
(define tagcache-file #f)

;;; Configuring the tagcache to a string creates/uses an index file
;;; Configuring it to a table just uses that table
;;; Configuring it to anything else resets it
(define (config-tagcache var (val))
  (cond ((not (bound? val)) tag-cache)
	((and (string? val) (has-suffix val ".index"))
	 (unless (file-exists? val) (make-hash-index val 2000))
	 (set! tag-cache (open-index val))
	 (notify "Tag cache is now " tag-cache)
	 tag-cache)
	((string? val)
	 (if (file-exists? val)
	     (set! tag-cache (file->dtype val))
	     (set! tag-cache (make-hashtable)))
	 (set! tag-cache-file val)
	 (notify "Saving tag cache to " tag-cache-file)
	 tag-cache)
	((table? val)
	 (set! tag-cache val)
	 (notify "Tag cache is now " tag-cache))
	(else (set! tag-cache (make-hashtable))
	      (notify "Reset tag cache to " tag-cache)
	      tag-cache)))
(config-def! 'tagcache config-tagcache)


;;;; Tag saving

(defslambda (save-tagcache)
  (if (index? tag-cache)
      (commit tag-cache)
      (if tagcache-file
	  (dtype->file tag-cache tagcache-file)
	  (warning "Couldn't save tagcache (no file specified)"))))

(define tagsave #f)

(define (save-tagcache-loop)
  (while tagsave
    (sleep tagsave)
    (if tagsave (save-tagcache))))

(defslambda (config-tagsave var (val))
  (cond ((not (bound? val)) tagsave)
	((not val) (set! tagsave val))
	((and (number? val) (not tagsave))
	 (set! tagsave val)
	 (unless (or (index? tag-cache) tagcache-file)
	   (warning "Can't save tag-cache " tag-cache))
	 (spawn (save-tagcache-loop)))
	((number? val) (set! tagsave val))
	(else (save-tagcache))))
(config-def! 'tagsave config-tagsave)


;;; Generic methods

(define site-getfns {})
(define site-urifns {})
(define site-uriurifns {})
(define site-info {})

(define (tag/getitems tag (site #f))
  (if (tag? tag)
      (meltcache/get tag-cache
		     (get site-getfns (tag-siteid tag)) (tag-terms tag))
      (meltcache/get tag-cache
		     (get site-getfns site) tag)))

(define (tag/geturi tag (for #f))
  (if (tag? tag)
      (let ((method (get site-urifns (tag-siteid tag))))
	(tryif (exists? method) (method tag for)))
      (fail)))

(define (tag/getinfo tag field)
  (if (tag? tag)
      (get (get site-info (tag-siteid tag)) field)
      (get (get site-info tag) field)))

(define (tag/%getitems tag (site #f))
  (if (tag? tag)
      ((get site-getfns (tag-siteid tag)) (tag-terms tag))
      ((get site-getfns site) tag))) 

(define (tag/istag? tag (site #f)) (exists? (tag/getitems tag site)))

(define (tag/geturls tag (site #f)) (get (tag/getitems tag site) 'ts%uri))

(define (gettags-threshold values)
  (if (fail? values) (* 24 60 60 2) (* 60 60)))

(do-choices (method '{deliciousget flickrget technoratiget youtubeget})
  (config! 'meltcache (cons method gettags-threshold)))

(module-export!
 '{tag/getitems tag/geturi tag/getinfo tag/%getitems tag/istag? tag/geturls})


;;; Summary of different tagging systems

;; Flickr, technorati, youtube, and del.icio.us all ignore case
;; Technorati keeps spaces, the other guys ignore them

;;; Technorati stuff

(define (technoratiget tag (limit 3))
  (tryif (or (string? tag) (= (length tag) 1))
	 (let* ((url (if (string? tag)
			 (scripturl (string-append "http://feeds.technorati.com/tag/" tag))
			 (scripturl (string-append "http://feeds.technorati.com/tag/" (first tag)))))
		(result (getfeed url))
		(rss (tryif (or (pair? result) (table? result))
			    (xmlget result 'rss))))
	   (technorati-entry (get (get rss 'channel) 'item)))))
(set+! site-getfns (cons 'technorati technoratiget))

(define (technoratigeturi tag (for #f))
  (cond ((string? tag)
	 (scripturl (string-append "http://feeds.technorati.com/tag/" tag)))
	((not (tag? tag)) (fail))
	((= (length (tag-terms tag)) 1)
	 (scripturl (string-append "http://feeds.technorati.com/tag/"
				   (first (tag-terms tag)))))
	(else (fail))))
(set+! site-urifns (cons 'technorati technoratigeturi))

(define (technorati-entry entry)
  (let ((uri (parseuri (decode-entities (pickstrings (get entry 'link)))))
	(title (decode-entities (xget entry 'title)))
	(tags (for-choices (term (elts (segment (get entry 'subject))))
		(make-tag 'technorati term))))
    (frame-create #f
      'uri (unparseuri uri) 'title title 'guid (get entry 'guid)
      'description (decode-entities (xget entry 'description)) 'tags tags
      'host (get uri 'hostname) 'date (get entry 'pubdate))))

(define (technorati/getitems tag (limit #f))
  (if limit
      (meltcache/get tag-cache technoratiget tag limit)
      (meltcache/get tag-cache technoratiget tag)))
(define (technorati/geturls tag (limit #f))
  (get (technorati/getitems tag limit) 'link))

(define (technorati/istag? tag)
  (exists? (get (technorati/getitems tag) 'link)))

(define (technorati/variants word)
  (downcase (stdspace
	     (choice word (depunct word)
		     (textsubst word '(subst (* (ispunct+)) " "))))))
(add! site-tagify 'technorati technorati/variants)

(module-export!
 '{technorati/getitems technorati/geturls technorati/istag?
   technorati/variants})

(set+! site-info
       (cons 'technorati
	     (frame-create #f
	       'icon "/graphics/technorati14.png"
	       'medicon "/graphics/technorati29.png"
	       'uri "http://technorati.com/")))

;;; Delicious stuff

(define (deliciousget tag)
  (let* ((url (stringout ;; "http://del.icio.us/rss/tag/"
		  "http://feeds.delicious.com/rss/tag/"
		(if (vector? tag)
		    (doseq (t tag i)
		      (printout (if (> i 0) "+")
			(remove #\Space (remove #\. t))))
		    (do-choices (t tag i)
		      (printout (if (> i 0) "+")
			(remove #\Space (remove #\. t)))))))
	 (result (getfeed url)))
    (delicious-entry (get (xmlget result 'rdf) 'item))))
(set+! site-getfns (cons 'delicious deliciousget))

(define (delicious-urifn tag (for #f))
  (stringout
      "http://feeds.delicious.com/rss/tag/"
    (doseq (t (tag-terms tag) i)
      (printout (if (> i 0) "+")
	(remove #\Space (remove #\. t))))))
(set+! site-urifns (cons 'delicious delicious-urifn))

(define (delicious-entry entry)
  (let* ((uri (parseuri (decode-entities (pickstrings (get entry 'link)))))
	 (title (decode-entities (xget entry 'title)))
	 (tags (make-tag 'delicious (elts (segment (get entry 'subject)))))
	 (uristring (unparseuri uri)))
    (frame-create #f
      'uri uristring 'title title 'guid (get entry 'guid)
      'tags tags 'description (decode-entities (xget entry 'description))
      'host (get uri 'hostname) 'source (get entry 'creator)
      'pubdate (get entry 'date)
      'metauri
      (scripturl (string-append"http://del.icio.us/url/"
			       (packet->base16 (md5 uristring)))))))

(define (delicious/getitems tag  (limit 3))
  (meltcache/get tag-cache deliciousget tag))
(define (delicious/geturls tag (limit 3))
  (get (delicious/getitems tag limit) 'link))

(define (delicious/istag? tag)
  (exists? (get (delicious/getitems tag) 'link)))

(define (delicious/variants word)
  (downcase (depunct word)))
(add! site-tagify 'delicious delicious/variants)

(module-export!
 '{delicious/getitems delicious/geturls delicious/istag?
   delicious/variants})

(set+! site-info
       (cons 'delicious
	     (frame-create #f
	       'icon "/graphics/delicious14px.gif"
	       'medicon "/graphics/delicious29.png"
	       'uri "http://del.icio.us/")))

;;; Flickr stuff

(define sample "http://api.flickr.com/services/feeds/photos_public.gne?tags=shark&lang=en-us&format=atom")

(defambda (getflickrfeed tags)
  (stringout "http://api.flickr.com/services/feeds/photos_public.gne?tags="
    (if (vector? tags)
	(doseq (tag tags i)
	  (printout (if (> i 0) ",") (remove #\Space tag)))
	(do-choices (tag tags i)
	  (printout (if (> i 0) ",") (remove #\Space tag))))
    "&lang=en-us&format=atom"))

(define (flickrgeturi tags)
  (stringout "http://api.flickr.com/services/feeds/photos_public.gne?tags="
    (if (vector? tags)
	(doseq (tag tags i)
	  (printout (if (> i 0) ",") (remove #\Space tag)))
	(do-choices (tag tags i)
	  (printout (if (> i 0) ",") (remove #\Space tag))))
    "&lang=en-us&format=atom"))
(define (flickr-urifn tag (for #f))
  (flickrgeturi (tag-terms tag)))
(set+! site-urifns (cons 'flickr flickr-urifn))

(define (flickrfeed tag)
  (let* ((url (getflickrfeed tag))
	 (result (getfeed url)))
    (xmlget result 'feed)))

(define (flickrget tag)
  (let* ((url (getflickrfeed tag))
	 (result (getfeed url)))
    (flickr-entry (get (xmlget result 'feed) 'entry))))
(set+! site-getfns (cons 'flickr flickrget))

(define (flickr-entry entry)
  (let ((uri (parseuri (get (pick (get entry 'link) 'type "alternate") 'href)))
	(title (decode-entities (xget entry 'title)))
	(tags (for-choices (xtag (get entry 'category))
		(make-tag 'flickr (get xtag 'term)))))
    (frame-create #f
      'uri (unparseuri uri) 'title title 'guid (get entry 'guid)
      'tags tags 'description (decode-entities (get entry 'description))
      'host (get uri 'hostname) 'source (get entry 'author)
      'date (get entry 'published) 'recorded (get entry 'date.taken))))

(define (flickr/getitems tag  (limit 3))
  (meltcache/get tag-cache flickrget tag))
(define (flickr/geturls tag (limit 3))
  (get (get (flickr/getitems tag limit) 'link) 'href))

(define (flickr/istag? tag)
  (exists? (get (flickr/getitems tag) 'link)))

(define (flickr/iscombo tag vs)
  (let ((items (flickr/geturls tag)))
    (and (exists? items)
	 (let ((vsitems (flickr/geturls vs)))
	   (not (identical? items vsitems))))))

;; Just like del.icio.us
(define (flickr/variants word) (downcase (depunct word)))
(add! site-tagify 'flickr flickr/variants)

(module-export!
 '{flickr/getitems
   flickr/geturls flickr/istag?
   flickr/variants})

(set+! site-info
       (cons 'flickr
	     (frame-create #f
	       'icon "/graphics/flickr16.png"
	       'medicon "/graphics/flickr29.png"
	       'uri "http://flickr.com/")))

;;; Youtube stuff

(defambda (getyoutubefeed tags)
  (stringout "http://gdata.youtube.com/feeds/api/videos/-"
    (if (vector? tags)
	(doseq (tag tags i)
	  (printout "/" (remove #\Space tag)))
	(do-choices (tag tags i)
	  (printout "/" (remove #\Space tag))))))
(define (youtube-urifn tag (for #f))
  (getyoutubefeed (tag-terms tag)))
(set+! site-urifns (cons 'youtube youtube-urifn))

(define (youtubefeed tag)
  (let* ((url (getyoutubefeed tag))
	 (result (getfeed url)))
    (xmlget result 'feed)))

(define (youtubeget tag)
  (let* ((url (getyoutubefeed tag))
	 (result (getfeed url)))
    (youtube-entry (get (xmlget result 'feed) 'entry))))
(set+! site-getfns (cons 'youtube youtubeget))

(define (youtube-entry entry)
  (let ((uri (parseuri (get (pick (get entry 'link) 'rel "self") 'href)))
	(title (decode-entities (xget entry 'title)))
	(tags (for-choices (xtag (get entry 'category))
		(make-tag 'youtube (get xtag 'term)))))
    (frame-create #f
      'uri (unparseuri uri) 'title title 'guid (get entry 'guid)
      'tags tags 'description (decode-entities (get entry 'description))
      'source (get entry 'author) 'host (get uri 'hostname)
      'date (get entry 'published) 'recorded (get entry 'recorded))))

(define (youtube/getitems tag  (limit 3))
  (meltcache/get tag-cache youtubeget tag))
(define (youtube/geturls tag (limit 3))
  (get (get (youtube/getitems tag limit) 'link) 'href))

(define (youtube/istag? tag)
  (exists? (get (youtube/getitems tag) 'link)))

(define (youtube/iscombo tag vs)
  (let ((items (flickr/geturls tag)))
    (and (exists? items)
	 (let ((vsitems (flickr/geturls vs)))
	   (not (identical? items vsitems))))))

;; Just like del.icio.us
(define (youtube/variants word)
  (downcase (depunct word)))
(add! site-tagify 'youtube youtube/variants)

(module-export!
 '{youtube/getitems
   youtube/geturls youtube/istag?
   youtube/variants})

(set+! site-info
       (cons 'youtube
	     (frame-create #f
	       'icon "/graphics/YouTube14.png"
	       'medicon "/graphics/YouTube29.png"
	       'uri "http://youtube.com/")))

;;; Amazon stuff

(define (amazonget tag (limit 3))
  (tryif (or (string? tag) (= (length tag) 1))
	 (let* ((url (if (string? tag)
			 (string-append "http://www.amazon.com/rss/tag/"
					(string-subst tag " " "%20") "/recent")
			 (string-append "http://www.amazon.com/rss/tag/"
					(string-subst (first tag) " " "%20") "/recent")))
		(result (getfeed url))
		(rss (tryif (or (pair? result) (table? result))
			    (xmlget result 'rss))))
	   (amazon-entry (get (get rss 'channel) 'item)))))
(set+! site-getfns (cons 'amazon amazonget))

(define (amazon-entry entry)
  (let ((uri (parseuri (get entry 'link)))
	(title (decode-entities (xget entry 'title)))
	(tags (for-choices (xtag (get entry 'category))
		(make-tag 'amazon (get xtag 'term)))))
    (frame-create #f
      'uri (unparseuri uri) 'title title 'guid (get entry 'guid)
      'tags tags 'description (get entry 'description)
      'host (get uri 'hostname))))

#|
(define (amazon-entry entry)
  (let ((uri (parseuri (get entry 'link)))
	(title (decode-entities (xmlcontent (get entry 'title))))
	(tags (for-choices (xtag (get entry 'category))
		(make-tag 'amazon (get xtag 'term)))))
    (modify-frame entry
      'ts%uri (unparseuri uri) 'ts%title title 'ts%tags tags
      'ts%host (get uri 'hostname))))
|#

(define (amazon/getitems tag  (limit 3))
  (meltcache/get tag-cache amazonget tag))
(define (amazon/geturls tag (limit 3))
  (get (get (amazon/getitems tag limit) 'link) 'href))

(define (amazon/istag? tag)
  (exists? (get (amazon/getitems tag) 'link)))

;; Just like del.icio.us
(define (amazon/variants word)
  (choice (downcase word) (downcase (depunct word))))
(add! site-tagify 'amazon amazon/variants)

(module-export!
 '{amazon/getitems
   amazon/geturls amazon/istag?
   amazon/variants})

;;; MyWeb stuff

(define (mywebget tag)
  (let* ((url (stringout
		  "http://myweb.yahoo.com/mywebrss/tag/"
		(string-subst (stdspace tag) " " "+")
		"/urls.xml"))
	 (result (getfeed url)))
    (get (get (xmlget result 'rss) 'channel) 'item)))
(set+! site-getfns (cons 'myweb mywebget))

(define (myweb/getitems tag (limit 3))
  (meltcache/get tag-cache mywebget tag))
(define (myweb/geturls tag (limit 3))
  (get (myweb/getitems tag limit) 'link))

(define (myweb/istag? tag)
  (exists? (get (myweb/getitems tag) 'link)))

(define (myweb/variants word)
   (textsubst (stdspace word) '(subst (ispunct+) "")))
(add! site-tagify 'myweb myweb/variants)

(module-export!
 '{myweb/getitems
   myweb/geturls myweb/istag?
   myweb/variants})

;;; Additional data


;;; Generic methods

(do-choices (method (choice deliciousget flickrget technoratiget youtubeget))
  (config! 'meltcache (cons method gettags-threshold)))


;;; Configuration handlers

(defslambda (config-tagsiteinfo var (val))
  (cond ((not (bound? val)) site-info)
	((not (pair? val)) (error 'tagsite-config "Must be site/value pair"))
	((not (test site-info (car val)))
	 (set+! site-info val))
	(else (set! site-info (cons val (reject site-info (car val)))))))
(config-def! 'tagsiteinfo config-tagsiteinfo)

(defslambda (config-tagsitegetfn var (val))
  (cond ((not (bound? val)) site-getfns)
	((not (pair? val)) (error 'tagsite-config "Must be site/value pair"))
	((not (test site-getfns (car val)))
	 (set+! site-info val))
	(else (set! site-getfns (cons val (reject site-getfns (car val)))))))
(config-def! 'tagsiteget config-tagsitegetfn)

(defslambda (config-tagsiteurifn var (val))
  (cond ((not (bound? val)) site-urifns)
	((not (pair? val)) (error 'tagsite-config "Must be site/value pair"))
	((not (test site-urifns (car val)))
	 (set+! site-info val))
	(else (set! site-urifns (cons val (reject site-urifns (car val)))))))
(config-def! 'tagsiteuri config-tagsiteurifn)

