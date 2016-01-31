;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2011-2016 beingmeta, inc. All rights reserved

(in-module 'gravatar)

(use-module '{fdweb texttools})

(module-export!
 '{gravatar/hash gravatar/image
   gravatar/profile gravatar/json})

(define-init gravatars (make-hashtable))
(define gravatars (make-hashtable))

(define (gravatar/hash s)
  (downcase (packet->base16 (md5 (downcase (trim-spaces s))))))

(define plus-pat #((not> {"@" "+"}) "+" (not> "@") "@"))
(define plus-extract
  #((label id (not> {"@" "+"})) "+" (not> "@") "@"
    (label host (rest))))
(define (strip-plus s)
  (let ((match (text->frame plus-extract s)))
    (glom (get match 'id) "@" (get match 'host))))

(define (gravatar/image s (size #f) (suffix #f) (canonical) (hash))
  (default! canonical (downcase (trim-spaces s)))
  (default! hash (downcase (packet->base16 (md5 canonical))))
  (try (scripturl (glom (or (get gravatars canonical) {}) (or suffix ""))
	   's (tryif size size))
       (let ((usehash (probehash hash))
	     (httpsurl #f))
	 (when (and (not usehash) (textsearch plus-pat canonical))
	   (set! canonical (strip-plus canonical))
	   (set! hash (downcase (packet->base16 (md5 canonical))))
	   (set! usehash (probehash hash)))
	 (when usehash
	   (set! httpsurl
		 (glom "https://secure.gravatar.com/avatar/" usehash)))
	 (if httpsurl
	     (store! gravatars canonical httpsurl)
	     (store! gravatars canonical #f))
	 (and httpsurl
	      (if (or suffix size)
		  (if (or suffix size)
		      (scripturl (glom httpsurl (or suffix ""))
			  's (tryif size size))
		      (if suffix (glom httpsurl (or suffix ""))
			  (scripturl httpsurl 'size size)))
		  httpsurl)))))

(define (probehash hash)
  (let* ((baseurl (glom "http://www.gravatar.com/avatar/" hash))
	 (httpsurl (glom "https://secure.gravatar.com/avatar/" hash))
	 (probeurl (scripturl baseurl "d" "UNlikeLY.jpg"))
	 (head (urlhead probeurl)))
    (and (not (equal? (basename (get head 'location))
		      "UNlikeLY.jpg"))
	 hash)))

;;; Profiles

(define-init profile-urls (make-hashtable))

(define terminals "/profiles/no-such-user")

(define (mergeuri base uri)
  (if (has-prefix uri {"http:" "https:"}) uri
      (if (has-prefix uri "/")
	  (glom (urischeme base) "://" (urihost base)
	    uri)
	  #f)))

(define (find-profile url (depth 0))
  (and (< depth 5)
       (let* ((head (urlhead url))
	      (response (get head 'response))
	      (redirect (and (<= 300 response 399)
			     (mergeuri url (get head 'location)))))
	 (if redirect
	     (find-profile redirect (1+ depth))
	     (and (<= 200 response 299) url)))))

(define (gravatar/profile s (canonical))
  (default! canonical (downcase (trim-spaces s)))
  (try (get profile-urls canonical)
       (let* ((url (stringout "http://www.gravatar.com/" (gravatar/hash s)))
	      (real (find-profile url)))
	 (store! profile-urls canonical real)
	 real)))

(define (gravatar/json s)
  (let* ((base (gravatar/profile s))
	 (url (and base (glom base ".json")))
	 (content (and url (urlcontent url)))
	 (parsed (and content (jsonparse content))))
    (and parsed (table? parsed) (test parsed 'entry)
	 (vector? (get parsed 'entry))
	 (first (get parsed 'entry)))))

