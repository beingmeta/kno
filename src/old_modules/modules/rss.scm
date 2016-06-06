;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'rss)

;;; Provides access to RSS feeds with automatic caching and intervals.

(use-module '{fdweb texttools varconfig parsetime})

(define rss-cache (make-hashtable))
(define rss-caches (make-hashtable))

(define default-update-frequency (* 15 60))
(varconfig! rss:spacing default-update-frequency)

(define default-analyzer #f)
(varconfig! rss:analyzer default-analyzer)

(define xmloptions 'slotify)
(varconfig! rss:xmlopts xmloptions)

(define try-unique-ids '{guid})
(varconfig! rss:uniqueid try-unique-ids #t choice)

(define (normalize-entry entry feeduri)
  (store! entry 'uri (get entry '{uri link}))
  (store! entry 'date
	  (try (parsetime (get entry '{updated pubdate}))
	       (gmtimestamp 'seconds)))
  (store! entry 'uniqueid
	  (try (pickstrings (get entry '{guid id}))
	       (vector feeduri (try (get (get entry 'date) 'iso) #f)
		       (get entry 'uri))))
  ;; (store! entry 'keywords)
  (store! entry 'description
	  (strip-markup (decode-entities (get entry '{summary description}))))
  (store! entry 'title
	  (strip-markup (decode-entities (get entry 'title))))
  (store! entry 'uri (get entry '{link about}))
  entry)

(define (rssget uri (frequency default-update-frequency))
  (let* ((parsed (parseuri uri))
	 (normalized (unparseuri parsed))
	 (cache (try (get rss-caches normalized)
		     (get rss-caches (get parsed 'host))
		     rss-cache)))
    (let ((expiration (get cache (cons 'expires normalized))))
      (if (or (fail? expiration) (time>? expiration))
	  (let* ((fetched (urlget normalized))
		 (parsed (xmlparse (get fetched '%content) (qc xmloptions)))
		 (items (normalize-entry (xmlget parsed '{item entry})
					 uri)))
	    (store! cache (cons 'expires normalized)
		    (timestamp+ frequency))
	    (store! cache normalized items)
	    items)
	  (get cache normalized)))))

(module-export! 'rssget)

