;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'rss)

;;; Provides access to RSS feeds with automatic caching and intervals.
(define version "$Id$")
(define revision "$Revision:$")

(use-module '{fdweb texttools varconfig})

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

(define (rssget uri (analyzer default-analyzer) (uniqueid #f)
		(frequency default-update-frequency))
  (let* ((parsed (parseuri uri))
	 (normalized (unparseuri parsed))
	 (cache (try (get rss-caches normalized)
		     (get rss-caches (get parsed 'host))
		     rss-cache)))
    (let ((expiration (get cache (cons 'expires normalized))))
      (if (or (fail? expiration) (past-time? expiration))
	  (let* ((fetched (urlget normalized))
		 (parsed (xmlparse (get fetched '%content) (qc xmloptions)))
		 (items (if analyzer (analyzer parsed)
			    (xmlget parsed 'item)))
		 (uniqueid (or uniqueid
			       (try
				(filter-choices (id try-unique-ids)
				  (test items id))
				#f))))
	    (store! cache (cons 'expires normalized)
		    (timestamp+ frequency))
	    (store! cache normalized
		    (choice items
			    (if uniqueid
				(reject(get cache normalized) 
				       uniqueid (get items uniqueid))
				(get cache normalized))))
	    (get cache normalized))
	  (get cache normalized)))))

(module-export! 'rssget)

