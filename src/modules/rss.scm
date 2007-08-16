;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'rss)

;;; Provides access to RSS feeds with automatic caching and intervals.

(define version "$Id:$")

(use-module '{fdweb texttools})

(define rss-cache (make-hashtable))
(define rss-caches (make-hashtable))

(define default-update-frequency (* 15 60))

(define (rssget uri (frequency #f))
  (let* ((parsed (parseuri uri))
	 (normalized (unparseuri parsed))
	 (cache (try (get rss-caches normalized)
		     (get rss-caches (get parsed 'host))
		     rss-cache)))
    (let ((expiration (get cache (cons 'expires normalized))))
      (if (or (fail? expiration) (timestamp-earlier? expiration))
	  (let* ((fetched (urlget normalized))
		 (analyzed (xmlparse (get fetched '%content) 'slotify))
		 (items (xmlget analyzed 'item)))
	    
	    )
	  (get cache normalized)))))

(module-export! 'rssget)

