;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2014-2016 beingmeta, inc. All rights reserved

(in-module 'curlcache)

(use-module '{fdweb varconfig logger})
(define %used_modules '{varconfig xhtml/auth})

(define-init curlcache-default #t)
(varconfig! curlcache:default curlcache-default)
(define-init curlcache #f)
(varconfig! curlcache curlcache config:boolean+parse)

(module-export! '{curlcache/get curlcache/reset!})

(define (curlcache/get (cachesym))
  (default! cachesym
    (and curlcache (if (symbol? curlcache) curlcache 'curlcache)))
  (if cachesym
      (try (threadget cachesym)
	   (if curlcache-default
	       (let ((handle (curlopen)))
		 (threadset! cachesym handle)
		 handle)
	       (frame-create #f)))
      (frame-create #f)))

(define (curlcache/reset! (cachesym) (force))
  (default! force (or (bound? cachesym) curlcache-default))
  (default! cachesym
    (and curlcache (if (symbol? curlcache) curlcache 'curlcache)))
  (if (or force (exists? (threadget cachesym)))
      (let ((handle (curlopen)))
	(threadset! cachesym handle)))
  cachesym)



