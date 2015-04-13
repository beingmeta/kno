;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2014-2015 beingmeta, inc. All rights reserved

(in-module 'curlcache)

(use-module '{fdweb varconfig logger})
(define %used_modules '{varconfig xhtml/auth})

(define-init curlcache #f)
(varconfig! aws:curlcache curlcache)

(module-export! '{curlcache/get curlcache/reset!})

(define (curlcache/get (cachesym))
  (default! cachesym
    (and curlcache (if (symbol? curlcache) curlcache 'curlcache)))
  (if cachesym
      (try (threadget cachesym)
	   (let ((handle (curlopen)))
	     (threadset! cachesym handle)
	     handle))
      (frame-create #f)))

(define (curlcache/reset! (cachesym))
  (default! cachesym
    (and curlcache (if (symbol? curlcache) curlcache 'curlcache)))
  (let ((handle (curlopen)))
    (threadset! cachesym handle))
  cachesym)


