;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved

(in-module 'xhtml/pagedate)

(use-module '{webtools})

(module-export! '{pagedate pagedate!})

(define (pagedate)
  (if (req/test 'PAGEDATE) (req/get 'PAGEDATE)
      (begin (req/set! 'PAGEDATE (file-modtime (req/get 'script_filename)))
	(req/get 'PAGEDATE))))

(define (pagedate! arg)
  (let* ((time (timestamp arg)) (cur (pagedate)))
    (if (time>? time cur) (req/set! 'PAGEDATE time))
    (req/get 'pagedate)))




