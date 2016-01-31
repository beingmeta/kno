;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc. All rights reserved.

(in-module 'which)

(use-module '{logger texttools varconfig})

(module-export! '{which})

(define %loglevel %warn%)

(define (which cmd . alt)
  (let* ((paths (or (getenv "PATH") ""))
	 (scan (segment paths ":"))
	 (trying #f)
	 (found #f))
    (when (pair? alt)
      (dolist (c (cons cmd alt))
	(when (not found) (set! found (which c)))))
    (when (null? alt)
      (loginfo |Which| "Searching for " (write cmd) " in " (write paths))
      (while (and (not found) (pair? scan))
	(set! trying (mkpath (car scan) cmd))
	(set! scan (cdr scan))
	(logdebug |Which| "Trying path " (write trying))
	(when (file-exists? trying)
	  (lognotice |Which| "Found " (write cmd) " at " trying)
	  (set! found trying)))
      (when (not found)
	(lognotice |Which|
	  "Couldn't find " (write cmd) " in " (write paths))))
    found))






