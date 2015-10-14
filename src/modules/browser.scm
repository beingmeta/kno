;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc. All rights reserved.

(in-module 'browser)

(use-module '{logger varconfig gpath aws/s3})

(define %loglevel %info%)

(module-export! '{browse})

(define browse-commands
  #["linux" "xdg-open" "osx" "open" "darwin" "open"
    "bsd" "xdg-open"])

(define browser-command
  (try (get browse-commands (downcase (get (uname) 'osname))) #f)
  #f)
(varconfig! browser browser-command)

(define (browse loc)
  (if (string? loc) (set! loc (or (->gpath loc) loc)))
  (let ((url (if (string? loc) loc
		 (if (s3loc? loc)
		     (s3/signeduri loc)
		     (gpath->uri loc)))))
    (debug%watch "BROWSE" loc url)
    (if (position #\/ browser-command)
	(fork/exec browser-command url)
	(fork/cmd browser-command url))))
