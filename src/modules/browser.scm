;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc. All rights reserved.

(in-module 'browser)

(use-module '{logger varconfig gpath which aws/s3})

(define %loglevel %info%)

(module-export! '{browse})

(define browse-commands
  #["osx" "open" "darwin" "open"])

(define browser-command
  (try (get browse-commands (downcase (get (uname) 'osname)))
       (which "sensible-browser" "xdg-open" "gnome-open" "kde-open"
	      "google-chrome" "firefox")
       #f))
(varconfig! browser browser-command)

(define-init browser-wait 2)
(varconfig! browser:wait browser-wait)

(define (browse loc)
  (when (and (table? loc) (test loc 'url)
	     (string? (get loc 'url)))
    (set! loc (get loc 'url)))
  (when (string? loc) (set! loc (or (->gpath loc) loc)))
  (let ((url (if (string? loc) loc
		 (if (s3loc? loc)
		     (s3/signeduri loc)
		     (error BADREF loc)))))
    (debug%watch "BROWSE" loc url)
    (let ((pid (if (position #\/ browser-command)
		   (fork browser-command url)
		   (fork/cmd browser-command url))))
      (when browser-wait (sleep browser-wait))
      (logwarn |BrowserLaunch|
	"Using " browser-command " with PID " pid))))

