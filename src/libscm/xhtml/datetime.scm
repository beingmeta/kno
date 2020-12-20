;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved

(in-module 'xhtml/datetime)

(use-module '{kno/reflect texttools})
(use-module '{webtools xhtml i18n})
(use-module 'parsetime)

(module-export! 'time->html)

;; Whether to use the DATETIME element
(define html5 #f)
;; What to use as a default timezone
(define timezone #f)

;; Numbers smaller than this are offsets from now, larger than this
;; are Unix time values.
(define maxinterval (* 3600 24 30 6))

(define (time->html timearg (opts #[]) (time))
  (if (symbol? opts) (set! opts `#[STYLE ,opts]))
  (set! time timearg)
  (if (and (number? time) (not (complex? time)))
      (set! time
	    (if (< time maxinterval)
		(timestamp+ (gmtimestamp) time)
		(gmtimestamp time)))
      (if (string? time) (set! time (parsetime time))))
  (unless (timestamp? time)
    (error |Not a Timestamp| TIME->HTML "The value " timearg
	   " cannot be converted into a time"))
  (xmlblock `(,(if (getopt opts 'html5 html5) 'time 'span)
	      (datetime ,(get time 'iso8601))
	      (class ,(glom "fdjtime " (getopt opts 'class "fdjthumantime"))))
      (get time (getopt opts 'display 'string))))

