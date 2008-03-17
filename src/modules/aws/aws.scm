(in-module 'aws)

(module-export! 'awsop)

(use-module '{fdweb texttools})

;; Default (non-working) values from the Amazon web site
(define secretawskey
  (or (getenv "AWS_SECRET_ACCESS_KEY")
      "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o"))
(define awskeyid
  (or (getenv "AWS_ACCESS_KEY_ID")
      "0PN5J17HBGZHT7JJ3X82"))

(config-def! 'secretawskey
	     (lambda (var (val))
	       (if (bound? val)
		   (set! secretawskey val)
		   secretawskey)))
(config-def! 'awskeyid
	     (lambda (var (val))
	       (if (bound? val)
		   (set! awskeyid val)
		   awskeyid)))

(comment ;; Test case from Amazon website
 (set! accesskeysecret )
 (set! accesskeyid "0PN5J17HBGZHT7JJ3X82"))

(define teststring
  "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg")

(define (hack-date)
  (let ((t (gmtimestamp)))
    (stringout (capitalize (symbol->string (get t 'dowid))) ", "
	       (get t 'date) " " (get t 'month-short) " "
	       (get t 'year) " " (get t 'hms) " +0000")))

(define (awsop op bucket path subpath (content #f) (ctype "text"))
  (let* ((date (hack-date))
	 (cheaders "")
	 (cresource (string-append "/" bucket path))
	 (sig (string-append
	       op "\n"
	       (if content (packet->base64 (md5 content)) "") "\n"
	       (if content ctype "") "\n"
	       date "\n" cheaders "/" bucket path))
	 (authorization
	  (string-append
	   "AWS " awskeyid ":"
	   (packet->base64 (hmac-sha1 secretawskey sig))))
	 (url (string-append "http://" bucket ".s3.amazonaws.com" path))
	 (urlparams (frame-create #f)))
    (when content (add! urlparams 'content-type ctype))
    (add! urlparams 'header (string-append "Date: " date))
    (add! urlparams 'header (string-append "Authorization: " authorization))
    (urlget url urlparams)))

(comment
 (awsop "GET" "data.beingmeta.com" "/brico/brico.db" ""))

