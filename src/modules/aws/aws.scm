(use-module '{fdweb texttools})

(define accesskeysecret "0Q+UGBUJQYJL3vq85mTJXLnlN2kfEw5xdKmS84c7")
(define accesskeyid "1QYJ5P9NZE42YRQK5B02")

(define (awsop op bucket path subpath (content #f) (ctype "text"))
  (let* ((date (get (timestamp) 'iso))
	 (cheaders "")
	 (cresource (string-append "/" bucket path))
	 (sig (string-append op "\n"
			     (if content (packet->base64 (md5 content)) "") "\n"
			     (if content ctype "") "\n"
			     date "\n" cheaders cresource))
	 (authorization (string-append "AWS " accesskeyid ":"
				       (packet->base64 (hmac-sha1 accesskeysecret sig))))
	 (url (string-append "http://" bucket ".s3.amazonaws.com" path))
	 (urlparams (frame-create #f)))
    (%watch sig authorization)
    (when content (add! urlparams 'content-type ctype))
    (add! urlparams 'header (string-append "Date: " date))
    (add! urlparams 'header (cons 'date date))
    (add! urlparams 'header (cons 'authorization authorization))
    (urlget url urlparams)))





