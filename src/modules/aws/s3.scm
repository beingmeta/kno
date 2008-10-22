(in-module 'aws/s3)

(define version "$Id$")

(use-module '{aws fdweb texttools})

(module-export! '{s3/signature s3/op s3/signeduri})

;;; This is used by the S3 API sample code and we can use it to
;;;  test the signature algorithm
(define teststring
  "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg")

(define (s3/signature op bucket path (date (gmtimestamp)) (cheaders "")
		     (content-sig "") (content-ctype ""))
  (let* ((date (if (string? date) date
		   (if (number? date) (number->string date)
		       (get date 'rfc822))))
	 (description
	  (string-append
	   op "\n"
	   content-sig "\n"
	   content-ctype "\n"
	   date "\n" cheaders "/" bucket path)))
    ;; (message "Description=" (write description))
    (hmac-sha1 secretawskey description)))

(define (s3/op op bucket path (content #f) (ctype "text") (cheaders ""))
  (let* ((date (gmtimestamp))
	 (cresource (string-append "/" bucket path))
	 (sig (s3/signature op bucket path date ""
			    (if content (packet->base64 (md5 content)) "")
			    (if content ctype "")))
	 (authorization
	  (string-append "AWS " awskey ":" (packet->base64 sig)))
	 (url (string-append "http://" bucket ".s3.amazonaws.com" path))
	 (urlparams (frame-create #f)))
    ;; (message "sig=" sig)
    ;; (message "authorization: " authorization)
    (when content (add! urlparams 'content-type ctype))
    (add! urlparams 'header (string-append "Date: " (get date 'rfc822)))
    (add! urlparams 'header (string-append "Authorization: " authorization))
    (urlget url urlparams)))

(define (s3/signeduri bucket path expires (op "GET") (cheaders ""))
  (let* ((expires (if (number? expires) expires (get expires 'tick)))
	 (sig (s3/signature "GET" bucket path expires cheaders)))
    (string-append
     "http://" bucket ".s3.amazonaws.com" path "?"
     "AWSAccessKeyId=" awskey "&"
     "Expires=" (number->string expires) "&"
     "Signature=" (packet->base64 sig))))

(comment (s3/op "GET" "data.beingmeta.com" "/brico/brico.db" ""))

