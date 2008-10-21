(in-module 'aws)

(module-export! '{s3op s3signature s3uri})

(use-module '{fdweb texttools})

;; Default (non-working) values from the online documentation
;;  Helpful for testing requests
(define secretawskey
  (or (getenv "AWS_SECRET_ACCESS_KEY")
      #"uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o"))
(define awskey
  (or (getenv "AWS_ACCESS_KEY_ID")
      "0PN5J17HBGZHT7JJ3X82"))

(config-def! 'secretawskey
	     (lambda (var (val))
	       (if (bound? val)
		   (set! secretawskey val)
		   secretawskey)))
(config-def! 'awskey
	     (lambda (var (val))
	       (if (bound? val)
		   (set! awskey val)
		   awskey)))

(define teststring
  "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg")

(define (s3signature op bucket path (date (gmtimestamp)) (cheaders "")
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

(define (s3op op bucket path (content #f) (ctype "text") (cheaders ""))
  (let* ((date (gmtimestamp))
	 (cresource (string-append "/" bucket path))
	 (sig (s3signature op bucket path date ""
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

(define (s3uri bucket path expires (op "GET") (cheaders ""))
  (let* ((expires (if (number? expires) expires (get expires 'tick)))
	 (sig (s3signature "GET" bucket path expires cheaders)))
    (string-append
     "http://" bucket ".s3.amazonaws.com" path "?"
     "AWSAccessKeyId=" awskey "&"
     "Expires=" (number->string expires) "&"
     "Signature=" (packet->base64 sig))))

(comment
 (s3op "GET" "data.beingmeta.com" "/brico/brico.db" ""))

;;; Simple DB stuff

(define sdb-common-params
  '("Version" "2007-11-07" "SignatureVersion" "1"))

(define (simpledb-signature ptable)
  (let ((desc (stringout
		(doseq (key (lexsorted (getkeys ptable) downcase))
		  (printout key
			    (do-choices (v (get ptable key)) (printout v)))))))
    (message "desc=" desc)
    (hmac-sha1 secretawskey desc)))
(define (simpledb-signature0 action timestamp)
  (let ((desc (stringout action (get timestamp 'iso))))
    (message "desc=" desc)
    (hmac-sha1 secretawskey desc)))

(define (simpledb->uri . params)
  (let ((timestamp (gmtimestamp 'seconds))
	(ptable (frame-create #f)))
    (do ((p sdb-common-params (cddr p)))
	((null? p))
      (add! ptable (car p) (cadr p)))
    (do ((p params (cddr p)))
	((null? p))
      (add! ptable (car p) (cadr p)))
    (add! ptable "Timestamp" (get timestamp 'iso))
    (add! ptable "AWSAccessKeyId" awskey)
    (stringout "http://sdb.amazonaws.com/?"
	       (do ((p params (cddr p)) (first #t #f))
		   ((null? p) (printout))
		 (printout (unless first  "&")
			   (car p) "="
			   (uriencode (stringout (cadr p)))))
	       (do ((p sdb-common-params (cddr p)) (first #t #f))
		   ((null? p) (printout))
		 (printout "&" (car p) "=" (uriencode (stringout (cadr p)))))
	       "&Timestamp=" (get timestamp 'iso) 
	       "&AWSAccessKeyId=" awskey 
	       "&Signature="
	       (uriencode (packet->base64 (simpledb-signature ptable))))))

(define (simpledb . params)
  (let ((uri (apply simpledb->uri params)))
    (urlget uri)))

(comment (simpledb "Action" "CreateDomain" "DomainName" "bricotags"))

(module-export! '{simpledb-signature simpledb->uri simpledb})

