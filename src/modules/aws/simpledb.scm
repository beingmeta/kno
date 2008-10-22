(in-module '{aws/simpledb})

(define version "$Id$")

(module-export! '{sdb/signature sdb/uri sdb/op sdb/opxml})
(module-export! '{sdb/fromlisp sdb/tolisp})
(module-export! '{sdb/put sdb/get sdb/add sdb/drop})

(use-module '{aws fdweb texttools})

(define (get-ptable params (integrate #f))
  "This returns a parameter table from a list of arguments."
  (do ((p params (cddr p)))
      ((or (not (pair? p)) (null? (cdr p)))
       (if (not (pair? p)) (frame-create #f)
	   (if (table? (car p)) (car p)
	       (frame-create #f))))))

;;; Simple DB stuff

(define sdb-common-params
  '("Version" "2007-11-07" "SignatureVersion" "1"))

(define (sdb/signature ptable)
  (let ((desc (stringout
		(doseq (key (lexsorted (getkeys ptable) downcase))
		  (printout key
			    (do-choices (v (get ptable key)) (printout v)))))))
    (hmac-sha1 secretawskey desc)))
(define (sdb/signature0 action timestamp)
  (let ((desc (stringout action (get timestamp 'iso))))
    (hmac-sha1 secretawskey desc)))

(define (sdb/uri . params)
  (let ((timestamp (gmtimestamp 'seconds))
	;; We can specify parameters as the last (odd) argument
	(ptable (get-ptable params)))
    (do ((p params (cddr p)))
	((or (null? p)  (null? (cdr p))))
      (add! ptable (car p) (cadr p)))
    (do ((p sdb-common-params (cddr p)))
	((null? p))
      (unless (test ptable (car p))
	(store! ptable (car p) (cadr p))))
    (store! ptable "Timestamp" (get timestamp 'iso))
    (store! ptable "AWSAccessKeyId" awskey)
    (stringout "http://sdb.amazonaws.com/?"
      (do-choices (key (getkeys ptable) i)
	(printout (if (> i 0) "&")
	  key "=" (uriencode (stringout (get ptable key))))) 
      "&Signature="
      (uriencode (packet->base64 (sdb/signature ptable))))))

(define (sdb/op action . params)
  (let ((uri (apply sdb/uri "Action" action params)))
    (urlget uri)))
(define (sdb/opxml action . params)
  (let ((uri (apply sdb/uri "Action" action params)))
    (urlxml uri 'data)))

(comment (sdb/op "CreateDomain" "DomainName" "bricotags"))

;;; Converting values

(define (sdb/tolisp string)
  (if (has-prefix string "!")
      (let ((tcode (and (> (length string) 1) (elt string 1))))
	(cond
	  ((has-prefix string "!!") (subseq string 1))
	  ((has-prefix string "!d") (timestamp (subseq string 2)))
	  ((has-prefix string "!B") #f)
	  ((has-prefix string "!b") #t)
	  ((has-prefix string "!i") (string->number (subseq string 2)))
	  ((has-prefix string "!I") (- (string->number (subseq string 2))))
	  (else (string->lisp (subseq string 1)))))
      string))

(define (sdb/fromlisp object (padlen 10))
  (cond ((oid? object)
	 (string-append "!" (oid->string object)))
	((string? object)
	 (if (has-prefix object "!")
	     (stringout "!" object)
	     object))
	((timestamp? object)
	 (stringout "!d" (get (get object 'gmt) 'iso)))
	((integer? object)
	 (let ((irep (number->string (abs object))))
	   (stringout "!" (if (>= object 0) "i" "I")
		      (dotimes (i (- padlen (length irep))) (printout "0"))
		      irep)))
	((eq? object #t) "!b")
	((not object) "!B")
	(else (stringout "!:" object))))

;;; PUT/GET/ETC
;;; We convert items and attributes using FramerD's parse-arg/unparse-arg
;;; We convert values using the tolisp/fromlisp procedures defined above

(define (sdb/put domain item . keyvals)
  (let ((ptable (get-ptable keyvals)))
    (do ((kv keyvals (cddr kv))
	 (i 0 (1+ i)))
	((or (not (pair? kv)) (not (pair? (cdr kv))))
	 (sdb/op "PutAttributes"
		 "DomainName" domain
		 "ItemName" (unparse-arg item)
		 ptable))
      (store! ptable (stringout "Attribute." i ".Name")
	      (unparse-arg (car kv)))
      (store! ptable (stringout "Attribute." i ".Value")
	      (sdb/fromlisp (cadr kv))))))

(define (sdb/get domain item (key #f))
  (let* ((key (and key (unparse-arg key)))
	 (xml (if key
		  (sdb/opxml "GetAttributes"
			     "DomainName" domain
			     "ItemName" (unparse-arg item)
			     "AttributeName" key)
		  (sdb/opxml "GetAttributes"
			     "DomainName" domain
			     "ItemName" (unparse-arg item))))
	 (result (get (get (xmlget xml 'getattributesresponse)
			   'getattributesresult)
		      'attribute)))
    ;; (message "xml=" xml)
    ;; (message "result=" result)
    (if key
	(get (pick result 'name key) 'value)
	(let ((table (frame-create #f)))
	  (do-choices (r result)
	    (add! table
		  (parse-arg (get r 'name))
		  (sdb/tolisp (get r 'value))))
	  table))))

(defambda (sdb/add domain item slotids values)
  (do-choices item
    (let ((ptable `#["DomainName" ,domain
		     "ItemName" ,(unparse-arg item)])
	  (count 0))
      (do-choices (key (unparse-arg slotids))
	(do-choices (value (sdb/fromlisp values))
	  (store! ptable (stringout "Attribute." count ".Name")
		  key)
	  (store! ptable (stringout "Attribute." count ".Value")
		  value)
	  (set! count (1+ count))))
      (sdb/op "PutAttributes" ptable))))

(defambda (sdb/drop domain item slotids values)
  (do-choices item
    (let ((ptable `#["DomainName" ,domain
		     "ItemName" ,(unparse-arg item)])
	  (count 0))
      (do-choices (key (unparse-arg slotids))
	(do-choices (value (sdb/fromlisp values))
	  (store! ptable (stringout "Attribute." count ".Name")
		  key)
	  (store! ptable (stringout "Attribute." count ".Value")
		  value)
	  (set! count (1+ count))))
      (sdb/op "DeleteAttributes" ptable))))




