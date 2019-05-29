;;; -*- Mode: Scheme -*- 

;;; The GNOSYS/DOCDB module represents documents, their components,
;;; and their analysis.  Documents have sources, which are typically
;;; URLs and may be divided into passages which are generally
;;; paragraph-sized.

;;; This module provides the function MAKEDOC which takes content
;;; (either a string or a packet), a mime type, and an optional
;;; source.

;;; Document parsing dispatches on mime type, so that the module
;;; gnosys/analyzers/<major>/<minor> defines an analyze function for
;;; documents with mime type major/minor.  The HTML analyzer, for
;;; instance, lives in gnosys/analyzers/text/html.  This uses the
;;; function MAKEPASSAGE to create the component passages.  Documents
;;; and passages can be located in separate pools or indices.

(in-module 'gnosys/docdb)

(use-module '{fdweb texttools})
(use-module '{gnosys gnosys/urldb gnosys/analyze})

(module-export! '{makedoc
		  fetchdoc
		  analyzedoc
		  makepassage
		  url2doc
		  passagepool passageindex
		  docpool docindex})

;;; The GNOSYS/DOCDB modules uses potentially separate databases for
;;; creating and indexing documents and passages.  These can be
;;; specified by config functions.

(define docdb #f)
(define docpool #f)
(define docindex #f)
(define docindices {})

(define passagedb #f)
(define passagepool #f)
(define passageindex #f)
(define passageindices {})

(define docdb-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) docdb)
	  ((equal? docdb val))
	  (else (message "Setting DOCUMENT db to " val)
		(set! docpool (use-pool val))
		(set! docindex (open-index val))
		(set! docdb val)))))
(config-def! 'docdb docdb-config)

(define passagedb-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) passagedb)
	  ((equal? passagedb val))
	  (else (message "Setting FRAGMENT db to " val)
		(set! passagepool (use-pool val))
		(set! passageindex (open-index val))
		(set! passagedb val)))))
(config-def! 'passagedb passagedb-config)

;;; Analyzing documents

(default! analyzers (make-hashtable))

(define (get-analyzer mime-type)
  (try (get analyzers mime-type)
       (let* ((module-name (append "gnosys/analyzers/" (downcase mime-type)))
	      (module (get-module (string->lisp module-name))))
	 (if (exists? module)
	     (let ((analyzer (get module 'analyzer)))
	       (when (exists? analyzer)
		 (store! analyzers mime-type analyzer))
	       analyzer)))))

(define (url2doc url)
  (if (string? url)
      (url2doc (url2frame url))
      (try (find-frames docindex @?doc/source url)
	   (analyzedoc (makedoc url))
	   (find-frames docindex @?doc/source url))))

(define (getdoc url content mimetype)
  (let ((hash (md5 content)))
    (try (pick (find-frames docindex @?doc/source url
			    'type mimetype)
	       'md5 md5)
	 (let ((f (frame-create docpool
		    '%id (stringout "[" (pick-one (get url 'url)) "]")
		    'type 'document 'type mimetype
		    @?doc/source url
		    'md5 hash
		    'content content)))
	   (index-frame docindex f @?doc/source url)
	   (index-frame docindex f 'type)
	   f))))

(define makedoc
  (slambda (url (content #f) (mimetype #f) (user #f))
    (if content
	;; If you're provided with content, handle caching
	(let ((doc (getdoc url content mimetype)))
	  (add! doc 'users user)
	  doc)
	(try (find-frames docindex
	       @?doc/source url
	       (if user (choice user 'anyone) 'anyone))
	     (let ((doc (frame-create docpool
			  '%id (stringout "[" (pick-one (get url 'url)) "]")
			  'type 'document @?doc/source url)))
	       (index-frame docindex doc @?doc/source url)
	       (index-frame docindex doc 'type)
	       doc)))))

(define makedoc
  (slambda (url)
    (if (exists? (find-frames docindex @?doc/source url))
	(fail)
	(let ((f (frame-create docpool
		   '%id (stringout "[" (pick-one (get url 'url)) "]")
		   'type 'document @?doc/source url)))
	  (index-frame docindex f @?doc/source url)
	  f))))

;;; Content fetching

(define startfetch
  (slambda (doc)
    (if (or (test doc 'status 'fetched)
	    (test doc 'status 'fetching))
	#f
	(begin (add! doc 'status 'fetching)
	       #t))))

(define (fetchdoc doc)
  (if (startfetch doc)
      (fetchdoc-inner doc)
      doc))

(define (fetchdoc-inner doc)
  (let* ((url (pick-one (get doc @?doc/source)))
	 (clock-start (elapsed-time))
	 (fetchdata (urlget (pick-one (get url 'url))))
	 (clock-fetch (elapsed-time)))
    (when (and (number? trace-analysis) (> trace-analysis 1))
      (message "Fetched @" (elapsed-time) " fetched " doc " in "
	       (secs->string (- clock-fetch clock-start))))
    (if (or (fail? (get fetchdata '%content))
	    (zero? (length (get fetchdata '%content))))
	(begin (add! doc 'type 'failed-fetch)
	       (add! doc 'fetchdata fetchdata)
	       (add! doc 'status 'fetched)
	       (drop! doc 'status 'fetching)
	       doc)
	(let* ((mimetype (pick (get fetchdata 'type) string?))
	       (content (get fetchdata '%content))
	       (timing-info (try (get doc '%timing-info)
				 (frame-create #f))))
	  (add! doc '%timing timing-info)
	  (add! doc 'type mimetype)
	  (add! doc 'content content)
	  (drop! doc 'status 'fetching)
	  (add! doc 'status 'fetched)
	  (store! timing-info 'fetch (- clock-fetch clock-start))
	  doc))))

;;; Analysis

(define startanalyze
  (slambda (doc)
    (if (or (test doc 'status 'analyzed)
	    (test doc 'status 'analyzing))
	#f
	(begin (add! doc 'status 'analyzing)
	       #t))))

(define (analyzedoc doc)
  (if (startanalyze doc)
      (analyzedoc-inner doc)
      doc))

(define (analyzedoc-inner doc)
  (when trace-analysis
    (message "@" (elapsed-time) " Analyzing " doc))
  (unless (exists? (get doc 'content)) (fetchdoc doc))
  (let* ((mimetype (pick (get doc 'type) string?))
	 (content (get doc 'content))
	 (analyzer (get-analyzer mimetype))
	 (timing-info (try (get doc '%timing) (frame-create #f))))
    (add! doc '%timing timing-info)
    (when (exists? analyzer)
      (let ((clock-start (elapsed-time))
	    (clock-slice #f)
	    (clock-analyze #f))
	(analyzer doc)
	(set! clock-slice (elapsed-time))
	(store! timing-info 'slice (- clock-slice clock-start))
	(when (and (number? trace-analysis) (> trace-analysis 1))
	  (message "Sliced @" (elapsed-time) " sliced " doc " in "
		   (secs->string (- clock-slice clock-start))))
	(when (exists? (get doc @?doc/passages))
	  (analyze-doc doc docindex (or passageindex docindex)))
	(set! clock-analyze (elapsed-time))
	(when (and (number? trace-analysis) (> trace-analysis 1))
	  (message "Analyzed @" (elapsed-time) " analyzed " doc " in "
		   (secs->string (- clock-analyze clock-slice))))
	(store! timing-info 'analyze (- clock-analyze clock-slice))))
    (when trace-analysis
      (message "@" (elapsed-time) " Analyzed " doc "\n\t" (get doc '%timing)))
    (drop! doc 'status 'analyzing)
    (add! doc 'status 'analyzed)
    doc))

;;; Passages

(define passage-cache (make-hashtable))

(define newpassage
  (slambda (content)
    (try (get passage-cache content)
	 (let ((f (frame-create (or passagepool docpool)
		    'type 'passage 'text content)))
	   (add! passage-cache content f)
	   f))))

(define (makepassage content (id #f) (parent #f))
  (try (get passage-cache content)
       (let ((f (newpassage content)))
	 (if id (add! f '%id id) (add! f '%id 'passage))
	 (when parent
	   (add! f @?gn/parent parent)
	   (index-frame docindex f @?gn/parent parent))
	 f)))




