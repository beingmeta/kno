;;; -*- Mode: Scheme -*- 

(in-module 'gnosys/urldb)

;;; The GNOSYS/URLDB module provides a database of URLs, representing
;;; 'parent' relationships between queries, fragments, directories,
;;; and sites.

(define version "$Id$")

(use-module '{fdweb texttools gnosys})

;;; These are the core functions for the URL database.  The function
;;; url2frame generates a frame from a URL.

(module-export! '{url2frame urlpool urlindex url/parents})

(define urldb #f)
(define urlpool #f)
(define urlindex #f)
(define newurlfn #f)

(define (url2frame url (base #f))
  (if (oid? url) url
      (try (find-frames urlindex 'url url)
	   (let* ((parsed (parseuri url base))
		  (canonical (unparseuri parsed)))
	     (try (find-frames urlindex 'url canonical)
		  (newurl url canonical parsed))))))

(define (newurl url canonical parsed)
  (if newurlfn
      (let ((f (newurlfn url canonical parsed)))
	(index-decache urlindex 'url url)
	(index-decache urlindex 'url canonical)
	f)
      (let ((f (create-url canonical parsed)))
	(when (needs-init? f) (initialize-url f))
	f)))

(define needs-init?
  (slambda (url)
    (if (exists? (get url 'site)) #f
	(begin (add! url 'site 'computing)
	       #t))))
  
(define create-url
  (slambda (url parsed)
    (try (find-frames urlindex 'url url)
	 (let ((frame (allocate-oids urlpool)))
	   (set-oid-value! frame parsed)
	   (add! frame '%created (gmtimestamp))
	   (add! frame 'type 'url)
	   (add! frame '%id (unparseuri parsed #t))
	   (add! frame 'url url)
	   (index-frame urlindex frame '{url host name})
	   frame))))

(define (make-parent scheme host port path)
  (stringout scheme "://" host ":" port "/"
	     (dolist (elt path) (printout elt "/"))))

(define (initialize-site f)
  (if (equal? (get f 'port) 80)
      (let* ((scheme (get f 'scheme))
	     (hostname (get f 'hostname))
	     (dot (position #\. hostname)))
	(if (and (exists? dot) dot
		 (position #\. hostname (1+ dot)))
	    (let* ((newhost (subseq hostname (1+ dot)))
		   (parent (url2frame (make-parent scheme newhost 80 '()))))
	      (add! f @?gn/parent parent)
	      f)
	    f))
      f))

(define (url/parents url)
  (choice (get url @?gn/parent) (url/parents (get url @?gn/parent))))

(define (initialize-url f)
  (let ((scheme (get f 'scheme))
	(host (get f 'hostname))
	(port (try (get f 'port) 80))
	(path (get f 'path))
	(name (get f 'name)))
    (if (and (null? path) (equal? name ""))
	(begin (store! f 'site f)
	       (if (fail? (get f @?gn/parent)) (initialize-site f)
		   f))
	(let ((parent
	       (url2frame
		(if (equal? name "")
		    (make-parent scheme host port (subseq path 0 -1))
		    (make-parent scheme host port path)))))
	  (add! f @?gn/parent parent)
	  (store! f 'site (get parent 'site))
	  (index-frame urlindex f '{site parent})
	  (index-frame urlindex f 'parents (url/parents f))
	  f))))

(define urldb-config
  (slambda (var (val))
    (cond ((not (bound? val)) urldb)
	  ((equal? urldb val))
	  (else (when urldb (warning "URLDB already set to " urldb))
		(message "Setting URL db to " val)
		(set! urlpool (use-pool val))
		(set! urlindex (open-index val))
		(set! urldb val)
		(when (position #\@ val)
		  (set! newurlfn (dtproc 'create-url val)))))))
(config-def! 'urldb urldb-config)

;;;; URL rewrites

;;; This implements rule-based rewriting of URLs.

(module-export! '{rewrite-url})

(unless (bound? url-rewrites)
  (define url-rewrites '()))

(define (rewrite-url url (rules #f))
  (apply-rewrites (or rules url-rewrites) url))

(define (urlrewrites-config var val)
  (set! url-rewrites (cons val url-rewrites)))

(config-def! 'urlrewrites urlrewrites-config)

(define (apply-rewrite rule url)
  (let ((pos (textsearch rule url)))
    (if pos
	(let ((endpos (textmatcher rule url pos)))
	  (append (subseq url 0 pos)
		  (textrewrite rule url pos)
		  (subseq url endpos)))
	(fail))))
(define (apply-rewrites rules url)
  (if (null? rules) url
      (let ((newurl (apply-rewrite (car rules) url)))
	(apply-rewrites (cdr rules) (try newurl url)))))

;;;; Scoring URLs for similarity

;; This scores other URLs based on common parents and sites.

(define (scorecorpus/url scores url)
  (let ((scores (or scores (make-hashtable))))
    (hashtable-increment! scores
      (find-frames urlindex 'site (get url 'site)))
    (hashtable-increment! scores
      (find-frames urlindex 'parents (url/parents url)))
    scores))

(module-export! '{scorecorpus/url})
