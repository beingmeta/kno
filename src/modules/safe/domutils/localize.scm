;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'domutils/localize)

;;; Utilites for manipulating parsed XML in terms of the XHTML DOM
(define version "$Id$")
(define revision "$Revision$")

(use-module '{fdweb texttools domutils logger})

(define havezip #f)

(when (get-module 'ziptools)
  (use-module 'ziptools)
  (set! havezip #t))

(define %loglevel %notice!)

(module-export! '{dom/localize!})
(module-export! '{dom/getmanifest dom/textmanifest dom/datamanifest})

(define (chkdir filename)
  (if (file-directory? (dirname filename))
      filename
      (begin (system "mkdir -p " (dirname filename))
	     filename)))

(define absurlstart
  (choice #((isalpha) (isalpha) (isalpha+) ":") "/"))

;; Converts a URL into a local reference, copying its data if needed
;; BASE is the URL of the target document (making the reference)
;; TARGET is the where downloaded data should be stored locally
;; READ is subdir of base where things should be stored
(define (localref ref urlmap base saveto read amalgamate localhosts)
  (try ;; relative references are untouched
       (tryif (or (empty-string? ref) (has-prefix ref "#")) ref)
       ;; if we're gluing a bunch of files together (amalgamating them),
       ;;  the ref will just be move to the current file
       (tryif (exists has-prefix ref amalgamate)
	 (textsubst ref `(GREEDY ,amalgamate) ""))
       ;; If it's got a fragment identifer, make a localref without the
       ;;  fragment and put the fragment back.  We don't bother checking
       ;;; fragment ID uniqueness
       (tryif (position #\# ref)
	 (let ((hashpos (position #\# ref)))
	   (string-append (localref (subseq ref 0 hashpos)
				    urlmap base saveto read
				    (qc amalgamate) (qc localhosts))
			  (subseq ref hashpos))))
       ;; Check the cache
       (get urlmap ref)
       ;; don't bother localizing these references
       (tryif (exists string-starts-with? ref localhosts) ref)
       ;; No easy outs, fetch the content and store it
       (let ((absref
	      (if (string-starts-with? ref absurlstart) ref
		  (mkpath (dirname base) ref))))
	 ;; (%watch ref base target saveto read)
	 (try (get urlmap absref)
	      (let* ((name (basename (uribase ref)))
		     (content (urlcontent absref))
		     (lref (mkpath read name)))
		;; This has fragments and queries stripped (uribase)
		;; and additionally has the 'directory' part of the URI
		;; removed so that it's a local file name
		(loginfo "Downloaded " (write absref) " for " lref)
		(save-content saveto lref content)
		(store! urlmap absref lref)
		lref)))))

;; This will avoid optimization warnings if we don't have ziptools
(define save-content
  (if havezip
      (lambda (saveto lref content)
	(cond ((string? saveto)
	       (write-file (mkpath saveto lref) content))
	      ((zipfile? saveto)
	       (zip/add! saveto lref content))
	      ((and (pair? saveto)
		    (zipfile? (car saveto))
		    (string? (cdr saveto)))
	       (zip/add! saveto (mkpath (cdr saveto) lref) content))
	      (else (error "Bad SAVE-CONTENT call"))))
      (lambda (saveto lref content)
	(cond ((string? saveto)
	       (write-file (mkpath saveto lref) content))
	      (else (error "Bad SAVE-CONTENT call"))))))

(define (dom/localize! dom base write
		       (read) (amalgamate #f) (localhosts #f)
		       (doanchors #f))
  (default! read write)
  (let ((urlmap (try (get dom 'urlmap)  (make-hashtable)))
	(amalgamate (or amalgamate {}))
	(localhosts (or localhosts {}))
	(files {}))
    (do-choices (node (dom/find dom "img"))
      (let ((ref (localref (get node 'src)
			   urlmap base write read
			   (qc amalgamate) (qc localhosts))))
	(dom/set! node 'src ref)
	(set+! files ref)))
    (do-choices (node (pick (dom/find dom "link") 'rel "stylesheet"))
      (let ((ref (localref (get node 'href)
			   urlmap base write read
			   (qc amalgamate) (qc localhosts))))
	(dom/set! node 'href ref)
	(set+! files ref)))
    (do-choices (node (pick (dom/find dom "script") 'src))
      (let ((ref (localref (get node 'src)
			   urlmap base write read
			   (qc amalgamate) (qc localhosts))))
	(dom/set! node 'src ref)
	(set+! files ref)))
    (when doanchors
      (do-choices (node (pick (dom/find dom "a") 'href))
	(let ((ref (localref (get node 'href) urlmap base write read
			     (qc amalgamate) (qc localhosts))))
	  (dom/set! node 'href ref)
	  (set+! files ref))))
    (store! dom 'manifest files)
    (store! dom 'resources write)))

;;;; Manifests

(define (dom/getmanifest doc)
  (choice (for-choices (link (dom/find doc "LINK"))
	    (tryif (or (test link 'rel "css") (test link 'rel "knowlet"))
	      (get link 'href)))
	  (get (dom/find doc "SCRIPT") 'src)
	  (get (dom/find doc "IMG") 'src)))

(defambda (dom/textmanifest node (staticrefs {}) (dynamicrefs {}))
  "Generates a text manifest for a document, with additional static or dynamic refs"
  (let* ((urls (dom/getmanifest node))
	 (static (reject urls string-contains? "?"))
	 (dynamic (pick urls string-contains? "?"))
	 (given (choice staticrefs dynamicrefs)))
    (stringout "CACHE MANIFEST\n"
	       (do-choices (url (choice staticrefs (difference static given)))
		 (lineout url))
	       (when (or (exists? dynamic) (exists? dynamicrefs))
		 (lineout "NETWORK:")
		 (do-choices (url (choice dynamicrefs (difference dynamic given)))
		   (lineout url))))))

(defambda (dom/datamanifest node (staticrefs {}) (dynamicrefs {}))
  "Generates a data: URI containing a manifest.  Unfortunately, this \
   doesn't seem to work in the browsers I've tested, for all it's \
   cleverness."
  (let* ((manifest (dom/textmanifest node staticrefs dynamicrefs))
	 (packet (string->packet manifest))
	 (base64 (packet->base64 packet)))
    (string-append
     "data:text/cache-manifest;charset=\"utf-8\";base64,"
     base64)))
