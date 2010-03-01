;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'domutils/localize)

;;; Utilites for manipulating parsed XML in terms of the XHTML DOM
(define version "$Id$")
(define revision "$Revision$")

(use-module '{fdweb texttools domutils logger})

(define %loglevel %notice!)

(module-export! '{dom/localize!})
(module-export! '{dom/getmanifest dom/textmanifest dom/datamanifest})

(define (chkdir filename)
  (if (file-directory? (dirname filename))
      filename
      (begin (system "mkdir -p " (dirname filename))
	     filename)))

(define (localref ref urlmap base target read amalgamate)
  (try (tryif (or (empty-string? ref) (has-prefix ref "#")) ref)
       (tryif (exists has-prefix ref amalgamate)
	 (textsubst ref `(GREEDY ,amalgamate) ""))
       (tryif (position #\# ref)
	 (let ((hashpos (position #\# ref)))
	   (string-append (localref (subseq ref 0 hashpos) urlmap base target read (qc amalgamate))
			  (subseq ref hashpos))))
       (get urlmap ref)
       (if (string-starts-with? ref #((isalpha) (isalpha) (isalpha+) ":"))
	   ref
	   (let ((absref (if (string-starts-with? ref #((isalpha) (isalpha) (isalpha+) ":"))
			     ref
			     (mkpath (dirname base) ref)))
		 (saveto (if (has-suffix target "/") target
			     (string-append target "/"))))
	     ;; (%watch ref base target saveto read)
	     (try (get urlmap absref)
		  (let* ((name (basename (uribase ref)))
			 (content (urlcontent absref))
			 (outfile (chkdir (append saveto ref)))
			 (lref (mkpath read ref)))
		    ;; This has fragments and queries stripped (uribase)
		    ;; and additionally has the 'directory' part of the URI
		    ;; removed so that it's a local file name
		    (loginfo "Downloaded " (write outfile) " from " (write absref) " ref to " lref)
		    (write-file outfile content)
		    (store! urlmap absref lref)
		    lref))))))

(define (dom/localize! dom base write (read) (amalgamate #f))
  (default! read write)
  (let ((urlmap (make-hashtable))
	(amalgamate (or amalgamate {}))
	(files {}))
    (do-choices (node (dom/find dom "img"))
      (let ((ref (localref (get node 'src) urlmap base write read (qc amalgamate))))
	(dom/set! node 'src ref)
	(set+! files ref)))
    (do-choices (node (dom/find dom "link"))
      (let ((ref (localref (get node 'href) urlmap base write read (qc amalgamate))))
	(dom/set! node 'href ref)
	(set+! files ref)))
    (do-choices (node (pick (dom/find dom "script") 'src))
      (let ((ref (localref (get node 'src) urlmap base write read (qc amalgamate))))
	(dom/set! node 'src ref)
	(set+! files ref)))
    (do-choices (node (pick (dom/find dom "a") 'href))
      (let ((ref (localref (get node 'href) urlmap base write read (qc amalgamate))))
	(dom/set! node 'href ref)
	(set+! files ref)))
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
