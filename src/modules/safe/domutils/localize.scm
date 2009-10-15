;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'domutils/localize)

;;; Utilites for manipulating parsed XML in terms of the XHTML DOM
(define version "$Id: domutils.scm 4444 2009-10-13 02:18:26Z haase $")
(define revision "$Revision: 4444 $")

(use-module '{fdweb texttools domutils logger})

(define %loglevel %notice!)

(module-export! '{dom/localize!})

(define (chkdir filename)
  (if (file-directory? (dirname filename))
      filename
      (begin (system "mkdir -p " (dirname filename))
	     filename)))

(define (localref ref urlmap base target read)
  (try (get urlmap ref)
       (let ((absref (if (string-starts-with? ref #((isalpha) (isalpha) (isalpha+) ":"))
			 ref
			 (mkpath (dirname base) ref)))
	     (saveto (if (has-suffix target "/") target
			 (string-append target "/"))))
	 ;; (%watch ref base target saveto read)
	 (try (get urlmap absref)
	      (let* ((name (basename (uribase ref)))
		     (content (urlcontent absref))
		     (outfile (chkdir (append saveto ref))))
		;; This has fragments and queries stripped (uribase)
		;; and additionally has the 'directory' part of the URI
		;; removed so that it's a local file name
		(loginfo "Downloaded " (write outfile) " from " (write absref))
		(write-file outfile content)
		(store! urlmap absref outfile)
		(mkpath read ref))))))

(define (dom/localize! dom base write (read))
  (default! read write)
  (let ((urlmap (make-hashtable)))
    (do-choices (node (dom/find dom "img"))
      (dom/set! node 'src (localref (get node 'src) urlmap base write read)))
    (do-choices (node (dom/find dom "link"))
      (dom/set! node 'href
		(localref (get node 'href) urlmap base write read)))
    (do-choices (node (pick (dom/find dom "script") 'src))
      (dom/set! node 'href
		(localref (get node 'src) urlmap base write read)))))

