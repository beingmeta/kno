;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc. All rights reserved.

(in-module 'runtemp)

(use-module '{logger})

(module-export! '{runtemp/init! runtemp/root})

(define runtemp/root #f)

(define runtemp/init!
  (slambda ()
    (unless runtemp/root
      (let ((tempname (runfile ".tmp")))
	(when (or (file-exists? tempname) 
		  (and (readlink tempname)
		       (file-exists? (readlink tempname))))
	  (let* ((realtemp (realpath tempname))
		 (existing (file-directory? realtemp))
		 (tempdirs (and existing (getdirs realtemp))))
	    (if (exists? tempdirs)
		(logwarn |RUNTEMP/cleanup|
		  "Removing " (choice-size tempdirs)
		  " leftover temp directories under " realtemp)
		(logwarn |RUNTEMP/cleanup|
		  "Removing leftover tempdir " realtemp))
	    (when existing (remove-tree realtemp))
	    (remove-file tempname)))
	(when (and (readlink tempname) (not (file-exists? (readlink tempname))))
	  (remove-file tempname))
	(let* ((dir (tempdir))
	       (appid (config 'appid))
	       (tmptemp (mkpath dir (glom appid "-XXXXXXX"))))
	  (config! 'temproot tmptemp)
	  (set! runtemp/root dir)
	  (link-file dir tempname)
	  (config! 'atexit (lambda () (remove-file tempname)))
	  (lognotice |RUNTEMP/init|
	    "New tempfile template " tmptemp ", "
	    dir " linked to " tempname))))))






