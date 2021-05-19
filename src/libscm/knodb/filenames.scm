;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/filenames)

(use-module '{ezrecords text/stringfmts logger texttools})

(module-export! '{knodb/file knodb/partition-files opt-suffix
		  knodb/abspath knodb/relpath})

(define (opt-suffix string suffix)
  (if suffix
      (if (string-ends-with? string #("." (isalnum+)))
	  string
	  (glom string suffix))
      string))

(define (try-file . args)
  (let ((file (apply glom args)))
    (tryif (file-exists? file) file)))

(define (knodb/file prefix (suffix "pool") (simple #t))
  (set! prefix
    (textsubst prefix 
	       `#("." (opt #((isxdigit+) ".")) ,suffix)
	       ""))
  (abspath
   (try (try-file prefix ".0000." suffix)
	(try-file prefix ".000." suffix)
	(try-file prefix ".00." suffix)
	(try-file prefix ".0." suffix)
	(tryif simple (try-file prefix "." suffix)))))

(define (knodb/partition-files prefix (suffix #f))
  (cond ((index? prefix) (index-source (or (indexctl prefix 'partitions) {})))
	((pool? prefix) (index-source (or (poolctl prefix 'partitions) {})))
	((string? prefix)
	 (let* ((stripped (strip-suffix prefix {".flexindex" ".flexpool"}))
		(absprefix (abspath stripped))
		(absroot (strip-suffix absprefix stripped))
		(suffix `#("." (isxdigit+) "." ,(or suffix {"pool" "index"}))))
	   (tryif (file-directory? (dirname absprefix))
	     (strip-prefix
	      (pick (getfiles (dirname absprefix))
		has-prefix absprefix
		string-ends-with? suffix)
	      absroot))))
	(else (irritant prefix |NotAPartitionSpec|))))

(define (knodb/abspath path (root #f))
  (cond ((equal? path ".") (mkpath (or root (getcwd)) ""))
	((equal? path "..") (mkpath (dirname (or root (getcwd))) "/"))
	(root (abspath path root))
	(else (abspath path))))

(define (knodb/relpath path (root #f) (nullval #f))
  (when (and root (not (has-suffix root "/"))) (set! root (glom root "/")))
  (cond ((not root) path)
	((equal? root path) nullval)
	((has-prefix root path) (slice path (length root)))
	(else path)))
