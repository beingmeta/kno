;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/filenames)

(use-module '{ezrecords text/stringfmts logger texttools})

(module-export! '{knodb/file knodb/partition-files opt-suffix
		  knodb/abspath knodb/relpath
		  knodb/altpath knodb/bakpath})

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

(define (make-writable path)
  (if (file-writable? path)
      path
      (if (file-exists? path)
	  (irritant path |NotWritable|)
	  (if (file-directory? (dirname path))
	      (if (file-writable? (dirname path))
		  path
		  (irritant path |DirectoryNotWritable|))
	      (begin (mkdirs path)
		(if (file-writable? (dirname path))
		    path
		    (irritant path |DirectoryNotWritable|)))))))
  
(define-init alt-types
  #[backup ".bak"
    bak ".bak"
    partial ".part"
    part ".part"])

(define (knodb/altpath path pathtype (opts #f) (alt))
  (default! alt
    (getopt opts pathtype
	    (if (getopt opts 'useconfig #t) 
		(config pathtype
			(try (get alt-types pathtype) (glom "." pathtype)))
		(try (get alt-types pathtype) (glom "." pathtype)))))
  (cond ((not alt) #f)
	((overlaps? alt '{none no skip}) #f)
	((not (string? alt)) (irritant alt |BadAltSpec| "For " pathtype))
	((overlaps? (downcase alt) {"none" "no" "skip"}) #f)
	((file-directory? alt)
	 (make-writable
	  (if (has-prefix path "/")
	      (mkpath alt (basename path))
	      (mkpath alt path))))
	((has-prefix alt ".") (make-writable (glom path alt)))
	((position #\/ alt) (make-writable (mkpath alt path)))
	(else (make-writable (glom path  "." alt)))))

(define (knodb/bakpath path (opts #f) (backup))
  (default! backup
    (getopt opts 'backup (if (getopt opts 'useconfig #t) 
			     (config 'backup ".bak")
			     ".bak")))
  (cond ((not backup) #f)
	((overlaps? backup '{none no skip}) #f)
	((not (string? backup)) (irritant backup |BadBackupSpec|))
	((overlaps? (downcase backup) {"none" "no" "skip"}) #f)
	((file-directory? backup)
	 (make-writable
	  (if (has-prefix path "/")
	      (mkpath backup (basename path))
	      (mkpath backup path))))
	((has-prefix backup ".") (make-writable (glom path backup)))
	((position #\/ backup) (make-writable (mkpath backup path)))
	(else (make-writable (glom path  "." backup)))))
