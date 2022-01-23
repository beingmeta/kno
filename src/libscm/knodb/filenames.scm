;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/filenames)

(use-module '{ezrecords text/stringfmts logger texttools})

(module-export! '{knodb/partition-files opt-suffix ;; knodb/file
		  knodb/abspath knodb/relpath
		  knodb/altpath 
		  knodb/same-path?
		  knodb/backup knodb/backup!
		  knodb/install!
		  knodb/bakpath})

;;;; General utilities

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

(define (check-writable path)
  "This creates intermediate directories in path and signal an error if the "
  "terminal directory is not writable"
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

(define (try-file . args)
  "Creates a pathname from *args* and returns it if it exists, failing otherwise"
  (let ((file (apply glom args)))
    (tryif (file-exists? file) file)))

(defambda (knodb/same-path? spec source (dbsuffix #f))
  "Returns true if *spec* and *source* are the same path, extracting the source "
  "reference from database (pool/index) arguments."
  (when (or (pool? spec) (index? spec)) (set! spec (dbctl spec 'source)))
  (when (or (pool? source) (index? source)) (set! source (dbctl source 'source)))
  (or (equal? spec source)
      (equal? (realpath spec) (realpath source))
      (try (try-choices (dbsuffix (or dbsuffix {".pool" ".flexpool" ".index" ".flexindex"}))
	     (let ((specfiles {spec (realpath spec)
			       (tryif (file-directory? spec)
				 (realpath (mkpath spec (glom (dirbase spec) dbsuffix))))})
		   (sourcefiles {source (realpath source)
				 (tryif (file-directory? source)
				   (realpath (mkpath source (glom (dirbase source) dbsuffix))))}))
	       (tryif (exists equal? specfiles sourcefiles) #t)))
	   #f)))

(define (dirbase string) 
  (basename (strip-suffix string "/")))

;;;; OPT-SUFFIX

(define (opt-suffix string suffix)
  "Adds *suffix* to *string* if it doesn't already have one."
  (if suffix
      (if (string-ends-with? string #("." (isalnum+)))
	  string
	  (glom string suffix))
      string))

;; (define (knodb/file prefix (suffix "pool") (simple #f))
;;   (set! prefix
;;     (textsubst prefix 
;; 	       `#("." (opt #((isxdigit+) ".")) ,suffix)
;; 	       ""))
;;   (abspath
;;    (try (try-file prefix ".0000." suffix)
;; 	(try-file prefix ".000." suffix)
;; 	(try-file prefix ".00." suffix)
;; 	(try-file prefix ".0." suffix)
;; 	(tryif simple (try-file prefix "." suffix)))))

;;; Getting partition files

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
  
;;; Getting alternate filenames for temporary and backup paths

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
	 (check-writable
	  (if (has-prefix path "/")
	      (mkpath alt (basename path))
	      (mkpath alt path))))
	((has-prefix alt ".") (check-writable (glom path alt)))
	((position #\/ alt) (check-writable (mkpath alt path)))
	(else (check-writable (glom path  "." alt)))))

(define (knodb/backup path (opts #f) (backup))
  (default! backup
    (if (string? opts) opts
	(getopt opts 'backup
	  (if (getopt opts 'useconfig #t) 
	      (config 'backup ".bak")
	      ".bak"))))
  (when (not (string? backup)) (set! backup ".bak"))
  (when (string? opts) (set! opts #f))
  (local required (getopt opts 'required))
  (cond ((and (not backup) (not required)) #f)
	((overlaps? backup '{none no skip}) (if required ".bak" #f))
	((not (string? backup)) (irritant backup |BadBackupSpec|))
	((overlaps? (downcase backup) '{"remove" "delete" "discard" "drop"}) #f)
	((overlaps? (downcase backup) '{"leave" "inplace"}) path)
	((file-directory? backup)
	 (check-writable
	  (if (has-prefix path "/")
	      (mkpath backup (basename path))
	      (mkpath backup path))))
	((has-prefix backup ".") (check-writable (glom path backup)))
	((position #\/ backup) (check-writable (mkpath backup path)))
	(else (check-writable (glom path  "." backup)))))

(define knodb/bakpath (fcn/alias knodb/backup))

(define (knodb/backup! path . args)
  (local target (apply knodb/backup path args))
  (cond ((or (not target) (overlaps? (downcase target) '{"remove" "delete" "discard" "drop"}))
	 (remove-file path))
	((not (string? target))
	 (logerr |BadBackup| "Invalid target result " (write target)))
	((equal? (realpath target) (realpath path))
	 (logwarn |NoBackup| "Leaving " path " in place"))
	((string? target)
	 (onerror (domove path target)
	     (lambda (ex)
	       (logerr |BackupFailed| "Failed to copy " file " to " target)
	       #f)))
	(else (logerr |BadBackup| "knodb/backup! fell through on " (write target) " for " (write path)))))

(define (domove from to)
  (loginfo |MoveFile| from " ==> " to)
  (move-file! from to)
  ;; (onerror
  ;;     (move-file! from to)    
  ;;     (lambda (x)
  ;; 	(logwarn |RenameFailed|
  ;; 	  "Couldn't rename " form " to " to ", using shell")
  ;; 	  (exec/cmd "mv" from to)))
  to)

(define (knodb/install! source target (opts #f))
  (unless (file-exists? source) (irritant source |MissingSource| knodb/install!))
  (knodb/backup! target opts)
  (domove source target))
