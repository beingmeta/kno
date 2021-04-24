;;; -*- Mode: Scheme; -*-

(in-module 'kno/statefiles)

(use-module '{logger varconfig binio})

(define %loglevel %notice%)

(module-export! '{statefile/read statefile/save!})

(define default-format #f)
(varconfig! kno/state:format default-format)

(define (statefile/read file (opts #f) (format) (generator))
  (when (overlaps? opts '{xtype dtype lisp})
    (set! format opts)
    (set! opts #f))
  (default! format (getopt opts 'format (guess-format file)))
  (default! generator (getopt opts 'generator #f))
  (let* ((state (read-file file opts format generator))
	 (statefile (get state 'statefile)))
    (store! 'state 'readtime (timestamp))
    (when (exists? statefile)
      (unless (overlaps? {file (realpath file) (abspath file)}
			 {statefile (realpath statefile) (abspath statefile)})
	(logwarn |RelocatedStatefile|
	  "Statefile " (write file) " was written as "
	  (write (get state 'statefile)))))
    (store! state 'statefile (abspath statefile))
    state))

(define (read-file file opts format generator)
  (cond ((and (not (file-exists? file)) generator (thunk? generator))
	 (generator))
	((not (file-exists? file)) (getopt opts 'init #[]))
	((eq? format 'lisp) (read (open-input-file file)))
	((eq? format 'xtype) (read-xtype file))
	((eq? format 'dtype) (file->dtype file))
	(else (irritant file |UnspecifiedFormat|))))

(define (guess-format file)
  (cond ((has-suffix file {".scm" ".lsp" ".lsd" ".txt"}) 'lisp)
	((has-suffix file ".xtype") 'xtype)
	((has-suffix file ".dtype") 'dtype)
	((file-exists? file)
	 (let* ((in (open-byte-input file))
		(c1 (read-byte in))
		(c2 (read-byte in))
		(c3 (read-byte in)))
	   (cond ((= c1 0xa2) 'xtype)
		 ((or (= c1 0x5b) (and  (= c1 0x23) (= c2 0x5b))) 'lisp)
		 ((and (= c1 0x42)
		       (or (= c2 0xc1) (= c2 0x81)
			   (= c2 0xc5) (= c2 0x85)))
		  'dtype)
		 (else (irritant file |UnrecognizedFormat|)))))
	((overlaps? default-format '{lisp xtype dtype}) default-format)
	(default-format (irritant default-format |InvalidDefaultFormat|))
	(else #f)))

(define (statefile/save! state (opts #f) (file) (format))
  (when (and (string? opts) (bound? file))
    (let ((temp opts))
      (set! opts file)
      (set! file temp)))
  (default! file (try (get state 'statefile) (getopt opts 'statefile)))
  (default! format
    (getopt opts 'newformat
	    (if (file-exists? file) (guess-format file)
		(getopt opts 'useformat (guess-format file)))))
  (let ((copy (deep-copy state))
	(tempfile (getopt opts 'tempfile (glom file ".part")))
	(backup (getopt opts 'backup (glom file ".bak"))))
    (store! copy 'statefile (abspath file))
    (store! copy 'saved (timestamp))
    (cond ((eq? format 'lisp) (listdata copy #f (open-output-file tempfile)))
	  ((eq? format 'xtype) (write-xtype copy tempfile))
	  ((eq? format 'dtype) (dtype->file copy tempfile))
	  (else (listdata copy #f (open-output-file tempfile))))
    (when backup (system "cp " tempfile file backup))
    (unless (equal? tempfile file) (move-file tempfile file))))


