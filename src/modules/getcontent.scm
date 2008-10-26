;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'getcontent)

;;; This handles automatic updating of files into environments. 

(define version "$Id: updatefile.scm 1631 2007-08-16 14:12:11Z haase $")

(use-module '{fileio ezrecords varconfig})

(module-export! '{getcontent})

(defrecord (loaded MUTABLE OPAQUE) file modtime (checktime) (content))

(define loadinfo (make-hashtable))

;; The number of seconds to assume that a file hasn't been touched.
;; When debugging, make this small, in production, make it big.
(define filecheck-window 15)
(varconfig! getcontent:delay filecheck-window)

(define (getcontent file (parser #f) (enc #t))
  (try (loaded-content (getcontent/info file))
       (reload-content file parser enc)))

(define (getcontent/info file)
  (let ((info (get loadinfo file)))
    (if (exists? info)
	(if (< (difftime (loaded-checktime info)) filecheck-window)
	    info
	    (let ((modtime (file-modtime file)))
	      (if (equal? modtime (loaded-modtime info))
		  info
		  (fail))))
	(fail))))

(defslambda (reload-content file parser enc)
  (let ((info (getcontent/info file)))
    (if (exists? info)
	(loaded-content info)
	(let* ((mtime (file-modtime file))
	       (data (cond ((not enc) (filedata file))
			   ((string? enc) (filestring file enc))
			   (else (filestring file))))
	       (result (if parser (parser data) data))
	       (info (cons-loaded file mtime (timestamp) result)))
	  (store! loadinfo file info)
	  result))))
