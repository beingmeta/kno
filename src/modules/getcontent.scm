;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'getcontent)

;;; This handles automatic updating of the content of files
;;; It is a more flexible version of load-latest

(define version "$Id: updatefile.scm 1631 2007-08-16 14:12:11Z haase $")

(use-module '{fileio ezrecords varconfig logger reflection})

(module-export! '{getcontent getcontent/info})

;; The number of seconds to assume that a file hasn't been touched.
;; When debugging, make this small, in production, make it big.
(define filecheck-default-lag 2) ;; 15
(varconfig! getcontent:lag filecheck-default-lag)

(define (identity x) x)

(defrecord (loadinfo MUTABLE OPAQUE)
  file modtime
  (lagtime filecheck-default-lag)
  (encoding #t) (parser identity) (checktime) (content))

(define loadinfo (make-hashtable))

;;; Configuring file-specific lags

(define filecheck-lags (make-hashtable))

(config-def! 'getcontent:lags
	     (lambda (var (val))
	       (if (bound? val)
		   (if (not (and (pair? val) (string? (car val))
				 (or (not (cdr val)) (number? (cdr val)))))
		       (error "Invalid lag specification")
		       (if (cdr val)
			   (begin (store! filecheck-lags (car val) (cdr val))
				  (update-filecheck-lag (get loadinfo (car val)) (cdr val)))
			   (drop! filecheck-lags (car val))))
		   filecheck-lags)))

(define (update-filecheck-lag info lag)
  (let ((file (loadinfo-file info))
	(modtime (loadinfo-modtime info))
	(parser (loadinfo-parser info))
	(enc (loadinfo-encoding info))
	(checktime (loadinfo-checktime info))
	(content (loadinfo-content info)))
    (store! loadinfo (loadinfo-file info)
	    (cons-loadinfo file modtime lag enc parser checktime content))))

;;; The main event

(define (getcontent file (enc #t) (parser #f))
  (try (loadinfo-content (getcontent/info file enc parser))
       (reload-content file enc (or parser identity))))

(define (getcontent/info file (enc #f) (parser #f))
  (let ((info (get loadinfo file)))
    (if (exists? info)
	(if (< (difftime (loadinfo-checktime info)) (loadinfo-lagtime info))
	    info
	    (let ((modtime (file-modtime file)))
	      (if (and (or (not parser) (equal? (or parser identity)
						(loadinfo-parser info)))
		       (or (not enc) (equal? (or enc #t)
					     (loadinfo-encoding info)))
		       (equal? modtime (loadinfo-modtime info)))
		  info
		  (fail))))
	(fail))))

(defslambda (reload-content file enc parser)
  (let ((info (get loadinfo file))
	(mtime (file-modtime file)))
    ;; (%watch "RELOAD-CONTENT test" file info)
    (if (and (exists? info)
	     (equal? mtime (loadinfo-modtime info))
	     (equal? parser (loadinfo-parser info))
	     (equal? enc (loadinfo-encoding info)))
	(loadinfo-content info)
	(let* ((lagtime (try (get filecheck-lags file)
			     (get filecheck-lags (abspath file))
			     (get filecheck-lags (realpath file))
			     filecheck-default-lag))
	       (parser (if parser
			   (if (equal? parser (loadinfo-parser info)) parser
			       (begin (logwarn
				       "Changing parse function for " file
				       " to " parser
				       " from " (loadinfo-parser info))
				      parser))
			   (loadinfo-parser info)))
	       (enc (if enc
			(if (equal? enc (loadinfo-encoding info)) enc
			    (begin (logwarn "Changing text encoding for " file
					    " to " enc
					    " from " (loadinfo-encoding info))
				   enc))
			(loadinfo-encoding info)))
	       (data (cond ((not enc) (filedata file))
			   ((string? enc) (filestring file enc))
			   (else (filestring file))))
	       (result (if (and parser (not (eq? parser identity)))
			   (parser data)
			   data))
	       (newinfo (cons-loadinfo file mtime lagtime
				       enc parser (timestamp) result)))
	  (when (exists? info)
	    (notify "Reloaded content from " (write file)
		    " using " (or (procedure-name parser) parser)))
	  ;; (%watch "RELOAD-CONTENT newinfo" file newinfo)
	  (store! loadinfo file newinfo)
	  result))))







