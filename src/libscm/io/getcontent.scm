;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc. All rights reserved.
;;; Copyright (C) 2020-2021 beingmeta, llc.

(in-module 'io/getcontent)

;;; This handles automatic updating of the content of files
;;; It is a more flexible version of load-latest

(use-module '{fileio ezrecords varconfig logger reflection})

(define-init %loglevel %notice%)

(module-export! '{getcontent getcontent/info})

;; The number of seconds to assume that a file hasn't been touched.
;; When debugging, make this small, in production, make it big.
(define filecheck-default-lag 15) ;; 15
(varconfig! getcontent:lag filecheck-default-lag)

(define (identity x) x)

(defrecord (loadinfo MUTABLE OPAQUE)
  file modtime
  (lagtime filecheck-default-lag)
  (encoding #t) (parser identity) (checktime) (content))

(define loadinfo (make-hashtable))

;;; Configuring file-specific lags

(define-init filecheck-lags (make-hashtable))

(config-def! 'getcontent:lags
	     (lambda (var (val))
	       (cond ((not (bound? val)) filecheck-lags)
		     ((eq? filecheck-lags val) val)
		     ((hashtable? val) (set! filecheck-lags val) val)
		     ((and (pair? val) (string? (car val))
			   (number? (cdr val)))
		      (store! filecheck-lags (car val) (cdr val))
		      (update-filecheck-lag (get loadinfo (car val)) (cdr val))
		      (cdr val))
		     ((and (pair? val) (string? (car val))
			   (or (not (cdr val))  (null? (cdr val))))
		      (drop! filecheck-lags (car val))
		      #f)
		     (else (error "Invalid lag specification")))))

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
       (reload-content file enc parser)))

(define (getcontent/info file (enc #f) (parser #f))
  (let ((info (get loadinfo (vector file (or enc 'binary) parser))))
    (if (exists? info)
	(if (< (time-since (loadinfo-checktime info)) (loadinfo-lagtime info))
	    info
	    (let ((modtime (file-modtime file)))
	      (if (and (or (not parser)
			   (equal? parser (loadinfo-parser info)))
		       (or (not enc)
			   (equal? (or enc 'binary) (loadinfo-encoding info)))
		       (equal? modtime (loadinfo-modtime info)))
		  info
		  (fail))))
	(fail))))

(defslambda (reload-content file enc parser)
  (let ((info (try (get loadinfo (vector file (or enc 'binary) parser))
		   (get loadinfo file)))
	(mtime (file-modtime file)))
    (debug%watch "RELOAD-CONTENT" file enc parser info)
    (if (and (exists? info)
	     (equal? mtime (loadinfo-modtime info))
	     (equal? parser (loadinfo-parser info))
	     (equal? (or enc 'binary) (loadinfo-encoding info)))
	(debug%watch (loadinfo-content info) info)
	(let* ((lagtime (try (get filecheck-lags file)
			     (get filecheck-lags (abspath file))
			     (get filecheck-lags (realpath file))
			     filecheck-default-lag))
	       (parser (try (or parser (loadinfo-parser info)) #f))
	       (enc (try (or enc (loadinfo-encoding info)) #f))
	       (data (cond ((applicable? enc) (enc file))
			   ((or (not enc) (eq? enc 'binary)) (filedata file))
			   ((string? enc) (filestring file enc))
			   ;; This is UTF-8
			   (else (filecontent file))))
	       (result (if parser (parser data) data))
	       (newinfo (cons-loadinfo file mtime lagtime
				       enc parser (timestamp) result)))
	  (when (exists? info)
	    (if parser
		(notify "Reloaded content from " (write file)
			" using " (or (procedure-name parser) parser))
		(notify "Reloaded content from " (write file))))
	  (debug%watch "RELOAD-CONTENT newinfo" file newinfo)
	  (store! loadinfo (vector file enc parser) newinfo)
	  (when (or (fail? info) (test loadinfo file info))
	    (store! loadinfo file newinfo))
	  result))))








