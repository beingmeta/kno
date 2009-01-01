;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'updatefile)

;;; This handles automatic updating of files into environments. 

(define version "$Id: updatefile.scm 1631 2007-08-16 14:12:11Z haase $")

(use-module 'fileio)

(module-export! '{updatefile updatefiles})

(define (needs-reload? file table)
  (or (fail? (get table file))
      (time-earlier? (get table file) (file-modtime file))))

(define updatefile
  (macro expr
    (let ((filename-expr (get-arg expr 1)))
      `(begin (default! %loadtimes (make-hashtable))
	      (let ((filename ,filename-expr))
		(when (,needs-reload? filename %loadtimes)
		  (load filename)
		  (store! %loadtimes filename (,file-modtime filename))))))))
(define updatefiles
  (macro expr
    (let ((filename-expr (get-arg expr 1)))
      `(begin (default! %loadtimes (make-hashtable))
	      (do-choices (filename (getkeys %loadtimes))
		(when (,needs-reload? filename %loadtimes)
		  (load filename)
		  (store! %loadtimes filename (,file-modtime filename))))))))






