;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{fileio logger reflection varconfig})

(define (test-indexfile-deletion (fname "deltest.index"))
  (unless (file-exists? fname)
    (make-index fname #[type kindex slots 5000]))
  (let ((open1 (open-index fname)))
    (remove-file fname)
    (let ((newmade (make-index fname #[type kindex slots 500]))
	  (open2 (open-index fname))
	  (filesize (file-size fname)))
      (store! open2 "precious" 42)
      (commit open2)
      (applytest #t > (file-size fname) filesize))))


(define (main)
  (test-indexfile-deletion))

