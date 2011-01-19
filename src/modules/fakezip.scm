;;; -*- Mode: Scheme; -*-

(in-module 'fakezip)

;;; Provides access to the Open Library API

(use-module '{fdweb texttools ezrecords logger fileio})

(module-export! '{fz/open fz/add! fz/update! fz/close})

(define %loglevel %notify!)
;;(define %loglevel %debug!)

(defrecord fakezip filename tmpdir tmpzip)

(define (checkdir! path)
  (or (file-directory? path)
      (and (checkdir! (dirname path))
	   (begin (system "mkdir " path) path))))

(define (fz/open filename)
  (let* ((abs (abspath filename))
	 (tmpdir (tempdir (string-append (dirname abs) "tmpzipXXXXXXXX")))
	 (tmpzip (mkpath tmpdir (basename filename))))
    (cons-fakezip (abspath filename) tmpdir tmpzip)))

(define (fz/close fz)
  (write-file (fakezip-filename fz)
	      (filedata (fakezip-tmpzip fz)))
  (system (config 'RM "rm -rf") " " (fakezip-tmpdir fz)))

(define (fz/add! fz path content (compress #t))
  (let ((fspath (mkpath (fakezip-tmpdir fz) path))
	(curdir (getcwd))
	(args (list path)))
    (checkdir! (dirname fspath))
    (fileout fspath content)
    (unwind-protect
	(begin (setcwd (fakezip-tmpdir fz))
	       (forkwait (config 'zipbin "/usr/bin/zip")
			 (fakezip-tmpzip fz)
			 (if compress "-9" "-0")
			 path))
      (setcwd curdir))))

(define (fz/update! fz path content (compress #t))
  (let ((fspath (mkpath (fakezip-tmpdir fz) path))
	(curdir (getcwd))
	(args (list path)))
    (checkpath! fspath)
    (fileout fspath content)
    (unwind-protect
	(begin (setcwd (fakezip-tmpdir fz))
	       (forkwait (config 'zipbin "/usr/bin/zip")
			 (fakezip-tmpzip fz)
			 "-u"
			 (if compress "-9" "-0")
			 path))
      (chdir curdir))))





