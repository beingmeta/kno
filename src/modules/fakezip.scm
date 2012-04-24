;;; -*- Mode: Scheme; -*-

(in-module 'fakezip)

;;; Provides access to the Open Library API

(use-module '{fdweb texttools ezrecords logger fileio})

(module-export! '{fz/open fz/add! fz/update! fz/close})

(define %loglevel %notify!)
;;(define %loglevel %debug!)

(defrecord fakezip filename tmpdir tmpzip)

(define (mkdir path)
  (if (>= (system "mkdir " path) 0) path
      (begin (sleep 10)
	(if (>= (system "mkdir " path) 0) path
	    (error "MKDIR failed")))))

(define (checkdir! path)
  (and (not (empty-string? path))
       (not (equal? path "/"))
       (not (and (file-exists? path) (not (file-directory? path))))
       (or (file-directory? path)
	   (and (checkdir! (dirname path)) (mkdir path)))))

(define (fz/open filename)
  (let* ((abs (abspath filename))
	 (tmpdir (tempdir (mkpath (dirname abs) "tmpzipXXXXXXXX")))
	 (tmpzip (basename filename)))
    (cons-fakezip (abspath filename) tmpdir tmpzip)))

(define (fz/close fz (cleanup #t))
  (write-file (fakezip-filename fz)
	      (filedata (mkpath (fakezip-tmpdir fz) (fakezip-tmpzip fz))))
  (if cleanup
      (system (config 'RM "rm -rf") " " (fakezip-tmpdir fz))
      (message "Leaving temporary dir " (fakezip-tmpdir fz))))

(define (fz/add! fz path content (compress #t) (extra #f))
  (let ((fspath (mkpath (fakezip-tmpdir fz) path))
	(curdir (getcwd))
	(args (list path))
	(moreopts '()))
    (unless extra (set! moreopts (cons "-X" moreopts)))
    (checkdir! (dirname fspath))
    (write-file fspath content)
    (unwind-protect
	(begin (setcwd (fakezip-tmpdir fz))
	       (apply forkwait
		      (config 'zipbin "/usr/bin/zip")
		      (fakezip-tmpzip fz)
		      (if compress "-9" "-0")
		      (append moreopts (list path))))
      (setcwd curdir))))

(define (fz/update! fz path content (compress #t) (extra #t))
  (let ((fspath (mkpath (fakezip-tmpdir fz) path))
	(curdir (getcwd))
	(args (list path))
	(moreopts '()))
    (unless extra (set! moreopts (cons "-X" moreopts)))
    (checkpath! fspath)
    (write-file fspath content)
    (unwind-protect
	(begin (setcwd (fakezip-tmpdir fz))
	       (apply forkwait
		      (config 'zipbin "/usr/bin/zip")
		      (fakezip-tmpzip fz)
		      "-u"
		      (if compress "-9" "-0")
		      (append moreopts (list path))))
      (chdir curdir))))


