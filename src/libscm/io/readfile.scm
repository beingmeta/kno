;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
Copyright (C) 2020-2021 beingmeta, LLC

(in-module 'io/readfile)

(use-module '{binio})

(module-export! '{readfile})

(define (readfile file)
  (and (file-exists? file)
       (let ((header (onerror (read-bytes (open-dtype-input file) 4) #f)))
	 (if (and header
		  (or (< (elt header 0) 0x20)
		      (and (> (elt header 0) 0x80)
			   (<= (elt header 0) 0xc0))
		      (and (= (elt header 0) 0x42) (>= (elt header 1) 0x80 ))
		      (and (= (elt header 0) 0x41)
			   (or (< (elt header 1) 0x10)
			       (>= 0xbF (elt header 1) 0x80)
			       (and (>= 0x47 (elt header 1) 0x40)
				    (< (elt header 2) 0x20))))
		      (and (= (elt header 0) 0x40)
			   (or (< (elt header 1) 0x10)
			       (and (<= 0x42 (elt header 1) 0x45)
				    (< (elt header 2) 0x20))))))
	     (file->dtype file)
	     (read (open-input-file file))))))
