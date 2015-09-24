;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2011-2015 beingmeta, inc. All rights reserved

(in-module 'ximage)

(use-module '{fdweb texttools imagick gpath aws/s3
	      reflection varconfig logger})

(define-init %loglevel %notice%)
(varconfig! ximage:loglevel %loglevel)

(define ximage:root #f)
(varconfig! ximage:root ximage:root)

(define ximage:force #f)
(varconfig! ximage:force ximage:force)

(define thumbnail-width 150)
(varconfig! ximage:thumbnail thumbnail-width)

(module-export! '{ximage ximage/loc ximage/url ximage/transform})

(define (->fix n) (inexact->exact (floor n)))

(define-init xforms
  (frame-create #f
    'thumbnail #[name "thumbnail" width 150]))

(define (ximage content xform (root #f) (force ximage:force))
  (if (symbol? xform)
      (try (get xforms xform)
	   (get xforms (downcase xform))
	   xform))
  (let* ((useroot (cond ((not root) ximage:root)
			((and (string? root) (not (has-prefix root "/")))
			 (gp/mkpath ximage:root root))
			(else root)))
	 (name (ximage/xformname xform))
	 (hash (md5 content))
	 (path (mkpath (packet->base16 hash) name))
	 (gpath (gp/mkpath useroot path)))
    (if (and (not force) (gp/exists? gpath)) 
	(begin (loginfo |XIMAGE/Exists|
		 "Transformed image from (MD5:" (packet->base16 hash) ") "
		 "already exists: " (gpath->string gpath))
	  gpath)
	(let* ((newimage (ximage/transform content xform))
	       (newformat (get newimage 'format))
	       (newdata (imagick->packet newimage)))
	  (lognotice |XIMAGE/Cache|
	    "Saving " (length newdata) " generated bytes of " newformat
	    " image to " (gpath->string gpath))
	  (gp/save! gpath (imagick->packet newimage) 
	    (glom "image/" (downcase newformat)))
	  gpath))))

(define (ximage/loc content xform (root #f))
  (if (symbol? xform) (try (get xforms xform) xform))
  (let* ((useroot (cond ((not root) ximage:root)
			((and (string? root) (not (has-prefix root "/")))
			 (gp/mkpath ximage:root root))
			(else root)))
	 (name (ximage/xformname xform))
	 (hash (md5 content))
	 (path (mkpath (packet->base16 hash) name)))
    (gp/mkpath useroot path)))

(define (ximage/url content xform (root #f))
  (let ((loc (ximage/loc content xform root)))
    (if (s3loc? loc) (s3loc/uri loc)
	(if (string? loc) loc
	    (fail)))))

(define (ximage/transform data xform)
  (lognotice |XIMAGE/Transform|
    "Transforming " (length data) 
    " bytes of imagedata (MD5:" (packet->base16 (md5 data)) ")"
    "\n\t\twith " xform)
  (if (or (procedure? xform)
	  (getopt xform 'handler)
	  (procedure? (getopt xform 'handler)))
      (let* ((image (packet->imagick data))
	     (newimage (if (procedure? xform)
			   (xform image content)
			   ((getopt xform 'handler) image data xform))))
	newimage)
      (if (table? xform)
	  (let* ((image (packet->imagick data))
		 (width (try (getopt xform 'width {})
			     (* (getopt xform 'scalex (getopt xform 'scale {}))
				(get image 'width))))
		 (height (or (getopt xform 'height {})
			     (* (getopt xform 'scaley (getopt xform 'scale {}))
				(get image 'height))))
		 (w/h (/~ (get image 'width) (get image 'height)))
		 (result #f))
	    (if (or (exists? width) (exists? height))
		(begin
		  (when (fail? width) (set! width (* height w/h)))
		  (when (fail? height) (set! height (* (/ w/h) width)))
		  (lognotice |XIMAGE/Transform| "Fitting image to " 
			     (->fix width) "x" (->fix height))
		  (set! result (imagick/fit (imagick/clone image) 
					    (->fix width) (->fix height)
					    'catrom)))
		(set! result image))
	    (when (getopt xform 'format)
	      (set! result (imagick/format result (getopt xform 'format))))
	    result)
	  (packet->imagick data))))

(define (ximage/xformname xform)
  (if (string? xform) xform
      (if (symbol? xform)
	  (try (get (get xforms xform) 'name)
	       (downcase (symbol->string xform)))
	  (glom (if (procedure? xform)
		    (or (procedure-name xform)
			(number->string (hashptr xform) 16))
		    (if (table? xform)
			(try (get xform 'name)
			     (glom (try (get xform 'root) #f)
			       (get xform 'width) "x" (get xform 'height))
			     "original")
			"original"))))))




