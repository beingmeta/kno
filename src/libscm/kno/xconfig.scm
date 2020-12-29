;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'xconfig)

;;; Loading of modules from external (non-file system) sources

(use-module '{gpath varconfig logger})

(module-export! '{load-xconfig xconfig:root xconfig? xconfig/reset!})

(define-init %loglevel %notice%)
;;(set!  %loglevel %debug%)

(define-init xconfig:root #f)
(define-init xconfigs {})
(define-init xconfig:trace #t)

(varconfig! xconfig:root xconfig:root)
(varconfig! xconfig:trace xconfig:trace config:boolean+)

(define (xconfig? path)
  (or (overlaps? path xconfigs)
      (and (gpath? path) 
	   (overlaps? (gpath->string path xconfig:root) xconfig))
      (and (string? path)
	   (onerror
	       (overlaps? (gpath->string (->gpath path xconfig:root))
			  xconfig)
	     (lambda (ex) #f)))))

(define (canonical-gpath path)
  (if (string? gpath) (gpath->string (->gpath path xconfig:root))
      (if (gpath? path) (gpath->string path)
	  (fail))))

(defambda (xconfig/reset! (paths #f))
  (if (not paths) 
      (set! xconfigs {})
      (set! xconfigs (difference xconfigs (canonical-gpath paths)))))

(define (load-xconfig path (err #f))
  (let* ((root (try (thread/get 'xconfig:root) xconfig:root))
	 (gp (->gpath path (if (string? root) (->gpath root) root)))
	 (canonical (and (gpath? gp) (gpath->string gp))))
    (debug%watch "LOAD-XCONFIG" path root gp canonical)
    (if (gpath? gp)
	(unless (overlaps? canonical xconfigs) 
	  (if (gp/exists? gp)
	      (begin
		(when (or xconfig:trace (config 'traceconfigload))
		  (lognotice |XConfig| "Loading " canonical))
		(unwind-protect
		    (begin (thread/set! 'xconfig:root (gp/location gp))
		      (read-config (->string (gp/fetch gp))))
		  (thread/set! 'xconfig:root root))
		(set+! xconfigs canonical))
	      (if err
		  (irritant canonical |LoadXConfigFailed|)
		  (logwarn |LoadXConfigFailed| 
		    "Can't get content for " 
		    canonical))))
	(if err
	    (irritant path |InvalidXConfigPath|)
	    (logwarn |InvalidXConfigPath| "Can't handle " path)))))

(define (xconfig-handler var (val))
  (if (not (bound? val)) xconfigs
      (if (symbol? val) 
	  (if (and (config val) (string? (config val)))
	      (load-xconfig (config val) #t)
	      (irritant val |BadXconfigRef| "Could't resolve XCONFIG "))
	  (if (or (string? val) (gpath? string))
	      (load-xconfig val #t)
	      (irritant val |BadXconfigValue| val)))))
(config-def! 'xconfig xconfig-handler)

(define (xxconfig-handler var (val))
  (if (not (bound? val)) xconfigs
      (if (symbol? val) 
	  (if (and (config val) (string? (config val)))
	      (load-xconfig (config val))
	      (logwarn |BadXconfigRef| "Could't resolve XCONFIG " val))
	  (if (or (string? val) (gpath? string))
	      (load-xconfig val)
	      (logwarn |BadXconfigValue| "Couldn't handle " val)))))
(config-def! 'xxconfig xxconfig-handler)


