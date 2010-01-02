(in-module 'xhtml/webapp)

;;; This provides a simple web application framework

(define version "$Id$")

(use-module '{fdweb xhtml texttols xhtml/auth xhtml/openid varconfig rulesets})

(define apphost "www.example.com")
(define approot "/smartapp")
(define appid "WEBAPP")
(define userelpaths #t)
(varconfig! app:hostname apphost)
(varconfig! app:root approotd)
(varconfig! app:id appid)

(define secure-site #f)
(define secure-roots {})
(define insecure-roots {})
(varconfig! app:secure secure-site)
(ruleconfig! app:https secure-roots)
(ruleconfig! app:justhttp insecure-roots)

;;;; SITEURL

(define (app/sitepath app (userel userelpaths))
  (let* ((hostname (cgiget 'server_name))
	 (cursecure (cgitest 'server_port 443))
	 (curpath  (cgiget 'request_uri))
	 (curdir  (dirname curpath))
	 (basepath (mkpath (cgiget 'appbase approot) app))
	 (secure (or (not (textsearch (qc insecure-roots) basepath))
		     (textsearch (qc secure-roots) basepath)
		     secure-site)))
    (if (or (not userel) (not (eq? secure cursecure))
	    (not (eq? hostname apphost)))
	(stringout (if secure "https:" "http:") "//"  apphost basepath)
	(if (has-prefix basepath curdir)
	    (subseq basepath (length curdir))
	    basepath))))

(define (app/siteurl app . args)
  (if (null? args)
      (app/sitepath app)
      (apply scripturl (app/sitepath app) args)))

(module-export! '{app/siteurl app/sitepath})


