;;; -*- Mode: Scheme -*-

(config! 'quiet #t)

(use-module '{logger varconfig optimize reflection})

(define (resolve-module config)
  (or (cond ((symbol? config) (get-module config))
	    ((and (string? config) (file-exists? config)) (get-module config))
	    ((string? config) (get-module (string->symbol config)))
	    (else #f))
      (irritant config |BadAppModule| resolve-module config)))

(define appmodule #f)
(varconfig! appmodule appmodule resolve-module)

(define apphandler 'main)
(varconfig! apphandler apphandler #t)

(define optmods {})
(varconfig! optmods optmods resolve-module)

(define (knapp config-file . args)
  (if (file-exists? config-file)
      (load-config config-file)
      (irritant config-file |MissingConfigFile| knapp))
  (unless appmodule (irritant config-file |NoAppModule|))
  (let ((setup (try (get appmodule 'setup) #f))
	(main (get appmodule apphandler)))
    (when setup 
      (onerror (apply setup args)
	  (lambda (ex)
	    (set! main #f)
	    (logerr |SetupError|
	      "Calling " setup ": " (exception-summary ex #f)))))
    (when main
      (when (config 'optimized #t)
	(optimize* optmods)
	(optimize* appmodule))
      (apply main args))))

(define main knapp)
