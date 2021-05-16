;;; -*- Mode: Scheme -*-

(config! 'quiet #t)

(use-module '{logger varconfig optimize reflection})

(define (resolve-module config)
  (or (cond ((symbol? config) (get-module config))
	    ((and (string? config) (file-exists? config)) (get-module config))
	    ((string? config) (get-module (string->symbol config)))
	    (else #f))
      (irritant config |BadAppModule|)))

(define appmodule #f)
(varconfig! appmodule appmodule resolve-module)

(define apphandler 'main)
(varconfig! apphandler apphandler #t)

(define optmods {})
(varconfig! optmods optmods resolve-module)

(define (main config . args)
  (if (file-exists? config) (load-config config)
      (irritant config |MissingConfigFile|))
  (unless appmodule (error |NoAppModule|))
  (let ((setup (try (get appmodule 'setup) #f))
	(main (get appmodule apphandler)))
    (when (or (not setup) (apply setup args))
      (when (config 'optimized #t)
	(optimize* optmods)
	(optimize* (get appmodule '%optmods)))
      (apply main args))))
