;;; -*- Mode: Scheme -*-

(use-module '{knosocks logger varconfig})

(define %loglevel %info%)

(define (get-server-mod mod)
  (or (get-module mod)
      (and (string? mod) (get-module (string->symbol mod)))))

(define (dbserv?)
  (and (not (config 'nodbserv #f))
       (or (config 'servepools) (config 'serveindexes))))

(define (main (port (config 'PORT)) . mods)
  (when (and (string? port) (file-exists? port))
    (cond ((has-suffix port {".fdz" ".scm"}) (load-file port))
	  ((has-suffix port {".cfg" ".conf"}) (load-config port)))
    (set! port (config 'port (config 'listen))))
  (cond ((fixnum? port) (set! port (glom "0.0.0.0:" port)))
	((not (string? port)) (set! port (config 'port (config 'listen))))
	(else))
  (let* ((server-mods (remove #f (map get-server-mod mods)))
	 (opts [initclients (config 'startclients 7)
		maxclients (config 'maxclients 100)
		nthreads (config 'nthreads 16)
		dbserv (config 'dbserv (dbserv?))])
	 (listener
	  (knosockd/listener {port (config 'listen {})} opts
			     (and (pair? server-mods) server-mods))))
    (loginfo |Server_Starting| 
      "Starting server listening at " port " with " listener)
    (onerror (begin (knosockd/run listener)
	       (loginfo |Server_Finished|
		 "Stopped listening at " port " with " listener))
	(lambda (ex)
	  (loginfo |Server_Aborted|
	    "Aborted listener at " port 
	    "\n  " listener
	    "\n  " ex)
	  #f))))


