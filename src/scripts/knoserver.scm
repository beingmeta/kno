;;; -*- Mode: Scheme -*-

(use-module '{knosocks logger varconfig})

(define %loglevel %info%)

(define (get-server-mod mod)
  (or (get-module mod)
      (and (string? mod) (get-module (string->symbol mod)))))


(define (main port . mods)
  (let* ((server-mods (remove #f (map get-server-mod mods)))
	 (opts [initclients (config 'startclients 7)
		maxclients (config 'maxclients 100)
		nthreads (config 'nthreads 16)])
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


