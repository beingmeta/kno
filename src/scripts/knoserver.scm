;;; -*- Mode: Scheme -*-

(use-module '{knosocks knodb logger varconfig})

(define %loglevel %info%)

(define-init serve-pools {})
(varconfig! servepool serve-pools)
(varconfig! servepools serve-pools)

(define-init serve-indexes {})
(varconfig! serveindex serve-indexes)
(varconfig! serveindexes serve-indexes)

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
  (let* ((dbserver (dbserv?))
	 (server-env (append (remove #f (map get-server-mod mods))
			     (remove #f (map get-server-mod (config 'USEMODS '())))
			     (if dbserver (list (get-module 'knodb/dbserv)) '())))
	 (server-config #[]))
    (when dbserver
      (add! server-config 'pools (pool/ref serve-pools))
      (add! server-config 'indexes (index/ref serve-indexes))
      (unless (test server-config 'indexes)
	(add! server-config 'indexes
	  (pool/getindex (get server-config 'pools) #[register #t])))
      (unless (test server-config 'index)
	(let ((indexes (get server-config 'indexes)))
	  (if (singleton? indexes) 
	      (store! server-config 'index indexes)
	      (store! server-config 'index (make-aggregate-index indexes #[register #t]))))))
    (let ((listener
	   (knosockd/listener {port (config 'listen {})}
			      [initclients (config 'startclients 7)
			       maxclients (config 'maxclients 100)
			       nthreads (config 'nthreads 16)]
			      (and (pair? server-env) server-env)
			      server-config)))
      (loginfo |Server_Starting|
	"Starting server listening at " port " with " listener 
	" and CONFIG=" server-config)
      (onerror (begin (knosockd/run listener)
		 (loginfo |Server_Finished|
		   "Stopped listening at " port " with " listener))
	  (lambda (ex)
	    (loginfo |Server_Aborted|
	      "Aborted listener at " port 
	      "\n  " listener
	      "\n  " ex)
	    #f)))))
