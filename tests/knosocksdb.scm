(use-module '{brico knosocks})

(config! 'servepools brico.pool)
(config! 'serveindexes brico.index)

(define (main (port "localhost:8888"))
  (let* ((listener
	  (knosockd/listener port [initclients 3 maxclients 8 nthreads 2]
			     (get-module 'dbserv)))
	 (thread (thread/call knosockd/run listener)))
    (thread/join thread)))
