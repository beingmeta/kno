(in-module 'gnosys/usetaghub)

(use-module '{meltcache})
(use-module '{brico brico/lookup gnosys gnosys/tagsites})

(define checktags-cache (make-hashtable))
(define queuetags-cache (make-hashtable))
(define istag?-cache (make-hashtable))
(define getitems-cache (make-hashtable))

(define checktags-dtproc
  (dtproc 'checktags (config 'taghub "taghub@localhost")
	  ;; min and max arity
	  1 1
	  ;; reserve 3 sockets, max 8 sockets, init 2 sockets
	  3 8 2))
(define cachedtags
  (dtproc 'cachedtags (config 'taghub "taghub@localhost")
	  ;; min and max arity
	  1 1
	  ;; reserve 3 sockets, max 8 sockets, init 2 sockets
	  3 8 2))
(define queuetags-dtproc
  (dtproc 'queuetags (config 'taghub "taghub@localhost")
	  ;; min and max arity
	  1 1
	  ;; reserve 3 sockets, max 8 sockets, init 2 sockets
	  3 8 2))
(define istag?-dtproc
  (dtproc 'tagp (config 'taghub "taghub@localhost")
	  ;; min and max arity
	  1 1
	  ;; reserve 3 sockets, max 8 sockets, init 2 sockets
	  3 8 2))

(define getitems-dtproc
  (dtproc 'getitems (config 'taghub "taghub@localhost")
	  ;; min and max arity
	  1 1
	  ;; reserve 3 sockets, max 8 sockets, init 2 sockets
	  3 8 2))

(define (gettags concept)
  (meltcache/get checktags-cache checktags-dtproc concept))

(define (fasttags concept)
  (try (meltcache/info checktags-cache checktags-dtproc concept)
       (if (exists? (cachedtags concept))
	   (meltcache/getentry checktags-cache checktags-dtproc concept)
	   (meltentry #f #{} 8))))

(define (gettags/info concept)
  (meltcache/getentry checktags-cache checktags-dtproc concept))
(define (gettags/cached concept)
  (meltcache/probe checktags-cache checktags-dtproc concept))
(define (gettags/ttl concept)
  (meltentry/ttl (meltcache/info checktags-cache checktags-dtproc concept)))

(define (queuetags concept)
  (meltcache/get queuetags-cache queuetags-dtproc concept))

(define (istag? concept)
  (meltcache/get istag?-cache istag?-dtproc concept))
(define (getitems concept)
  (meltcache/get getitems-cache getitems-dtproc concept))

(config! 'meltcache (cons getitems-dtproc 7))

(module-export! '{gettags
		  ;; This goes over the network, but uses the cache there
		  cachedtags fasttags queuetags
		  gettags/cached gettags/ttl gettags/info
		  istag? getitems})

