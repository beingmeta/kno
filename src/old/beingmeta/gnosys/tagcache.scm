(in-module 'gnosys/tagcache)

(message "Loading tagcache file")

(use-module '{brico ezrecords meltcache cachequeue})
(use-module '{gnosys gnosys/tagsites gnosys/urldb})

(module-export! '{gettags probetags gettags/ttl tagcq})

(define tagcache (make-hashtable))

(define checktags
  (dtproc 'checktags (config 'taghub "taghub@localhost")
	  ;; min and max arity
	  1 1
	  ;; reserve 3 sockets, max 8 sockets, init 2 sockets
	  3 8 2))

(define (remote-gettags concept)
  (try (checktags concept) #f))

(define tagcq (cachequeue remote-gettags tagcache 4 3600))

(define (tagcachedata concept)
  (get tagcache (list remote-gettags concept)))

(define (gettags-old concept (tryhard #f))
  "Returns the tags associated with a concept from a remote server"
  (let ((tags (meltcache/get tagcache remote-gettags concept)))
    (if (and tryhard (fail? tags))
	(let ((ttl (gettags/ttl concept)))
	  (sleep (+ ttl 3))
	  (meltcache/get tagcache remote-gettags concept))
	tags)))
(define (gettags concept (tryhard #f))
  "Returns the tags associated with a concept from a remote server"
  (cq/require tagcq concept))
(define (probetags concept)
  "Returns the tags associated with a concept from a remote server"
  (pickoids (cq/call tagcq concept)))

(define (gettags/ttl concept)
  (unless (test tagcache (list remote-gettags concept))
    (gettags concept))
  (let ((entry (get tagcache (list remote-gettags concept))))
    (if (meltentry? entry)
	(+ 4 (meltentry/ttl entry))
	#f)))

;;; Getting items

(define itemscache-file #f)
(define itemscache (make-hashtable))

(define (getitems tag (n 20))
  (message "Getting items for " tag)
  (let ((site (get tag 'site))
	(tagval (get tag 'value)))
    (cond ((eq? site delicious-site)
	   (delicious/getitems tagval n))
	  ((eq? site flickr-site)
	   (flickr/getitems tagval n))
	  ((eq? site technorati-site)
	   (technorati/getitems tagval n))
	  ((eq? site youtube-site)
	   (youtube/getitems tagval n))
	  (else (fail)))))

(config! 'meltcache (cons getitems (* 30 60)))

(define (tag->items tag)
  (meltcache/get itemscache getitems tag))

(define (tagcache-save) (commit itemscache))

(module-export! '{tag->items tagcache-save})

(define (config-itemscache var (val))
  (cond ((not (bound? val)) itemscache)
	((string? val)
	 (unless (file-exists? val) (make-hash-index val 1000000))
	 (set! itemscache (open-index val))
	 val)
	((table? val) (set! itemscache val) val)
	(else (error 'type-error "not a table" val))))

(config-def! 'itemscache config-itemscache)

;;; Taggregation

(defrecord (urlitem mutable)
  uri (score 0) (titles {}) (description {}) (tags {}) (sites {}))

(define (square x) (* x x))

(defambda (recompute-scores items tags)
  (do-choices (item items)
    (let ((hits (intersection tags (urlitem-tags item)))
	  (score 0))
      (do-choices (site (get hits 'site))
	(set! score (+ score
		       (/~ (choice-size (pick hits 'site site))
			   (choice-size (pick tags 'site site))))))
      (set-urlitem-score! item score))))

(define (taggregate concept)
  (let ((urlitems (make-hashtable))
	(tags (gettags concept))
	(items {}))
    (do-choices (tag (if (> (choice-size tags) 20)
			 (sample-n tags 10)
			 tags))
      (do-choices (item (tag->items tag))
	(let* ((url (cachecall urlitems cons-urlitem (get item 'ts%uri))))
	  (set+! items url)
	  (set-urlitem-score! url (1+ (urlitem-score url)))
	  (set-urlitem-titles!
	   url (choice (get item 'ts%title) (urlitem-titles url)))
	  (set-urlitem-sites!
	   url (choice (get tag 'site) (urlitem-sites url)))
	  (set-urlitem-tags!
	   url (choice tag (get item 'ts%tags) (urlitem-tags url))))))
    (message "Saving tagcache")
    (tagcache-save)
    (recompute-scores items tags)
    items))

(define (trim-vecvec vecvec depth)
  (remove #f (map (lambda (x) (and (> (length x) depth) x))
		  vecvec)))

(define (vecmerge vecvec)
  (let* ((lengths (->list (map length vecvec)))
	 (sumlen (apply + lengths))
	 (maxlen (apply max lengths))
	 (result (make-vector sumlen))
	 (depth 0)
	 (i 0))
    (while (< i sumlen)
      (doseq (vec vecvec j)
	(vector-set! result (+ i j) (elt vec depth)))
      (set! depth (1+ depth))
      (set! i (+ i (length vecvec)))
      (when (>= depth (length (elt vecvec 0)))
	(set! vecvec (trim-vecvec vecvec depth))))
    result))

(define (taggregate-results concept)
  (let* ((urlitems (taggregate concept))
	 (vecvec
	  (sorted
	   (for-choices (site (urlitem-sites urlitems))
	     (rsorted (pick urlitems urlitem-sites site) urlitem-score))
	   length)))
    (vecmerge vecvec)))

(module-export!
 '{taggregate-results
   taggregate
   urlitem? cons-urlitem urlitem-uri urlitem-score
   urlitem-titles urlitem-description urlitem-tags urlitem-sites})
