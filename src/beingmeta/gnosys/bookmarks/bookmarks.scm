(in-module 'gnosys/bookmarks)

(use-module 'texttools)
(use-module 'gnosys)
(use-module 'gnosys/utils/glomming)
(use-module 'gnosys/urldb)
(use-module 'gnosys/webapp/userinfo)
(use-module 'gnosys/metakeys)
(use-module 'gnosys/metakeys/tagmaps)
(use-module 'gnosys/metakeys/disambiguate)

(module-export! '{get-tag-scores get-frequency bookmarkindex bookmarkpool})
(module-export! '{make-bookmark
		  get-tags get-tags*
		  bookmark-tags bookmark-tags*})
(module-export! '{import-bookmark temp-bookmarks})

;;; Utilities

(define (get-frequency meaning)
  (+ (choice-size (find-frames disambig-indices @?gn/concepts meaning))
     (let ((score 0))
       (do-choices (wordform (?? 'of meaning))
	 (set! score (+ score (get wordform 'freq))))
       score)))

(define (get-tag-scores bookmarks (users #f))
  (let ((scores (make-hashtable))
	(users (or users (get bookmarks 'source)))
	(tags (get bookmarks 'tags)))
    (do-choices (user users)
      (do-choices (tag tags)
	(hashtable-increment!
	    scores tag
	    (choice-size (find-frames bookmarkindex
			   'source user @?gn/concepts tag)))))
    scores))

;;;; Bookmarks

(define bookmarksource #f)
(define bookmarkpool #f)
(define bookmarkindex #f)

(define (make-bookmark source about)
  (try (find-frames bookmarkindex 'about about 'source source)
       (let* ((now (timestamp))
	      (f (frame-create bookmarkpool
		   'type 'bookmark
		   'about about 'source source
		   @?gn/parent (get about @?gn/parent)
		   'created now 'touched now)))
	 (index-frame bookmarkindex f '{about source type})
	 ;; (index-frame bookmarkindex f 'created (get now 'features))
	 f)))

(define (get-tags about source)
  (get (if source
	   (choice
	    (find-frames bookmarkindex 'about about 'source source)
	    (find-frames temp-bookmarks 'about about 'source source))
	   (choice (find-frames bookmarkindex 'about about)
		   (find-frames temp-bookmarks 'about about)))
       'tags))
(define (get-tags* about source (untags #{}))
  (let ((bookmark (if source
		      (choice
		       (find-frames bookmarkindex 'about about 'source source)
		       (find-frames temp-bookmarks 'about about 'source source))
		      (choice (find-frames bookmarkindex 'about about)
			      (find-frames temp-bookmarks 'about about)))))
    (choice (difference (get bookmark 'tags) untags)
	    (get-tags* (get about @?gn/parent) source
		       (qc untags (get bookmark 'untags))))))
(define (bookmark-tags bookmark) (get bookmark 'tags))
(define (bookmark-tags* bookmark)
  (get-tags* (get (get bookmark 'about) @?gn/parent)
	     (get bookmark 'source)
	     (qc (get bookmark 'untags))))

;;; Configuring bookmarks db

(define bookmarks-config
  (slambda (var (val unbound))
    (cond ((eq? val 'unbound) bookmarksource)
	  ((equal? val bookmarksource)
	   #f)
	  (else
	   (when bookmarksource
	     (message "Warning: reconfiguring bookmark source"))
	   (set! bookmarksource val)
	   (set! bookmarkpool (use-pool val))
	   (set! bookmarkindex (open-index val))))))
(config-def! 'bookmarks bookmarks-config)

;;; Temporary bookmarks

;;; This is a table for temporary bookmarks which might be 
;;;  returned from foreign tagging sites.  It maps URL objects to
;;;  slotmaps.

(unless (bound? temp-bookmarks)
  (define temp-bookmarks (make-hashtable)))

;;; Converting bookmarks

(define (temp-bookmark about user provider)
  (try (find-frames temp-bookmarks 'about about 'user user 'provider provider)
       (let ((f (frame-create #f 'about about 'user user 'provider provider)))
	 (index-frame temp-bookmarks f '{about user provider})
	 f)))

(define (import-bookmark bm)
  (let* ((url (get bm 'about))
	 (urlframe (url2frame url))
	 (source (find-frames userindex
		   (get bm 'providerid) (get bm 'user)))
	 (language (try (get bm 'language)
			(get source 'language)
			@?english))
	 (keywords (choice (get bm 'keywords)
			   (list->phrase
			    (deglom (get bm 'keywords) @?english))))
	 (raw-entries
	  (keyentry/usetagmaps (term->keyentry keywords language #t)
			       (qc source)))
	 (cscores (scorecorpus/keyentry (scorecorpus/url #f urlframe)
					(qc raw-entries)))
	 (entries
	  (keyentry/disambig raw-entries cscores #f 1.1))
	 (tags (keyentry->tag entries)))
    (when (test source 'flags 'learntags)
      (do-choices (entry entries)
	(let* ((keyword (keyentry-base entry))
	       (tmap (tagmap keyword source (get bm 'provider))))
	  (unless (test tmap 'flags 'edited)
	    (assert! tmap @?genls (keyentry-actual entry))
	    (index-frame userindex @?genls (keyentry-actual entry))
	    (index-frame userindex @?genls*
	      (get (keyentry-actual entry) @?genls*))))))
    (if (test source 'flags 'convert)
	(let* ((rbm (make-bookmark source urlframe))
	       (concepts (tag-concepts tags))
	       (tags* (choice
		       (get concepts {@?genls @?partof})
		       (get (get concepts @?isa) @?genls))))
	  (assert! rbm 'title (get bm 'title))
	  (assert! rbm 'description (get bm 'description))
	  (assert! rbm @?gn/keywords keywords)
	  (assert! rbm 'provider (get bm 'provider))
	  (assert! rbm 'providerid (get bm 'providerid))
	  (assert! rbm 'user (get bm 'user))
	  (index-frame bookmarkindex rbm
	    @?gn/keywords
	    (choice (get bm 'keywords) (stdstring  (get bm 'keywords))))
	  (assert! rbm 'tags tags)
	  (assert! rbm @?gn/xconcepts (tag-concepts tags))
	  (index-frame bookmarkindex rbm '{provider user})
	  (index-frame bookmarkindex rbm 'tags tags)
	  (index-frame bookmarkindex rbm @?gn/xconcepts tags*)
	  (index-frame bookmarkindex rbm @?gn/concepts tags*)
	  rbm)
	(try (find-frames bookmarkindex
	       'about urlframe 'user (get bm 'user)
	       'provider (get bm 'provider))
	     (let ((temp (temp-bookmark
			  urlframe (get bm 'user) (get bm 'provider))))
	       (when (exists? (get bm 'description))
		 (unless (test temp 'description (get bm 'description))
		   (store! temp 'description (get bm 'description))))
	       (when (exists? (get bm 'title))
		 (unless (test temp 'title (get bm 'title))
		   (store! temp 'title (get bm 'title))))
	       (unless (test temp 'source source)
		 (add! temp 'source source)
		 (index-frame temp-bookmarks temp 'source source))
	       (let* ((added (difference tags (get temp 'tags)))
		      (newconcepts (tag-concepts added))
		      (newkeywords (tag-string added)))
		 ;; (message "Tags for " urlframe ": " added)
		 ;; (message "Concepts for " urlframe ": " newconcepts)
		 (add! temp 'tags added)
		 (add! temp @?gn/xconcepts newconcepts)
		 (index-frame temp-bookmarks temp 'tags added)
		 (index-frame temp-bookmarks temp 'tags newkeywords)
		 (index-frame temp-bookmarks temp 'tags newconcepts)
		 (index-frame temp-bookmarks
		     temp
		   @?gn/xconcepts (choice (get newconcepts @?genls*)
					  (get (get newconcepts @?isa) @?genls*)
					  (get newconcepts @?partof*))))
	       (let ((added (difference keywords (get temp @?gn/keywords))))
		 (add! temp @?gn/keywords added)
		 (index-frame temp-bookmarks temp @?gn/keywords
		   (choice added (stdstring added))))
	       temp)))))



