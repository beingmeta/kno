(in-module 'gnosys/bookmarks/search)

(use-module '{texttools
	      gnosys
	      gnosys/metakeys gnosys/metakeys/tagmaps
	      gnosys/bookmarks
	      gnosys/webapp/userinfo})

(module-export! '{tagsearch fullsearch deepsearch})

;;; Search just for tags

(define (tagsearch tags domain)
  (let ((scores (make-hashtable)))
    (do-choices (tag tags)
      (let* ((keyword (choice (tag-string tag)
			      (porter-stem (tag-string tag))))
	     (concepts (tag-concepts tag))
	     (direct
	      (if (empty? domain)
		  (choice
		   (find-frames bookmarkindex @?gn/keywords keyword)
		   (find-frames temp-bookmarks @?gn/keywords keyword)
		   (find-frames bookmarkindex
		     @?gn/xconcepts (list concepts))
		   (find-frames temp-bookmarks
		     @?gn/xconcepts (list concepts)))
		  (choice
		   (find-frames bookmarkindex
		     @?gn/keywords keyword 'source domain)
		   (find-frames bookmarkindex
		     @?gn/xconcepts (list concepts) 'source domain)
		   (find-frames temp-bookmarks
		     @?gn/keywords keyword 'source domain)
		   (find-frames temp-bookmarks
		     @?gn/xconcepts (list concepts) 'source domain))))
	     (indirect
	      (if (empty? domain)
		  (find-frames (choice bookmarkindex temp-bookmarks)
		    @?gn/xconcepts concepts)
		  (choice
		   (find-frames bookmarkindex
		     @?gn/xconcepts concepts 'source domain)
		   (find-frames temp-bookmarks
		     @?gn/xconcepts concepts 'source domain)))))
	(hashtable-increment! scores (get direct 'about))
	(hashtable-increment! scores (get indirect 'about))))
    scores))

(define (fullsearch tags domain webindex)
  (let ((scores (tagsearch (qc tags) (qc domain))))
    (do-choices (tag tags)
      (let* ((keyword (choice (tag-string tag)
			      (porter-stem (tag-string tag))))
	     (concepts (tag-concepts tag))
	     (direct
	      (choice (find-frames webindex @?gn/keywords keyword)
		      (find-frames webindex @?gn/xconcepts (list concepts))))
	     (indirect (find-frames webindex @?gn/xconcepts concepts)))
	(hashtable-increment! scores direct)
	(hashtable-increment! scores indirect)))
    scores))

;;; Searching on content too

(define (passagesearch tags passageindex)
  (let ((scores (make-hashtable)))
    (do-choices (tag tags)
      (hashtable-increment! scores
	(find-frames passageindex @?gn/concepts tag)))
    scores))

(define (deepsearch keywords tags domain docindex passageindex)
  (let* ((urlscores (fullsearch (qc keywords) (qc tags) (qc domain) docindex))
	 (passagescores (passagesearch (qc tags) passageindex))
	 (passages (getkeys passagescores))
	 (urlpassagescores (make-hashtable))
	 (urlpassagecount (make-hashtable)))
    (prefetch-oids! passages)
    (prefetch-oids! (get passages @?gn/parent))
    (prefetch-oids! (get (get passages @?gn/parent)
			 '{@?doc/source about}))
    (do-choices (passage passages)
      (let ((url (get (get passage @?gn/parent)
		      '{@?doc/source about})))
	(cond ((exists? url)
	       (let ((score (get passagescores passage)))
		 (hashtable-increment! urlpassagescores url (get passagescores passage))
		 (hashtable-increment! urlpassagecount url (get passagescores passage))
		 (store! urlscores passage (get passagescores passage)))
	       (add! urlscores (list url) passage))
	      (else (store! urlscores passage (get passagescores passage))))))
    (do-choices (url (getkeys urlpassagescores))
      (let ((delta (inexact->exact
		    (ceiling (/~ (get urlpassagescores url)
				 (get urlpassagecount url))))))
	(hashtable-increment! urlscores url delta)))
    urlscores))







