(in-module 'gnosys/metakeys)

(use-module '{brico brico/lookup morph})

(define default-language english)

(module-export! '{lookup-term lookup-norm vary-word lookup-term-prefetch})

(define (lookup-term term (language default-language) (tryhard #f))
  (choice (lookup-word term language tryhard)
	  (intersection
	   (lookup-word (get-noun-root term language) language tryhard)
	   (?? 'type 'noun))
	  (intersection
	   (lookup-word (get-verb-root term language) language tryhard)
	   (?? 'type 'verb))))

(define (lookup-norm term (language default-language) (tryhard #f))
  (let ((norm (try (get norm-map language) language)))
    (choice (lookup-word term norm tryhard)
	    (intersection
	     (lookup-word term norm tryhard)
	     (?? 'type 'noun))
	    (intersection
	     (lookup-word term norm tryhard)
	     (?? 'type 'verb)))))

(define (vary-word word language (varycase #f))
  (let ((baseform (basestring word)))
    (choice word baseform
	    (get-noun-root (choice word baseform) language)
	    (get-verb-root (choice word baseform) language)
	    (tryif varycase
		   (if (lowercase? word)
		       (upcase (choice word baseform))
		       (if (and (capitalized? word) (not (uppercase? word)))
			   (downcase (choice word baseform))
			   (fail)))))))



(defambda (lookup-term-prefetch words (language default-language) (tryhard #f))
  (lookup-word-prefetch (choice words (get-noun-root words language)
				(get-verb-root words language))
			language tryhard))
