(in-module 'knowlets/usebrico)

(use-module '{texttools knowlets brico brico/lookup brico/dterms})

(module-export! '{brico->kno kno/usebrico})

;;;; Resolving references

(define serial 0)

(define (get-unique-id base knowlet)
  (let ((dterms (knowlet-dterms knowlet)))
    (do ((i 1 (1+ i)))
	((fail? (get dterms (stringout base "(" i ")")))
	 (stringout base "(" i ")")))))

(define (brico->kno bf knowlet (create #f))
  (try (find-frames (knowlet-index knowlet) 'oid bf)
       (?? 'oid bf 'knowlet (knowlet-oid knowlet))
       (tryif create
	      (let* ((language (knowlet-language knowlet))
		     (langframe (get language-map language))
		     (dterm (pick-one (get-dterm bf langframe)))
		     (kf (kno/dterm
			  (try (or dterm {})
			       (get-unique-id (get-norm bf language)
					      knowlet))
			  knowlet)))
		(when (ambiguous? bf)
		  (add! kf 'dterms (get-dterm bf langframe))
		  (add! (knowlet-index knowlet)
			(cons 'dterms (get-dterm bf langframe))
			kf))
		(unless (eq? language 'en)
		  (add! kf 'dterms (cons 'en (get-dterm bf @?en)))
		  (add! (knowlet-index knowlet)
			(cons 'dterms (cons 'en (get-dterm bf langframe)))
			kf))
		(when (fail? dterm)
		  (warning "Can't get DTERM for " bf))
		(add! kf 'oid bf)
		(add! (knowlet-index knowlet) (cons 'oid bf) kf)
		(store! kf 'gloss
			(try
			 (tryif (eq? language 'en) (get bf 'gloss))
			 (get-gloss bf langframe)))
		(store! kf 'en (get-norm bf @?en))
		(add! (knowlet-index knowlet)
		      (cons 'en (get-norm bf @?en))
		      kf)
		(store! kf language (get bf (get language-map language)))
		(add! (knowlet-index knowlet)
		      (cons (knowlet-language knowlet)
			    (get-norm bf  (get language-map language)))
		      kf)
		kf))))

(define (brico->kno* bf knowlet (visits (make-hashset)))
  (try (find-frames (knowlet-index knowlet) 'oid bf)
       (?? 'oid bf 'knowlet (knowlet-oid knowlet))
       (let* ((always (get bf @?always)))
	 (hashset-add! visits bf)
	 (brico->kno* (reject always visits) knowlet visits))))

;;; Copying information from brico

(define usebrico-defaults
  '#{languages})

(define (kno/usebrico (kf #f) (bf #f) (slotids usebrico-defaults))
  (when (or kf bf)
    (when (not kf)
      (set! kf (find-frames (knowlet-index default-knowlet)
		 'oid bf)))
    (when (not bf) (set! bf (get kf 'oid)))
    (when (overlaps? slotids 'languages)
      (set+! slotids langids))
    (let* ((knowlet (get knowlets (get kf 'knowlet)))
	   (knolang (knowlet-language knowlet))
	   (languages (intersection slotids langids)))
      (if (and (test bf 'gloss) (eq? knolang 'en) (not (test kf 'gloss)))
	  (store! kf 'gloss (get bf 'gloss)))
      (do-choices (lang languages)
	(let ((bricolang (get language-map lang)))
	  (add! kf lang (get bf bricolang))
	  (if (eq? lang knolang)
	      (add! kf 'norms (get-norm bf bricolang))
	      (add! kf 'norms (cons lang (get-norm bf bricolang))))
	  (when (and (eq? lang knolang) (not (test kf 'gloss)))
	    (store! kf 'gloss (get-single-gloss bf bricolang)))
	  (if (eq? lang knolang)
	      (add! kf 'glosses (get bf (get gloss-map lang)))
	      (add! kf 'glosses
		    (cons lang (get bf (get gloss-map lang)))))
	  (if (eq? lang knolang)
	      (add! kf 'hooks (get bf (get index-map lang)))
	      (add! kf 'hooks
		    (cons lang
			  (get bf (get index-map lang)))))))
      (unless (test bf 'sensecat '{noun.tops verb.tops})
	(let* ((genls (get bf {@?always @?commonly}))
	       (commonly (difference (get bf @?commonly) genls))
	       (sometimes (difference (get bf @?sometimes) genls)))
	  (kno/add! kf 'genls (brico->kno genls knowlet))
	  (kno/add! kf 'commonly (brico->kno commonly knowlet))
	  (kno/add! kf 'sometimes (brico->kno sometimes knowlet))))
      (let* ((never (get bf @?never))
	     (rarely (difference (get bf @?rarely) never))
	     (somenot (difference (get bf @?somenot) never)))
	(kno/add! kf 'never (brico->kno never knowlet))
	(kno/add! kf 'rarely (brico->kno rarely knowlet))
	(kno/add! kf 'somenot (brico->kno somenot knowlet)))
      (when (test bf @?refterms)
	(kno/add! kf 'refs (brico->kno (get bf @?refterms) knowlet)))
      (when (test bf @?sumterms)
	(kno/add! kf 'assocs
		  (brico->kno (get bf @?sumterms) knowlet #t)))
      (when (test bf @?sumterms)
	(kno/add! kf 'defs
		  (brico->kno (get bf @?diffterms) knowlet #t)))
      (when (test bf 'country)
	(kno/add! kf (kno/dterm "country" knowlet)
		  (brico->kno (get bf 'country) knowlet #t)))
      (when (test bf 'region)
	(kno/add! kf (kno/dterm "region" knowlet)
		  (brico->kno (get bf 'region) knowlet #t)))
      (when (test bf @?ingredients)
	(kno/add! kf (kno/dterm "ingredient" knowlet)
		  (brico->kno (get bf @?ingredients) knowlet #t)))
      (when (test bf @?memberof)
	(kno/add! kf (kno/dterm "group" knowlet)
		  (brico->kno (get bf @?memberof) knowlet #t)))
      (when (test bf @?partof)
	(kno/add! kf (kno/dterm "assemblage" knowlet)
		  (brico->kno (get bf @?memberof) knowlet #t)))
      (do-choices (role (pick (pickoids (getkeys bf)) 'sensecat))
	(do-choices (v (pickoids (%get bf role)))
	  (kno/add! kf (brico->kno role knowlet #t)
		    (brico->kno v knowlet #t))))
      kf)))









