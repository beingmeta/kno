(in-module 'histogram)

(defambda (index-histogram index (slotids #f) (normalize #f))
  (let* ((values (if slotids
		     (get (getkeys index) slotids)
		     (getkeys index)))
	 (histogram (make-hashtable)))
    (do-choices (value values)
      (hashtable-increment! histogram value
	  (choice-size (get index (cons 'roots value)))))
    histogram))

(module-export! '{index-histogram})

