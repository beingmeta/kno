(config! 'bricosource "./brico/") (use-module 'brico)

(dbctl brico.pool 'readonly #f)

(define (getlangid x)
  (try (get x 'iso639/1)
       (get x 'iso639/2)
       (get x @?"P218")
       (get x @?"P219")
       (pick-one (pick (get x 'words) uppercase?))
       (pick-one (smallest (downcase (get x '%mnemonics)) length))))

(define (fix-key f)
  (if (exists symbol? (get f 'key))
      (store! f 'key (pick (get f 'key) symbol?))
      (let* ((key (pickoids (get f 'key)))
	     (langid (getlangid key)))
	(when (exists? langid)
	  (message "For " f " got " langid " via " key)
	  (store! f 'key langid)))))

(define (get-to-fix) (filter-choices (f (?? 'has 'key)) (not (or (symbol? (get f 'key))  (string? (get f 'key))))))

(define (fix-mnemonics f suffix) (store! f '%mnemonics (glom (downcase (get f 'key)) suffix)))

;;(fix-key (get-to-fix))
