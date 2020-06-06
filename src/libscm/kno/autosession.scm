;;; -*- Mode: Scheme; -*-

(in-module 'kno/autosession)

(use-module '{kno/sessions})

;;;; This loads a session defined in the current directory

(unless (or *session* (config 'quick) (config 'session)
	    (not (config 'autosession #t config:boolean)))
  (cond ((file-directory? (abspath ".knoc"))
	 (config! 'session (abspath ".knoc")))
	((file-directory? (abspath "_knoc"))
	 (config! 'session (abspath "_knoc")))))

