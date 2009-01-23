;;; -*- Mode: Scheme; -*-

(use-module 'mergeutils)

(define (main file rep base)
  (let ((working (file->dtype file))
	(current (file->dtype rep))
	(base (file->dtype base)))
    (dtype->file (merge-overlays working current base)
		 file)))
