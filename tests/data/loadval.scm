;;; -*- Mode: Scheme; -*-

(define (%postload) (message "We're loaded"))

(message "Loaded load5")

(cons (config 'sessionid) (qc (getfiles (dirname (get-source)))))



