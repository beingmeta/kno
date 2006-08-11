(define (main (donefile "countup.done") (count 20) (interval 1))
  (dotimes (i count) (sleep interval) (message "Counting " i))
  (when (config 'error #f) (+ 3 'a))
  (notify "Finished counting up to " count)
  (display (config 'sessionid) (open-output-file donefile)))


