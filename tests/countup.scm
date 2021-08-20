(define testbase (config 'testbase "tmpcountup"))

(define (main (donefile (glom testbase ".done")) (count 20) (interval 1))
  (config! 'appid (stringout "countup " donefile))
  (dotimes (i count) (sleep interval) (message "Counting " i))
  (when (config 'error #f) (+ 3 'a))
  (notify "Finished counting up to " count)
  (display (config 'sessionid) (open-output-file donefile)))



