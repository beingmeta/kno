# Time functions

The basic time structure in Kno is the timestamp which comes in two
flavors: simple timestamps which represent moments with a precision of seconds
and complex timestamps which representation moments with varying degrees of
precision (days, seconds, milliseconds, microseconds, etc) and also carry
timezone information.

  * `(timestamp)`  
`(timestamp string)`  
`(timestamp string timezone)`  
`(timestamp timestamp timezone)`  
Returns a timestamp object. Without an argument or with `#f` as an argument,
the timestamp describes the current moment; with an argument, the string is
parsed as an ISO-8601 formatted time, e.g. `1990-01-20T15:00:00-5:00`
describes the 20th of January, 1990 at 3pm in the afternoon (Easter Standard
Time) while `1990-01-20T20:00:00GMT` describes the same moment in Greenwich
Mean Time. When the timezone argument is provided it either changes the
timezone of the first argument (keeping the moment the same) or is used in
interpreting it. For example, (timestamp "199O-01-20T15:00:00EST" "GMT") would
return a timestamp which prints out as `1990-01-20T20:00:00UTC`.

  * `(xtimestamp)`  
`(xtimestamp precision)`  
`(xtimestamp timestamp precision)`  
Returns a timestamp object with a particular precision. With no arguments, it
returns a timestamp with the greatest possible precision; with one argument,
it returns a timestamp with a particular precision (providing that timestamp
is Precision can be a symbol `year`, `month`, `day`, `hour`, `minute`,
`second`, `millisecond`, `microsecond`, or `nanosecond`. For example,
(xtimestamp #f 'millisecond) returns something like .

  * `(get-month)  
(get-month timestamp)`returns a symbol denoting the current month or the month
of a particular timestamp (in the local timezone), e.g. `(get-month) ==>
MARCH`.

  * `(get-year)` Gets the current year (AD), e.g. `(get-year) ==> 1997`
  * `(get-hour)` Gets the current hour (hours since midnight), e.g. `(get-hour) ==> 14`
  * `(get-season)` Gets the current season, being ambiguous on the edges, e.g. `(get-season) ==> {winter spring}`
  * `(get-day)` returns a symbol describing the current day of the week, e.g. `(get-day) ==> THURSDAY`
  * `(get-daytime)` Returns a symbol describing the current time of day, being ambiguous on the edges, e.g. `(get-daytime) ==> {afternoon evening}`

