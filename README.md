GoQue - Resque implementation in Go
-------------------------------------

> This is a work in progress

Similar to [goworker](github.com/benmanns/goworker), but built as a library, rather than imposing a framework for your binary.

## Benefits

It doesn't impose configuration via flags. You handle your own configuration parsing and pass them to `goque`

It is more up to date with how resque registers workers and the resque protocol. At least that is my intention.

It doesn't (won't) do its own logging. I want logging to be pluggable. More of this to come.

## Left To Do

1. More Tests!
2. Lighten the consuming load
3. Pluggable logging
4. Variadic options
