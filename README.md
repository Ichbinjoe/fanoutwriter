# FanoutWriter

FanoutWriter is a Golang package that implements `io.WriteCloser` and allows for
multiple `io.ReadCloser`s to be created which may read the data which is written
to the FanoutWriter at a speed independent of other `io.ReadCloser`s.  This is
ideal for a single data source which must broadcast data to multiple readers
who, for example, may be sitting over a variable speed network connection.

## Documentation

Documentation for the FanoutWriter may be found on
[godoc.org](https://godoc.org/github.com/ichbinjoe/fanoutwriter)

## License

This code is licensed using the MIT License, which may be found here:
[LICENSE](./LICENSE).
