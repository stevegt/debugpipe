// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Pipe adapter to connect code expecting an io.Reader
// with code expecting an io.Writer.

package debugpipe

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

func init() {
	var debug string
	debug = os.Getenv("DEBUG")
	if debug == "1" {
		log.SetLevel(log.DebugLevel)
	}
	log.SetReportCaller(true)
	formatter := &log.TextFormatter{
		CallerPrettyfier: caller(),
		FieldMap: log.FieldMap{
			log.FieldKeyFile: "caller",
		},
	}
	formatter.TimestampFormat = "15:04:05.999999999"
	log.SetFormatter(formatter)
}

// caller returns string presentation of log caller which is formatted as
// `/path/to/file.go:line_number`. e.g. `/internal/app/api.go:25`
// https://stackoverflow.com/questions/63658002/is-it-possible-to-wrap-logrus-logger-functions-without-losing-the-line-number-pr
func caller() func(*runtime.Frame) (function string, file string) {
	return func(f *runtime.Frame) (function string, file string) {
		p, _ := os.Getwd()
		return "", fmt.Sprintf("%s:%d gid %d", strings.TrimPrefix(f.File, p), f.Line, getGID())
	}
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

// onceError is an object that will only store an error once.
type onceError struct {
	sync.Mutex // guards following
	err        error
}

func (a *onceError) Store(err error) {
	a.Lock()
	defer a.Unlock()
	if a.err != nil {
		return
	}
	a.err = err
}
func (a *onceError) Load() error {
	a.Lock()
	defer a.Unlock()
	return a.err
}

// ErrClosedPipe is the error used for read or write operations on a closed pipe.
var ErrClosedPipe = errors.New("io: read/write on closed pipe")

// A pipe is the shared pipe structure underlying PipeReader and PipeWriter.
type pipe struct {
	wrMu sync.Mutex // Serializes Write operations
	wrCh chan []byte
	rdCh chan int

	once sync.Once // Protects closing done
	done chan struct{}
	rerr onceError
	werr onceError
}

func (p *pipe) Read(b []byte) (n int, err error) {
	select {
	case <-p.done:
		log.Debugf("pipe Read() done A")
		return 0, p.readCloseError()
	default:
	}

	select {
	case bw := <-p.wrCh:
		nr := copy(b, bw)
		p.rdCh <- nr
		log.Debugf("pipe Read() returning %v, nil", nr)
		return nr, nil
	case <-p.done:
		log.Debugf("pipe Read() done B")
		return 0, p.readCloseError()
	}
}

func (p *pipe) readCloseError() error {
	rerr := p.rerr.Load()
	if werr := p.werr.Load(); rerr == nil && werr != nil {
		log.Debugf("pipe readCloseError() returning %v", werr)
		return werr
	}
	log.Debugf("pipe readCloseError() returning %v", ErrClosedPipe)
	return ErrClosedPipe
}

func (p *pipe) CloseRead(err error) error {
	if err == nil {
		err = ErrClosedPipe
	}
	p.rerr.Store(err)
	p.once.Do(func() { close(p.done) })
	log.Debugf("pipe CloseRead() returning nil")
	return nil
}

func (p *pipe) Write(b []byte) (n int, err error) {
	log.Debugf("pipe Write()\n")
	select {
	case <-p.done:
		log.Debugf("pipe Write done A")
		return 0, p.writeCloseError()
	default:
		p.wrMu.Lock()
		defer p.wrMu.Unlock()
	}

	for once := true; once || len(b) > 0; once = false {
		select {
		case p.wrCh <- b:
			nw := <-p.rdCh
			b = b[nw:]
			n += nw
		case <-p.done:
			log.Debugf("pipe Write done B")
			return n, p.writeCloseError()
		}
	}
	log.Debugf("pipe Write() returning %v, nil", n)
	return n, nil
}

func (p *pipe) writeCloseError() error {
	werr := p.werr.Load()
	if rerr := p.rerr.Load(); werr == nil && rerr != nil {
		log.Debugf("pipe writeCloseError() returning %v", rerr)
		return rerr
	}
	log.Debugf("pipe writeCloseError() returning %v", ErrClosedPipe)
	return ErrClosedPipe
}

func (p *pipe) CloseWrite(err error) error {
	if err == nil {
		err = io.EOF
	}
	p.werr.Store(err)
	p.once.Do(func() { close(p.done) })
	log.Debugf("pipe CloseWrite() returning nil")
	return nil
}

// A PipeReader is the read half of a pipe.
type PipeReader struct {
	p *pipe
}

// Read implements the standard Read interface:
// it reads data from the pipe, blocking until a writer
// arrives or the write end is closed.
// If the write end is closed with an error, that error is
// returned as err; otherwise err is EOF.
func (r *PipeReader) Read(data []byte) (n int, err error) {
	n, err = r.p.Read(data)
	log.Debugf("PipeReader Read() returning %v, %v", n, err)
	return
}

// Close closes the reader; subsequent writes to the
// write half of the pipe will return the error ErrClosedPipe.
func (r *PipeReader) Close() error {
	err := r.CloseWithError(nil)
	log.Debugf("PipeReader Close() returning %v", err)
	return err
}

// CloseWithError closes the reader; subsequent writes
// to the write half of the pipe will return the error err.
//
// CloseWithError never overwrites the previous error if it exists
// and always returns nil.
func (r *PipeReader) CloseWithError(err error) error {
	e := r.p.CloseRead(err)
	log.Debugf("PipeReader CloseWithError() returning %v", e)
	return err
}

// A PipeWriter is the write half of a pipe.
type PipeWriter struct {
	p *pipe
}

// Write implements the standard Write interface:
// it writes data to the pipe, blocking until one or more readers
// have consumed all the data or the read end is closed.
// If the read end is closed with an error, that err is
// returned as err; otherwise err is ErrClosedPipe.
func (w *PipeWriter) Write(data []byte) (n int, err error) {
	n, err = w.p.Write(data)
	log.Debugf("PipeWriter Write() returning %v, %v", n, err)
	return
}

// Close closes the writer; subsequent reads from the
// read half of the pipe will return no bytes and EOF.
func (w *PipeWriter) Close() error {
	err := w.CloseWithError(nil)
	log.Debugf("PipeWriter Close() returning %v", err)
	return err
}

// CloseWithError closes the writer; subsequent reads from the
// read half of the pipe will return no bytes and the error err,
// or EOF if err is nil.
//
// CloseWithError never overwrites the previous error if it exists
// and always returns nil.
func (w *PipeWriter) CloseWithError(err error) error {
	e := w.p.CloseWrite(err)
	log.Debugf("PipeWriter CloseWithError() returning %v", e)
	return e
}

// Pipe creates a synchronous in-memory pipe.
// It can be used to connect code expecting an io.Reader
// with code expecting an io.Writer.
//
// Reads and Writes on the pipe are matched one to one
// except when multiple Reads are needed to consume a single Write.
// That is, each Write to the PipeWriter blocks until it has satisfied
// one or more Reads from the PipeReader that fully consume
// the written data.
// The data is copied directly from the Write to the corresponding
// Read (or Reads); there is no internal buffering.
//
// It is safe to call Read and Write in parallel with each other or with Close.
// Parallel calls to Read and parallel calls to Write are also safe:
// the individual calls will be gated sequentially.
func Pipe() (*PipeReader, *PipeWriter) {
	p := &pipe{
		wrCh: make(chan []byte),
		rdCh: make(chan int),
		done: make(chan struct{}),
	}
	return &PipeReader{p}, &PipeWriter{p}
}
