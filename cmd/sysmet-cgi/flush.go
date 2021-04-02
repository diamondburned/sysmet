package main

import "net/http"

// AlwaysFlush is the middleware to always flush after a write.
func AlwaysFlush(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(flushWriter{w, flusher}, r)
	})
}

type flushWriter struct {
	http.ResponseWriter
	flusher http.Flusher
}

func (f flushWriter) Write(b []byte) (int, error) {
	n, err := f.ResponseWriter.Write(b)
	if err != nil {
		return n, err
	}

	f.flusher.Flush()
	return n, nil
}
