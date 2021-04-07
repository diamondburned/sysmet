package errpage

import (
	"io"
	"net/http"

	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
)

var errpage = frontend.Templater.Register("errpage", "pages/errpage/errpage.html")

// Render renders the error page.
func Render(w io.Writer, err error) {
	errpage.Execute(w, err)
}

// Respond responds to a ResponseWriter with the given code and error.
func Respond(w http.ResponseWriter, code int, err error) {
	w.WriteHeader(code)
	Render(w, err)
}
