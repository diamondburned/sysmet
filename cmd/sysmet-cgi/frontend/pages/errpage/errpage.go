package errpage

import (
	"io"

	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
)

var errpage = frontend.Templater.Register("errpage", "pages/errpage/errpage.html")

// Render renders the error page.
func Render(w io.Writer, err error) {
	errpage.Execute(w, err)
}
