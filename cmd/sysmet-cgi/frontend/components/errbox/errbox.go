package errbox

import (
	"html/template"
	"io"
	"strings"

	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
)

var errbox = frontend.Templater.Subtemplate("errbox")

func init() {
	frontend.Templater.OnRenderFail(func(w io.Writer, _ string, err error) {
		errbox.Execute(w, err)
	})
}

// Render renders the given error.
func Render(err error) template.HTML {
	buf := strings.Builder{}
	errbox.Execute(&buf, err)
	return template.HTML(buf.String())
}
