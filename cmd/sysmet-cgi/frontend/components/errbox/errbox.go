package errbox

import (
	"io"

	"git.unix.lgbt/diamondburned/sysmet/cmd/sysmet-cgi/frontend"
)

var errbox = frontend.Templater.Subtemplate("errbox")

func init() {
	frontend.Templater.OnRenderFail(func(w io.Writer, _ string, err error) {
		errbox.Execute(w, err)
	})
}
