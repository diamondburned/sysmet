package errbox

import (
	"embed"
	"html/template"
	"io"
)

var (
	//go:embed *.html
	htmls embed.FS
	tmpls *template.Template
)

func init() {
	tmpls = template.New("errbox")
	tmpls = template.Must(tmpls.ParseFS(htmls, "*"))
}

// Render renders the error box.
func Render(w io.Writer, err error) error {
	return tmpls.ExecuteTemplate(w, "errbox", err)
}
