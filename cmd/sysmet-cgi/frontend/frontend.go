package frontend

import (
	"embed"
	"html/template"
	"log"

	"io/fs"

	"net/http"

	"github.com/diamondburned/tmplutil"
)

//go:embed *
var webFS embed.FS

var Templater = tmplutil.Templater{
	FileSystem: webFS,
	Includes: map[string]string{
		"logo":   "components/logo/logo.html",
		"metric": "components/metric/graph.html",
		"errbox": "components/errbox/errbox.html",
		"rawcss": "static/index.css",
	},
	Functions: template.FuncMap{},
}

func init() {
	// tmplutil.Log = true
}

// MountStatic mounts a static HTTP handler.
func MountStatic() http.Handler {
	sub, err := fs.Sub(webFS, "static")
	if err != nil {
		log.Panicln("failed to get static:", err)
	}

	return http.FileServer(http.FS(sub))
}
