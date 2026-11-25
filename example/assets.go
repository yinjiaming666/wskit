// Package example embeds the browser demo served by the example gateway.
package example

import "embed"

//go:embed client/index.html client/client.js
var ClientFiles embed.FS
