package upstream

import "crypto/tls"

// SetHandoutHook installs f on u's transport; it runs first in every connection handout. Call before any request.
func SetHandoutHook(u *Upstream, f func()) { u.Transport.onHandout = f }

// SetClientTLS sets the base client TLS settings of u's transport. Call before any request.
func SetClientTLS(u *Upstream, cfg *tls.Config) { u.Transport.tlsConfig = cfg }
