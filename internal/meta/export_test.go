package meta

// ServeTxn registers the OpMetaTxn handler on a replica built from cfg, which
// no production configuration can do.
func ServeTxn(cfg *Config) { cfg.serveTxn = true }
