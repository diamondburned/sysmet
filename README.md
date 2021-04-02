# sysmet

A no-JavaScript CGI-based lightweight system metrics collector and frontend,
effectively a very tiny version of the Grafana + Telegraf + InfluxDB stack.

## Note to Self

If I ever ponder about using BadgerDB over bbolt: don't, simply don't. Bbolt
allows concurrent read opens AND (!!!) waits until the exclusive writer is done
WITHOUT needing write permissions.
