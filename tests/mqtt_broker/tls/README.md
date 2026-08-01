# Disposable TLS inputs

`scripts/run_mqtt_validation_tests.sh` creates a one-run CA, private server key,
and server certificate in its validated temporary directory. The certificate
contains `DNS:localhost` and `IP:127.0.0.1` subject alternative names and the
`serverAuth` extended key usage.

No generated certificate or private key belongs in this fixture directory.
