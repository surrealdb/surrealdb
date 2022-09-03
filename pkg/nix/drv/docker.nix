{ cacert, dockerTools, package, util }:

dockerTools.buildLayeredImage {
  name = "surrealdb/surrealdb";
  # Unfortunately Docker doesn't support semver's `+` so we are using `_` instead
  tag = "v${builtins.replaceStrings [ "+" ] [ "_" ] util.version}";
  config = {
    Env = [ "SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt" ];
    WorkingDir = "/";
    Entrypoint = "${package}/bin/${util.packageName}";
  };
}
