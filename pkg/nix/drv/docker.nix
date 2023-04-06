{ cacert, dockerTools, package, util }:

dockerTools.buildLayeredImage {
  name = "surrealdb/surrealdb";
  # Unfortunately Docker doesn't support semver's `+` so we are using `-` instead
  tag = "v${builtins.replaceStrings [ "+" ] [ "-" ] util.version}";
  config = {
    Env = [ "SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt" ];
    WorkingDir = "/";
    Entrypoint = "${package}/bin/${util.packageName}";
  };
}
