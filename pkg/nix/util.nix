{ lib, flake, systems, system }:

rec {
  inherit systems system;

  supportedPlatforms = let
    specDir = builtins.readDir ./spec;
    nixExt = ".nix";
    nixFilesFun = key: val:
      val == "regular" && lib.strings.hasSuffix nixExt key;
    trimNixExtFun = key: val: lib.strings.removeSuffix nixExt key;
    nixFiles = lib.attrsets.filterAttrs nixFilesFun specDir;
  in lib.attrsets.mapAttrsToList trimNixExtFun nixFiles;

  systemPlatforms = let
    matchingPlatforms = target:
      let targetSystem = targetToSystem target;
      in targetSystem == system;
  in builtins.filter matchingPlatforms supportedPlatforms;

  platforms = let
    kvFun = platform: {
      name = platform;
      value = platform;
    };
  in builtins.listToAttrs (map kvFun supportedPlatforms);

  targetToEnv = target:
    let withoutDashes = builtins.replaceStrings [ "-" ] [ "_" ] target;
    in lib.strings.toUpper withoutDashes;

  targetToUpperEnv = target: lib.strings.toUpper (targetToEnv target);

  targetToSystem = with builtins;
    target:
    let
      parts = split "-" target;
      arch = elemAt parts 0;
      prefix = lib.strings.optionalString (arch == "armv7") "l";
      os = elemAt parts 4;
    in "${arch}${prefix}-${os}";

  isNative = target:
    let targetSystem = targetToSystem target;
    in targetSystem == system;

  cpu = with builtins;
    target:
    let
      parts = split "-" target;
      arch = elemAt parts 0;
    in if arch == "armv7" then replaceStrings [ "v7" ] [ "" ] arch else arch;

  cargoToml = with builtins;
    let toml = readFile ../../Cargo.toml;
    in fromTOML toml;

  packageName = cargoToml.package.name;

  features = cargoToml.features;

  buildMetadata = with lib.strings;
    let
      lastModifiedDate = flake.lastModifiedDate or flake.lastModified or "";
      date = builtins.substring 0 8 lastModifiedDate;
      shortRev = flake.shortRev or "dirty";
      hasDateRev = lastModifiedDate != "" && shortRev != "";
      dot = optionalString hasDateRev ".";
    in "${date}${dot}${shortRev}";

  version = with lib.strings;
    let
      hasBuildMetadata = buildMetadata != "";
      plus = optionalString hasBuildMetadata "+";
    in "${cargoToml.workspace.package.version}${plus}${buildMetadata}";

  SURREAL_BUILD_METADATA = buildMetadata;
}
