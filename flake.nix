{
  description = "otelview-postgres — PostgreSQL remote storage for traces, logs and metrics (otelview and Jaeger v2 compatible)";

  nixConfig = {
    extra-substituters = [ "https://otelview.cachix.org" ];
    extra-trusted-public-keys = [
      "otelview.cachix.org-1:+Twrf64f2rg+cTAYU2MikV/hGMpHxnHK6l6yLvrseP4="
    ];
  };

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, crane }:
    let
      systems = [ "aarch64-darwin" "x86_64-linux" "aarch64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system:
        f (import nixpkgs { inherit system; }));
    in
    {
      packages = forAllSystems (pkgs:
        let
          inherit (pkgs) lib;
          craneLib = crane.mkLib pkgs;

          # Keep proto files (tonic codegen inputs) and the embedded
          # migrations alongside the cargo sources.
          src = lib.cleanSourceWith {
            src = ./.;
            filter = path: type:
              (craneLib.filterCargoSources path type)
              || (lib.hasSuffix ".proto" path)
              || (lib.hasSuffix ".sql" path);
          };

          commonArgs = {
            inherit src;
            pname = "otelview-postgres";
            version = "0.1.2";
            strictDeps = true;
            nativeBuildInputs = [ pkgs.protobuf ];
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };

          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          otelview-postgres = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
            meta = {
              description = "PostgreSQL remote storage for traces, logs and metrics — otelview and Jaeger v2 compatible";
              homepage = "https://github.com/tsirysndr/otelview-postgres";
              license = lib.licenses.mit;
              mainProgram = "otelview-postgres";
            };
          });
        in
        {
          inherit otelview-postgres;
          default = otelview-postgres;
        });

      checks = forAllSystems (pkgs: {
        build = self.packages.${pkgs.stdenv.hostPlatform.system}.otelview-postgres;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            rustc
            cargo
            clippy
            rustfmt
            rust-analyzer
            protobuf
            postgresql
          ];
          PROTOC = "${pkgs.protobuf}/bin/protoc";
        };
      });
    };
}
