{
  description = "A very basic flake";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = inputs@{ self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        ghcPkgs = pkgs.haskell.packages.ghc963;
        parquet-rs-outputs = import ./parquet-rs {inherit pkgs;};
        parquetrs = parquet-rs-outputs.parquetrs;
        parquet-rs-dev-shell = parquet-rs-outputs.parquetrs-dev;
        parquetPkgs =
          hpkgs: hpkgs.callCabal2nix "parquet-hs" ./parquet-hs { inherit parquetrs; };
        ghcPkgsWithParquetRs =
          ghcPkgs.extend(hfinal: hprev:
            { parquetrs = parquetPkgs hfinal; });
        shellDeps =
          with ghcPkgs; [ cabal-install ghcid haskell-language-server parquetrs ];
        haskellShell =
          ghcPkgsWithParquetRs.shellFor {
            packages =
              p:
              [
                p.parquetrs
              ];
            withHoogle = true;
            genericBuilderArgsModifier = args: args // { doHaddock = true; };
            buildInputs = shellDeps;
          };
      in
        {
          packages.default = ghcPkgsWithParquetRs.parquetrs;
          devShells.default = pkgs.mkShell {
            inputsFrom = [haskellShell parquet-rs-dev-shell];
          };
          # devShells.default = ghcPkgsWithParquetRs.shellFor {
          #   packages = p: [ p.parquetrs ];
          #   withHoogle = true;
          #   buildInputs = [shellDeps parquet-rs-dev-shell];
          # };
          # devShells.default = parquet-rs-dev-shell;

        });
}
