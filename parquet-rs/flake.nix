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
        parquet-rs-outputs = import ./. {inherit pkgs;};
        parquet-rs = parquet-rs-outputs.parquet-rs;
        parquet-rs-dev-shell = parquet-rs-outputs.parquet-rs_dev;
        parquetPkgs =
          hpkgs: hpkgs.callCabal2nix "parquet" ./../parquet { inherit parquet-rs; };
        ghcPkgsWithParquetRs =
          ghcPkgs.extend(hfinal: hprev:
            { parquet = parquetPkgs hfinal; });
        shellDeps =
          with ghcPkgs; [ cabal-install ghcid haskell-language-server ];
      in
        {
          packages.default = ghcPkgsWithParquetRs.parquet;
          devShells.default = ghcPkgsWithParquetRs.shellFor {
            packages = p: [ p.parquet ];
            withHoogle = true;
            buildInputs = shellDeps;
          };
        });
}
