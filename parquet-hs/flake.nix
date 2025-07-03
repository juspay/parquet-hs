{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = inputs@{ self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        extendedGhc =
          let
            ghc = pkgs.haskell.packages.ghc96;
          in ghc.extend (import ./overlay.nix { inherit pkgs; });

      in {
        devShells.default = (extendedGhc).shellFor {
          packages = p: [ p.parquet ];
          withHoogle = true;
          buildInputs = [ pkgs.cabal-install pkgs.ghcid ];
        };
        packages.extendedGhc = extendedGhc;
      });
}
