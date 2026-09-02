{ pkgs
, cargoDepsPkgs ? null
, ...
}:

let
  cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);

  lockedNixpkgs =
    let
      node = (builtins.fromJSON (builtins.readFile ../flake.lock)).nodes.nixpkgs.locked;
    in
    builtins.fetchTarball {
      url = "https://github.com/${node.owner}/${node.repo}/archive/${node.rev}.tar.gz";
      sha256 = node.narHash;
    };

  depsPkgs =
    if cargoDepsPkgs != null then
      cargoDepsPkgs
    else
      import lockedNixpkgs { inherit (pkgs.stdenv.buildPlatform) system; };

  rust-toolchain = pkgs.symlinkJoin {
    name = "rust-toolchain";
    paths = [ pkgs.rustc pkgs.cargo pkgs.cargo-watch pkgs.rust-analyzer pkgs.rustPlatform.rustcSrc ];
  };
in
{
  # Rust package
  parquetrs = pkgs.rustPlatform.buildRustPackage {
    inherit (cargoToml.package) name version;
    src = ./.;
    cargoDeps = depsPkgs.rustPlatform.importCargoLock { lockFile = ./Cargo.lock; };
    buildInputs = if pkgs.stdenv.isDarwin then [ pkgs.fixDarwinDylibNames ] else [ ];
    postInstall = ''
      ${if pkgs.stdenv.isDarwin then "fixDarwinDylibNames" else ""}
    '';
  };

  # Rust dev environment
  parquetrs-dev = pkgs.mkShell {
    shellHook = ''
      # For rust-analyzer 'hover' tooltips to work.
      export RUST_SRC_PATH=${pkgs.rustPlatform.rustLibSrc}
      export LIBRARY_PATH=./result/lib
    '';
    nativeBuildInputs = with pkgs; [
      just
      rust-toolchain
    ];
    RUST_BACKTRACE = 1;
  };
}
