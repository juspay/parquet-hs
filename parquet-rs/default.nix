{pkgs,...}:

let
  cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
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
    cargoLock.lockFile = ./Cargo.lock;
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
