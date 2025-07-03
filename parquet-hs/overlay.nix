{pkgs, ...}:
hfinal: hprev:
with pkgs.haskell.lib.compose;
let
  parquetrs = (import ./../parquet-rs { inherit pkgs; }).parquetrs;
in {
  parquet-hs  = hfinal.callCabal2nix "parquet-hs" (pkgs.lib.cleanSource ./.) {inherit parquetrs;};
}
