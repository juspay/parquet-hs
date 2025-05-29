{
  inputs.haskell-flake.url = "github:srid/haskell-flake";

  outputs = inputs:
    # Inside `mkFlake`
    {
      imports = [
        inputs.haskell-flake.flakeModule
        ./parquet-ffi
      ];
    };
}
