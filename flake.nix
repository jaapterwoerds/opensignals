{
  inputs = {
    mach-nix.url = "mach-nix/3.5.0";
  };

  outputs = {self, nixpkgs, mach-nix }@inp:
    let
      l = nixpkgs.lib // builtins;
      supportedSystems = [ "x86_64-linux" "aarch64-darwin" ];
      forAllSystems = f: l.genAttrs supportedSystems
        (system: f system (import nixpkgs {inherit system;}));
    in
    {
      # enter this python environment by executing `nix shell .`
      defaultPackage = forAllSystems (system: pkgs: mach-nix.lib."${system}".mkPython {
        requirements = ''
            docopt>=0.6.2
            pandas>=1.2.2
            numpy>=1.21.4
            pyarrow>=6.0.1
            requests>=2.25.1
            tqdm>=4.61.2
            ipython
        '';
      });
    };
}
