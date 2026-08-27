{
  description = "github.com/draganm/amber-store";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

    systems.url = "github:nix-systems/default";

  };

  outputs = { self, nixpkgs, systems, ... }@inputs:
    let
      eachSystem = f:
        nixpkgs.lib.genAttrs (import systems)
        (system: f system nixpkgs.legacyPackages.${system});
    in {

      packages = eachSystem (system: pkgs: {
        nix-cached = pkgs.buildGoModule {
          pname = "nix-cached";
          version = "0";
          src = self;
          vendorHash = "sha256-swCDPmI/DD/jcxYI2HfWqGtrLiINPp27YCW+yIYzync=";
          subPackages = [ "cmd/nix-cached" ];
          env.CGO_ENABLED = 0;
        };
      });

      nixosModules.nix-cached = ./nix/module.nix;

      checks = eachSystem (system: pkgs:
        nixpkgs.lib.optionalAttrs (pkgs.stdenv.isLinux) {
          swarm = pkgs.testers.runNixOSTest
            (import ./nix/tests/swarm.nix {
              nixCached = self.packages.${system}.nix-cached;
              module = ./nix/module.nix;
            });
        });

      devShells = eachSystem (system: pkgs: {
        default = pkgs.mkShell {
          shellHook = ''
            # Set here the env vars you want to be available in the shell
          '';
          hardeningDisable = [ "all" ];

          # nodejs builds the embedded admin SPA (go generate ./cmd/amber-store)
          packages = with pkgs; [ go nodejs ];
        };
      });
    };
}
