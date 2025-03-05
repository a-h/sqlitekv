{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    version = {
      url = "github:a-h/version";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    xc = {
      url = "github:joerdav/xc";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, version, xc }:
    let
      allSystems = [
        "x86_64-linux" # 64-bit Intel/AMD Linux
        "aarch64-linux" # 64-bit ARM Linux
        "x86_64-darwin" # 64-bit Intel macOS
        "aarch64-darwin" # 64-bit ARM macOS
      ];

      forAllSystems = f: nixpkgs.lib.genAttrs allSystems (system: f {
        system = system;
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            (final: prev: {
              rqlite = prev.rqlite.overrideAttrs (oldAttrs: {
                version = "8.36.4";
                src = prev.fetchFromGitHub {
                  owner = "rqlite";
                  repo = "rqlite";
                  rev = "v8.36.4";
                  hash = "sha256-kscSjT83wLiTgY6fOuvP2KnIXDTwmgHIAvNRq4IMawg=";
                };
                vendorHash = "sha256-lMDE8M8O6HIJE585OaI1islvffVHncr5CwLoVVSCOh4=";
              });
              version = version.packages.${system}.default;
              xc = xc.packages.${system}.xc;
            })
          ];
        };
      });

      # Build Docker container.
      dockerUser = pkgs: pkgs.runCommand "user" { } ''
        mkdir -p $out/etc
        echo "user:x:1000:1000:user:/home/user:/bin/false" > $out/etc/passwd
        echo "user:x:1000:" > $out/etc/group
        echo "user:!:1::::::" > $out/etc/shadow
      '';
      rqliteDockerImage = { pkgs, system }: pkgs.dockerTools.buildImage {
        name = "rqlite";
        tag = "latest";

        copyToRoot = [
          pkgs.coreutils
          pkgs.bash
          (dockerUser pkgs)
          pkgs.rqlite
        ];
        config = {
          Entrypoint = [ "rqlited" "-http-addr" "0.0.0.0:4001" "-http-adv-addr" "rqlite.sqlitekv.svc.cluster.local:4001" "-raft-addr" "0.0.0.0:4002" "-raft-adv-addr" "rqlite.sqlitekv.svc.cluster.local:4002" "-auth" "/mnt/rqlite/auth.json" "/mnt/data" ];
          User = "user:user";
          ExposedPorts = {
            "4001/tcp" = { };
            "4002/tcp" = { };
            "4003/tcp" = { };
          };
          Volumes = {
            "/rqlite/file" = { };
          };
        };
      };

      # Development tools used.
      devTools = { system, pkgs }: [
        pkgs.sqlite
        pkgs.crane
        pkgs.gh
        pkgs.git
        pkgs.go
        pkgs.gopls
        pkgs.xc
        pkgs.version
        # Database tools.
        pkgs.rqlite # Distributed sqlite.
      ];
    in
    {
      # `nix build .#rqlite-docker-image` builds the Docker container.
      packages = forAllSystems ({ system, pkgs }: {
        rqlite-docker-image = rqliteDockerImage { pkgs = pkgs; system = system; };
      });
      # `nix develop` provides a shell containing required tools.
      devShells = forAllSystems ({ system, pkgs }: {
        default = pkgs.mkShell {
          buildInputs = (devTools { system = system; pkgs = pkgs; });
        };
      });
    };
}
