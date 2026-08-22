{ config, lib, pkgs, ... }:
let
  cfg = config.services.nix-cached;
  flags =
    [ "-dir" "/var/lib/nix-cached" "-listen" cfg.listen "-upstream" cfg.upstream ]
    ++ lib.concatMap (u: [ "-catalog-url" u ]) cfg.catalogUrls
    ++ lib.concatMap (p: [ "-peer" p ]) cfg.peers
    ++ lib.optionals (cfg.p2pPort != null) [ "-p2p-port" (toString cfg.p2pPort) ]
    ++ lib.concatMap (k: [ "-trusted-key" k ]) cfg.trustedKeys
    ++ lib.optionals (cfg.syncEvery != null) [ "-sync-every" cfg.syncEvery ]
    ++ lib.optionals (cfg.catalogTtl != null) [ "-catalog-ttl" cfg.catalogTtl ]
    ++ lib.optionals (cfg.budgetBytes != null) [ "-budget-bytes" (toString cfg.budgetBytes) ]
    ++ lib.optionals (cfg.peerByteRate != null) [ "-peer-byte-rate" (toString cfg.peerByteRate) ]
    ++ lib.optional cfg.seed "-seed";
in
{
  options.services.nix-cached = {
    enable = lib.mkEnableOption "nix-cached p2p binary cache";
    package = lib.mkPackageOption pkgs "nix-cached" { };
    listen = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:8321";
      description = "Substituter listen address.";
    };
    upstream = lib.mkOption {
      type = lib.types.str;
      default = "https://cache.nixos.org";
      description = "Upstream cache URL.";
    };
    catalogUrls = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "store-paths list URLs.";
    };
    peers = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Peers as <endpointid>@host:port or endpoint tickets.";
    };
    p2pPort = lib.mkOption {
      type = lib.types.nullOr lib.types.port;
      default = null;
      description = "Swarm UDP port (8322 when peering). Setting it enables peering without static peers.";
    };
    trustedKeys = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Trusted narinfo signing keys (default: cache.nixos.org-1).";
    };
    syncEvery = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Catalog and peer sync interval.";
    };
    catalogTtl = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Drop paths this long after they leave the catalog.";
    };
    budgetBytes = lib.mkOption {
      type = lib.types.nullOr lib.types.int;
      default = null;
      description = "Total NAR-size budget; evicts to fit.";
    };
    peerByteRate = lib.mkOption {
      type = lib.types.nullOr lib.types.int;
      default = null;
      description = "Peer-serving bandwidth cap, bytes/second.";
    };
    seed = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Eagerly ingest every catalogued path.";
    };
    environmentFiles = lib.mkOption {
      type = lib.types.listOf lib.types.path;
      default = [ ];
      description = "EnvironmentFiles; NIX_CACHED_EXTRA_FLAGS is appended to the command line.";
    };
    credentials = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      example = [ "peers:/run/secrets/nix-cached-peers" ];
      description = "LoadCredential entries; files land in $CREDENTIALS_DIRECTORY.";
    };
    gc = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Periodic garbage collection via the admin socket.";
      };
      dates = lib.mkOption {
        type = lib.types.str;
        default = "daily";
        description = "systemd OnCalendar expression.";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.services.nix-cached = {
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      serviceConfig = {
        ExecStart = "${lib.getExe' cfg.package "nix-cached"} ${lib.escapeShellArgs flags} $NIX_CACHED_EXTRA_FLAGS";
        EnvironmentFile = cfg.environmentFiles;
        LoadCredential = cfg.credentials;
        DynamicUser = true;
        StateDirectory = "nix-cached";
        Restart = "on-failure";
      };
    };

    systemd.services.nix-cached-gc = lib.mkIf cfg.gc.enable {
      serviceConfig = {
        Type = "oneshot";
        ExecStart = "${lib.getExe pkgs.curl} -fsS -X POST --unix-socket /var/lib/nix-cached/admin.sock http://gc/-/gc";
      };
    };
    systemd.timers.nix-cached-gc = lib.mkIf cfg.gc.enable {
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = cfg.gc.dates;
        Persistent = true;
      };
    };
  };
}
