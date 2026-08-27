{ nixCached, module }:
{ pkgs, ... }:
let
  fixture = pkgs.hello;
  # file:// binary cache with signed narinfos, store-paths list and
  # public key
  origin =
    pkgs.runCommand "origin-cache"
      {
        nativeBuildInputs = [ pkgs.nix ];
        unsigned = pkgs.mkBinaryCache { rootPaths = [ fixture ]; };
      }
      ''
        export HOME=$TMPDIR
        mkdir -p $out
        cp -r $unsigned $out/cache
        chmod -R u+w $out/cache
        nix key generate-secret --extra-experimental-features nix-command \
          --key-name cache-test-1 > sk
        nix key convert-secret-to-public --extra-experimental-features nix-command \
          < sk > $out/pk
        nix store sign --extra-experimental-features nix-command \
          --store "file://$out/cache" --key-file sk --recursive ${fixture}
        grep -h '^StorePath: ' $out/cache/*.narinfo | cut -d' ' -f2 \
          > $out/cache/store-paths
      '';
  # relay certificate so the seeder relay serves HTTPS and QAD
  relayCert = pkgs.runCommand "relay-cert" { nativeBuildInputs = [ pkgs.openssl ]; } ''
    mkdir $out
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
      -subj /CN=seeder -addext "subjectAltName=DNS:seeder" -days 3650 \
      -keyout $out/key.pem -out $out/cert.pem
  '';
  common = {
    imports = [ module ];
    virtualisation.vlans = [ 1 ];
    networking.firewall.allowedUDPPorts = [ 8322 ];
    environment.systemPackages = [
      pkgs.curl
      pkgs.nix
    ];
    services.nix-cached = {
      enable = true;
      package = nixCached;
      upstream = "http://origin";
      catalogUrls = [ "http://origin/store-paths" ];
      syncEvery = "2s";
      p2pPort = 8322;
      relays = [ "https://seeder:3340" ];
      relayCa = "${relayCert}/cert.pem";
      environmentFiles = [ "/run/nix-cached.env" ];
    };
    # started from the test script, which knows the signing key and
    # the seeder's peer address
    systemd.services.nix-cached.wantedBy = pkgs.lib.mkForce [ ];
  };
in
{
  name = "nix-cached-swarm";

  containers = {
    origin = {
      virtualisation.vlans = [ 1 ];
      networking.firewall.allowedTCPPorts = [ 80 ];
      services.nginx = {
        enable = true;
        virtualHosts."origin".root = "${origin}/cache";
      };
    };
    seeder = {
      imports = [ common ];
      networking.firewall.allowedTCPPorts = [ 3340 ];
      networking.firewall.allowedUDPPorts = [ 3340 ];
      services.nix-cached = {
        seed = true;
        serveRelay = ":3340";
        relayUrl = "https://seeder:3340";
        relayCert = "${relayCert}/cert.pem";
        relayKey = "${relayCert}/key.pem";
      };
    };
    leaf1 = common;
    # budget below the closure's NAR size
    leaf2 = {
      imports = [ common ];
      services.nix-cached.budgetBytes = 8 * 1024 * 1024;
    };
    # no UDP in or out: only reachable through the seeder's relay
    leaf3 = { lib, ... }: {
      imports = [ common ];
      networking.firewall.allowedUDPPorts = lib.mkForce [ ];
      networking.firewall.extraCommands = ''
        iptables -A OUTPUT -p udp -m multiport --dports 8322,3340 -j DROP
        ip6tables -A OUTPUT -p udp -m multiport --dports 8322,3340 -j DROP
      '';
    };
  };

  testScript = ''
    import shlex

    start_all()
    origin.wait_for_unit("nginx.service")
    pub = origin.succeed("cat ${origin}/pk").strip()

    hp = "${fixture}".removeprefix("/nix/store/")[:32]

    def start_cached(machine, extra=""):
        machine.succeed(
            f"echo NIX_CACHED_EXTRA_FLAGS='-trusted-key {pub} {extra}' > /run/nix-cached.env",
            "systemctl start nix-cached",
        )
        machine.wait_for_open_port(8321)
        machine.wait_until_succeeds(
            f"grep -q {hp} /var/lib/nix-cached/catalog", timeout=30
        )

    start_cached(seeder)
    # wait until the seeder has ingested the whole catalog
    seeder.wait_until_succeeds(
        "for p in $(cat /var/lib/nix-cached/catalog); do"
        " curl -fsS -o /dev/null http://127.0.0.1:8321/$p.narinfo || exit 1; done",
        timeout=120,
    )

    peer_id = seeder.succeed(
        "journalctl -u nix-cached --grep 'swarm endpoint' -o cat | grep -o 'id=[0-9a-f]*' | tail -1"
    ).strip().removeprefix("id=")
    peer = f"{peer_id}@seeder:8322"

    start_cached(leaf1, f"-peer {peer}")
    start_cached(leaf2, f"-peer {peer}")
    start_cached(leaf3, f"-peer {peer}")

    # leaf1 and leaf2 only know the seeder and find each other via gossip
    leaf1.wait_until_succeeds(
        "curl -fsS --unix-socket /var/lib/nix-cached/admin.sock http://x/metrics | grep -x 'nix_cached_known_peers [3-9]'",
        timeout=60,
    )

    def substitute(machine):
        machine.succeed(
            "nix --extra-experimental-features nix-command copy"
            " --from http://127.0.0.1:8321 --to 'local?root=/root/dest'"
            f" --option trusted-public-keys {shlex.quote(pub)}"
            " --option narinfo-cache-negative-ttl 0"
            " ${fixture}",
            "cmp ${fixture}/bin/hello /root/dest${fixture}/bin/hello",
        )

    # leaf1 fetches while the origin is up
    substitute(leaf1)

    # leaf2 must get everything from the other nodes
    origin.succeed("systemctl stop nginx")
    substitute(leaf2)

    # leaf1 and leaf2 have UDP and must be on direct paths only
    metrics = leaf1.succeed("curl -fsS --unix-socket /var/lib/nix-cached/admin.sock http://x/metrics")
    assert 'nix_cached_swarm_peers{path="direct"} 0' not in metrics, metrics

    # leaf3 has no UDP path and must come through the relay
    substitute(leaf3)
    metrics = leaf3.succeed("curl -fsS --unix-socket /var/lib/nix-cached/admin.sock http://x/metrics")
    assert 'nix_cached_swarm_peers{path="direct"} 0' in metrics, metrics
    assert 'nix_cached_swarm_peers{path="relay"} 0' not in metrics, metrics
    seeder.succeed(
        "curl -fsS --unix-socket /var/lib/nix-cached/admin.sock http://x/metrics"
        " | grep nix_cached_relay_datagrams_forwarded_total | grep -qv ' 0$'"
    )

    # after eviction and GC, leaf2's store is far below the full cache
    leaf2.succeed(
        "curl -fsS -X POST --unix-socket /var/lib/nix-cached/admin.sock http://gc/-/gc"
    )
    store = int(leaf2.succeed("du -sb /var/lib/nix-cached/store | cut -f1"))
    full = int(leaf2.succeed("du -sb ${origin}/cache | cut -f1"))
    assert store < full // 2, f"store {store} not evicted below half of {full}"
  '';
}
