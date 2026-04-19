# Security policy

## Reporting a vulnerability

If you find a security issue in `trajectory`, please report it by email to
**aero.gemini.accout@gmail.com** with `[trajectory security]` in the subject.

Please do not open a public GitHub issue for suspected vulnerabilities.

I aim to acknowledge reports within 72 hours and to publish a fix or a
mitigation timeline within two weeks. There is no bug bounty.

## Scope

In scope:
- The Rust crate in this repository.
- The SQL schema files under [`sql/`](sql/).
- The systemd unit under [`systemd/`](systemd/).

Out of scope:
- Third-party upstream data feeds (OpenSky, adsb.lol, etc.).
- Homelab deployment details (see [ARCHITECTURE.md §6](ARCHITECTURE.md) for
  the threat model).

## Hardening notes

The production deployment runs under a restricted systemd sandbox; see
[`systemd/trajectory.service`](systemd/trajectory.service) and the README
`Operations` section for the full profile. `systemd-analyze security`
reports an exposure of 1.5 against this unit.
