![Plugin Icon](assets/icon.png)

# OpenDeck FIFINE Ampligame D6 Plugin

An unofficial plugin for FIFINE Ampligame D6 device

This is a maintained fork of [shugotekitten/opendeck-ampgd6](https://github.com/shugotekitten/opendeck-ampgd6) focused on responsiveness and rendering reliability:

- Corruption-free key images: all HID writes are serialized, so keepalive or brightness commands can no longer land in the middle of an image upload
- Keepalive heartbeat so the first press after idling is not swallowed
- Batched, cached and parallel image rendering for fast page switches
- Recovery from lost button press/release reports
- Anti-aliased rounded key corners and sharper icon scaling

## OpenDeck version

Requires OpenDeck 2.5.0 or newer

## Supported devices

- FIFINE Ampligame D6 (3142:0007)

## Known issues

- The D6 currently behaves like a protocol v1 device for identity purposes, so using two identical D6 units at the same time is not supported.
- If you used a pre-`0.2.0` build of this plugin, OpenDeck may not reuse old page bindings automatically because the device namespace changed from `99` to `d6`.
- A key held for more than 60 seconds is automatically released, as a safety net for lost release reports.

## Platform support

- Linux: Guaranteed, if stuff breaks - I'll probably catch it before public release
- Mac: Best effort, no tests before release, things may break, but I probably have means to fix them
- Windows: Zero effort, no tests before release, if stuff breaks - too bad, it's up to you to contribute fixes

## Installation

1. Download an archive from [releases](https://github.com/ciscosweater/opendeck-ampgd6/releases)
2. In OpenDeck: Plugins -> Install from file
3. Linux: Download [udev rules](./40-opendeck-ampgd6.rules) and install them by copying into `/etc/udev/rules.d/` and running `sudo udevadm control --reload-rules`
4. Unplug and plug again the device, restart OpenDeck

## Device specifications

- Layout: 3 rows × 5 columns (15 buttons)
- Protocol version: 1

## Troubleshooting

Set `OPENDECK_AMPGD6_LOG=debug` (or `trace`) in the environment OpenDeck runs in to get verbose plugin logs, including every raw button report.

## Building

### Prerequisites

You'll need:

- A Linux OS of some sort
- Docker (release builds for all platforms run in a pinned `cargo-zigbuild` image)
- [just](https://just.systems)
- Rust 1.87 and up, only for local development (`cargo test`, `cargo clippy`)

### Building a release package

```sh
$ just package
```

## Acknowledgments

Forked from [shugotekitten/opendeck-ampgd6](https://github.com/shugotekitten/opendeck-ampgd6) by Cyrille Babon, which is itself based on the akp153 plugin by Andrey Viktorov.

This plugin is heavily based on work by contributors of [elgato-streamdeck](https://github.com/streamduck-org/elgato-streamdeck) crate
