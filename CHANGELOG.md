# Changelog

## [1.0.0] - 2026-10-16

First release of the [ciscosweater](https://github.com/ciscosweater/opendeck-ampgd6) fork.

### 🚀 Features

- *(device)* Send periodic keepalive so the first press after idling is not lost
- *(renderer)* Split input and render pipelines; batch, cache and decode images in parallel
- *(renderer)* Anti-aliased rounded key corners and sharper icon scaling
- *(inputs)* Recover from lost press/release reports and stuck buttons
- *(main)* Configurable log level via `OPENDECK_AMPGD6_LOG`

### 🐛 Bug Fixes

- *(device)* Serialize HID writes to prevent corrupted key images
- *(device)* Never drop render updates when the queue is full
- *(device)* Prevent duplicate device entries on reconnect
- *(renderer)* Calibrate D6 key image size and orientation

## [0.2.1] - 2026-02-23

### 🚀 Features

- Add build to release

### 🐛 Bug Fixes

- correct cargo edition to 2024
- remove ref of template plugin name

## [0.2.0] - 2026-01-23

### 🚀 Features

- Adapt size of logo for Ampligame D6 - #1
- Change namespace from 99 to d6 to avoid duplication with the akp153 plugin

## [0.1.0] - 2025-11-28

### 🚀 Features

- First version, fork for akp153 Plugin with modification to work with Fifine Ampligame D6
