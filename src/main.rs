// The release toolchain (cargo-zigbuild image pinned in the justfile) predates
// stable let chains, so keep nested `if let` and silence clippy's suggestion.
#![allow(clippy::collapsible_if)]

use device::{handle_error, handle_set_image, set_brightness};
use mirajazz::device::Device;
use openaction::*;
use std::{collections::HashMap, sync::Arc, sync::LazyLock};
use tokio::sync::RwLock;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use watcher::watcher_task;

#[cfg(not(target_os = "windows"))]
use tokio::signal::unix::{SignalKind, signal};

mod device;
mod inputs;
mod mappings;
mod watcher;

pub static DEVICES: LazyLock<RwLock<HashMap<String, Arc<Device>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
pub static TOKENS: LazyLock<RwLock<HashMap<String, CancellationToken>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
pub static TRACKER: LazyLock<TaskTracker> = LazyLock::new(TaskTracker::new);

struct GlobalEventHandler {}
impl openaction::GlobalEventHandler for GlobalEventHandler {
    async fn plugin_ready(
        &self,
        _outbound: &mut openaction::OutboundEventManager,
    ) -> EventHandlerResult {
        let token = CancellationToken::new();
        TRACKER.spawn(watcher_task(token.clone()));

        TOKENS
            .write()
            .await
            .insert("_watcher_task".to_string(), token);

        log::info!("Plugin initialized");

        Ok(())
    }

    async fn set_image(
        &self,
        event: SetImageEvent,
        _outbound: &mut OutboundEventManager,
    ) -> EventHandlerResult {
        log::debug!("Asked to set image: {:#?}", event);

        // Skip knobs images
        if event.controller == Some("Encoder".to_string()) {
            log::debug!("Looks like a knob, no need to set image");
            return Ok(());
        }

        let id = event.device.clone();

        let device = DEVICES.read().await.get(&event.device).cloned();

        if let Some(device) = device {
            if let Err(err) = handle_set_image(device.as_ref(), event).await {
                handle_error(&id, err).await;
            }
        } else {
            log::error!("Received event for unknown device: {}", event.device);
        }

        Ok(())
    }

    async fn set_brightness(
        &self,
        event: SetBrightnessEvent,
        _outbound: &mut OutboundEventManager,
    ) -> EventHandlerResult {
        log::debug!("Asked to set brightness: {:#?}", event);

        let id = event.device.clone();

        let device = DEVICES.read().await.get(&event.device).cloned();

        if let Some(device) = device {
            if let Err(err) = set_brightness(device.as_ref(), &id, event.brightness).await {
                handle_error(&id, err).await;
            }
        } else {
            log::error!("Received event for unknown device: {}", event.device);
        }

        Ok(())
    }
}

struct ActionEventHandler {}
impl openaction::ActionEventHandler for ActionEventHandler {}

async fn shutdown() {
    let tokens = TOKENS.write().await;

    for (_, token) in tokens.iter() {
        token.cancel();
    }
}

async fn connect() -> EventHandlerResult {
    init_plugin(GlobalEventHandler {}, ActionEventHandler {})
        .await
        .map_err(|error| {
            log::error!("Failed to initialize plugin: {}", error);
            error
        })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn sigterm() -> EventHandlerResult {
    let mut sig = signal(SignalKind::terminate())?;

    sig.recv().await;

    Ok(())
}

#[cfg(target_os = "windows")]
async fn sigterm() -> EventHandlerResult {
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> EventHandlerResult {
    // Set OPENDECK_AMPGD6_LOG=debug (or trace/warn/...) to change verbosity.
    let log_level = std::env::var("OPENDECK_AMPGD6_LOG")
        .ok()
        .and_then(|level| level.parse().ok())
        .unwrap_or(simplelog::LevelFilter::Info);

    simplelog::TermLogger::init(
        log_level,
        simplelog::Config::default(),
        simplelog::TerminalMode::Stdout,
        simplelog::ColorChoice::Never,
    )
    .unwrap_or_else(|err| eprintln!("Failed to initialize logger: {err}"));

    let result = tokio::select! {
        result = connect() => result,
        result = sigterm() => result,
    };

    log::info!("Shutting down");

    shutdown().await;

    log::info!("Waiting for tasks to finish");

    TRACKER.close();
    TRACKER.wait().await;

    log::info!("Tasks are finished, exiting now");

    result
}
