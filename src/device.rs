use ahash::AHasher;
use data_url::DataUrl;
use image::{
    DynamicImage, GenericImage, ImageBuffer, Rgb, RgbImage, imageops::FilterType,
    load_from_memory_with_format,
};
use lru::LruCache;
use mirajazz::{
    device::Device, error::MirajazzError, images::convert_image_with_format,
    state::DeviceStateUpdate, types::ImageFormat,
};
use openaction::{OUTBOUND_EVENT_MANAGER, SetImageEvent};
use std::{
    array,
    collections::HashMap,
    hash::{Hash, Hasher},
    num::NonZeroUsize,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, mpsc},
    task::spawn_blocking,
};
use tokio_util::sync::CancellationToken;

use crate::{
    DEVICES, TOKENS, TRACKER,
    inputs::{apply_input_event, decode_input_report, ignore_process_input, opendeck_to_device},
    mappings::{
        COL_COUNT, CandidateDevice, ENCODER_COUNT, KEY_COUNT, Kind, ROW_COUNT,
        get_image_format_for_key,
    },
};

const IMAGE_CACHE_LIMIT: usize = 64;
const CLEAR_ALL_BATCH_WINDOW: Duration = Duration::from_millis(8);
const IMAGE_BATCH_WINDOW: Duration = Duration::from_millis(1);
const BUTTON_CORNER_RADIUS: u32 = 16;
// Long enough not to cut off intentional holds (push-to-talk, hold actions);
// a lost release is also recovered as soon as the same key is pressed again.
const STUCK_BUTTON_TIMEOUT: Duration = Duration::from_secs(60);
const HID_READ_TIMEOUT: Duration = Duration::from_secs(1);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(8);
const RENDER_CHANNEL_CAPACITY: usize = 64;
const USB_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

static DEVICE_RENDERERS: LazyLock<Mutex<HashMap<String, Arc<DeviceRenderHandle>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Prebuilt per-kind render assets (button formats + pre-converted black
/// frames). These never change for a given kind, so they are built once and
/// reused across reconnects instead of being re-encoded on every render worker.
#[derive(Clone)]
struct PreparedRenderAssets {
    button_formats: Vec<ImageFormat>,
    black_frames: Vec<Arc<Vec<u8>>>,
}

static RENDER_ASSET_CACHE: LazyLock<Mutex<HashMap<Kind, Arc<PreparedRenderAssets>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// mirajazz only locks the HID writer per report, so a multi-report transaction
/// (an image upload is a `BAT` header + N data chunks + `STP`) can get other
/// commands such as keepalive or brightness spliced into the middle. The
/// firmware then treats those packets as image bytes and the key shows a
/// corrupted frame. Every write path takes this per-device lock so each
/// transaction reaches the device in one piece.
static DEVICE_IO_LOCKS: LazyLock<std::sync::Mutex<HashMap<String, Arc<Mutex<()>>>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

fn device_io_lock(device_id: &str) -> Arc<Mutex<()>> {
    DEVICE_IO_LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(device_id.to_string())
        .or_default()
        .clone()
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RenderCommand {
    SetImage {
        position: u8,
        image_hash: u64,
        payload: Arc<str>,
    },
    ClearButton {
        position: u8,
    },
    ClearAll,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BatchedRenderOp {
    Clear,
    Image { image_hash: u64, payload: Arc<str> },
}

struct DeviceRenderHandle {
    command_tx: mpsc::Sender<RenderCommand>,
    shutdown_token: CancellationToken,
}

type RenderBatch = [Option<BatchedRenderOp>; KEY_COUNT];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ConvertedImageCacheKey {
    image_hash: u64,
    width: usize,
    height: usize,
    mode: u8,
    rotation: u8,
    mirror: u8,
}

/// Initializes a device and listens for events
pub async fn device_task(candidate: CandidateDevice, token: CancellationToken) {
    log::info!("Running device task for {:?}", candidate);
    if candidate.kind.known_singleton_limit() {
        log::info!(
            "Device {} uses a stable synthetic identity; multiple identical D6 units are not supported simultaneously",
            candidate.id
        );
    }
    if !candidate.kind.supports_keepalive() {
        log::debug!("Keepalive is disabled for {}", candidate.id);
    }

    let device = async {
        log::info!("Connecting to device...");
        let device = connect(&candidate).await?;
        log::info!("Device connected successfully");

        if candidate.kind.supports_brightness() {
            log::info!("Setting brightness...");
            if let Err(e) = device.set_brightness(50).await {
                log::warn!(
                    "Failed to set brightness (this may be normal for this device): {}",
                    e
                );
            } else {
                log::info!("Brightness set successfully");
            }
        }

        log::info!("Clearing all button images...");
        if let Err(e) =
            clear_all_button_images_for_kind(&device, &candidate.id, &candidate.kind).await
        {
            log::warn!(
                "Failed to clear all button images during init for device {}: {}",
                candidate.id,
                e
            );
        } else {
            log::info!("Button images cleared successfully");
        }

        log::info!("Flushing device...");
        if let Err(e) = device.flush().await {
            log::warn!(
                "Failed to flush device (this may be normal for this device): {}",
                e
            );
        } else {
            log::info!("Device flushed successfully");
        }

        Ok(device)
    }
    .await;

    let device: Device = match device {
        Ok(device) => device,
        Err(err) => {
            handle_error(&candidate.id, err).await;

            log::error!(
                "Had error during device init, finishing device task: {:?}",
                candidate
            );

            return;
        }
    };

    log::info!("Registering device {}", candidate.id);
    let mut registered = false;
    if let Some(outbound) = OUTBOUND_EVENT_MANAGER.lock().await.as_mut() {
        match outbound
            .register_device(
                candidate.id.clone(),
                candidate.kind.human_name().to_string(),
                ROW_COUNT as u8,
                COL_COUNT as u8,
                ENCODER_COUNT as u8,
                0,
            )
            .await
        {
            Ok(()) => registered = true,
            Err(err) => log::warn!("Failed to register device {}: {}", candidate.id, err),
        }
    }

    if !registered {
        log::error!(
            "Device {} could not be registered with OpenDeck, aborting device task",
            candidate.id
        );
        device.shutdown().await.ok();
        return;
    }

    let device = Arc::new(device);

    DEVICES
        .write()
        .await
        .insert(candidate.id.clone(), device.clone());

    let keepalive_token = CancellationToken::new();
    if candidate.kind.supports_keepalive() {
        log::info!("Starting keepalive for device {}", candidate.id);
        TRACKER.spawn(keepalive_task(
            device.clone(),
            candidate.id.clone(),
            keepalive_token.clone(),
        ));
    }

    tokio::select! {
        _ = device_events_task(&candidate) => {},
        _ = token.cancelled() => {}
    };

    // Stop the heartbeat as soon as the device task is done, regardless of
    // which branch won the select above.
    keepalive_token.cancel();

    log::info!("Shutting down device {:?}", candidate);

    if let Some(device) = DEVICES.read().await.get(&candidate.id).cloned() {
        let io_lock = device_io_lock(&candidate.id);
        let _io = io_lock.lock().await;
        device.shutdown().await.ok();
    }

    log::info!("Device task finished for {:?}", candidate);
}

/// Sends a periodic heartbeat (`CONNECT`) to the device so its firmware does
/// not fall into the low-power idle state. Without this, the first button press
/// after a period of inactivity is consumed while the device wakes up and gets
/// lost, which is why the user has to press twice.
async fn keepalive_task(device: Arc<Device>, device_id: String, token: CancellationToken) {
    let mut interval = tokio::time::interval(KEEPALIVE_INTERVAL);
    // If the runtime is busy and we miss a tick, delay the next one instead of
    // firing a burst of catch-up heartbeats.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let io_lock = device_io_lock(&device_id);

    loop {
        tokio::select! {
            // The first tick completes immediately, acting as an extra wake-up
            // right after connecting.
            _ = interval.tick() => {
                // Wait for any in-flight image upload to finish; the timeout only
                // covers the write itself, not a long render batch.
                let _io = io_lock.lock().await;
                match tokio::time::timeout(USB_WRITE_TIMEOUT, device.keep_alive()).await {
                    Ok(Ok(())) => log::trace!("Keepalive sent to {}", device_id),
                    Ok(Err(err)) => {
                        log::warn!("Keepalive failed for device {}: {}", device_id, err)
                    }
                    Err(_) => log::warn!("Keepalive timed out for device {}", device_id),
                }
            }
            _ = token.cancelled() => break,
        }
    }

    log::debug!("Keepalive stopped for device {}", device_id);
}

/// Sets brightness without interleaving with an in-flight image upload.
pub async fn set_brightness(
    device: &Device,
    device_id: &str,
    brightness: u8,
) -> Result<(), MirajazzError> {
    let io_lock = device_io_lock(device_id);
    let _io = io_lock.lock().await;
    device.set_brightness(brightness).await
}

/// Handles errors, returning true if should continue, returning false if an error is fatal
pub async fn handle_error(id: &String, err: MirajazzError) -> bool {
    log::error!("Device {} error: {}", id, err);

    if matches!(err, MirajazzError::ImageError(_) | MirajazzError::BadData) {
        return true;
    }

    log::info!("Deregistering device {}", id);
    if let Some(outbound) = OUTBOUND_EVENT_MANAGER.lock().await.as_mut() {
        if let Err(err) = outbound.deregister_device(id.clone()).await {
            log::warn!("Failed to deregister device {}: {}", id, err);
        }
    }

    cleanup_device_state(id).await;

    log::info!("Finished clean-up for {}", id);

    false
}

pub async fn cleanup_device_state(id: &str) {
    log::info!("Cancelling tasks for device {}", id);
    if let Some(token) = TOKENS.write().await.remove(id) {
        token.cancel();
    }

    log::info!("Removing device {} from the list", id);
    DEVICES.write().await.remove(id);

    if let Some(renderer) = DEVICE_RENDERERS.lock().await.remove(id) {
        renderer.shutdown_token.cancel();
    }
}

pub async fn connect(candidate: &CandidateDevice) -> Result<Device, MirajazzError> {
    let result = Device::connect(
        &candidate.dev,
        candidate.kind.write_protocol_version(),
        KEY_COUNT,
        ENCODER_COUNT,
    )
    .await;

    match result {
        Ok(device) => Ok(device),
        Err(e) => {
            log::error!("Error while connecting to device: {e}");
            Err(e)
        }
    }
}

/// Handles events from device to OpenDeck
async fn device_events_task(candidate: &CandidateDevice) -> Result<(), MirajazzError> {
    log::info!("Connecting to {} for incoming events", candidate.id);

    let device = DEVICES.read().await.get(&candidate.id).cloned();
    let reader = match device {
        Some(device) => device.get_reader(ignore_process_input),
        None => return Ok(()),
    };

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    TRACKER.spawn(input_dispatch_worker(candidate.id.clone(), event_rx));

    log::info!("Connected to {} for incoming events", candidate.id);
    log::info!(
        "Reader is ready for {} (write pv={}, read pv={})",
        candidate.id,
        candidate.kind.write_protocol_version(),
        candidate.kind.read_protocol_version()
    );

    let mut pressed_buttons = 0u16;
    let mut pressed_since: [Option<Instant>; KEY_COUNT] = [None; KEY_COUNT];

    loop {
        log::debug!("Reading updates...");

        let report = match tokio::time::timeout(HID_READ_TIMEOUT, reader.raw_read_data(512)).await {
            Ok(Ok(report)) => report,
            Ok(Err(e)) => {
                // HID read error — release all pressed buttons to avoid stuck state
                release_all_pressed(
                    &mut pressed_buttons,
                    &mut pressed_since,
                    &event_tx,
                    "hid_read_error",
                );
                if !handle_error(&candidate.id, e).await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            Err(_timeout) => {
                // No data within timeout — check for stuck buttons
                check_stuck_buttons(&mut pressed_buttons, &mut pressed_since, &event_tx);
                continue;
            }
        };

        let updates = match decode_input_report(&report) {
            Ok(Some(event)) => apply_input_event(&mut pressed_buttons, event),
            Ok(None) => continue,
            Err(e) => {
                if !handle_error(&candidate.id, e).await {
                    break;
                }
                continue;
            }
        };

        // Update pressed_since timestamps based on state changes
        for update in &updates {
            match update {
                DeviceStateUpdate::ButtonDown(key) => {
                    pressed_since[*key as usize] = Some(Instant::now());
                }
                DeviceStateUpdate::ButtonUp(key) => {
                    pressed_since[*key as usize] = None;
                }
                _ => {}
            }
        }

        for update in updates {
            if event_tx.send(update).is_err() {
                return Ok(());
            }
        }
    }

    Ok(())
}

/// Releases all currently pressed buttons and sends ButtonUp events.
/// Used on HID read errors and during shutdown to prevent stuck buttons.
fn release_all_pressed(
    pressed_buttons: &mut u16,
    pressed_since: &mut [Option<Instant>; KEY_COUNT],
    event_tx: &mpsc::UnboundedSender<DeviceStateUpdate>,
    reason: &str,
) {
    if *pressed_buttons == 0 {
        return;
    }
    log::warn!(
        "Releasing all pressed buttons (reason: {}, mask: {:#06x})",
        reason,
        *pressed_buttons
    );
    for key in 0..KEY_COUNT {
        if (*pressed_buttons & (1 << key)) != 0 {
            let _ = event_tx.send(DeviceStateUpdate::ButtonUp(key as u8));
        }
    }
    *pressed_buttons = 0;
    pressed_since.fill(None);
}

/// Checks for buttons that have been pressed longer than STUCK_BUTTON_TIMEOUT
/// and synthesizes release events for them.
fn check_stuck_buttons(
    pressed_buttons: &mut u16,
    pressed_since: &mut [Option<Instant>; KEY_COUNT],
    event_tx: &mpsc::UnboundedSender<DeviceStateUpdate>,
) {
    if *pressed_buttons == 0 {
        return;
    }
    let now = Instant::now();
    for (key, slot) in pressed_since.iter_mut().enumerate() {
        if let Some(since) = *slot {
            if now.duration_since(since) >= STUCK_BUTTON_TIMEOUT {
                log::warn!(
                    "Button {} stuck for {:?}, synthesizing release",
                    key,
                    now.duration_since(since)
                );
                *pressed_buttons &= !(1u16 << key);
                *slot = None;
                let _ = event_tx.send(DeviceStateUpdate::ButtonUp(key as u8));
            }
        }
    }
}

async fn dispatch_single_update(
    outbound: &mut openaction::OutboundEventManager,
    device_id: &str,
    update: DeviceStateUpdate,
) {
    match update {
        DeviceStateUpdate::ButtonDown(key) => {
            log::debug!(
                "Sending key_down event: device_id={}, key={}",
                device_id,
                key
            );
            if let Err(err) = outbound.key_down(device_id.to_string(), key).await {
                log::warn!(
                    "Failed to send key_down event: device_id={}, key={}, err={}",
                    device_id,
                    key,
                    err
                );
            }
        }
        DeviceStateUpdate::ButtonUp(key) => {
            log::debug!("Sending key_up event: device_id={}, key={}", device_id, key);
            if let Err(err) = outbound.key_up(device_id.to_string(), key).await {
                log::warn!(
                    "Failed to send key_up event: device_id={}, key={}, err={}",
                    device_id,
                    key,
                    err
                );
            }
        }
        DeviceStateUpdate::EncoderDown(encoder) => {
            if let Err(err) = outbound.encoder_down(device_id.to_string(), encoder).await {
                log::warn!("Failed to send encoder_down event: {}", err);
            }
        }
        DeviceStateUpdate::EncoderUp(encoder) => {
            if let Err(err) = outbound.encoder_up(device_id.to_string(), encoder).await {
                log::warn!("Failed to send encoder_up event: {}", err);
            }
        }
        DeviceStateUpdate::EncoderTwist(encoder, val) => {
            if let Err(err) = outbound
                .encoder_change(device_id.to_string(), encoder, val as i16)
                .await
            {
                log::warn!("Failed to send encoder_change event: {}", err);
            }
        }
    }
}

async fn input_dispatch_worker(
    device_id: String,
    mut event_rx: mpsc::UnboundedReceiver<DeviceStateUpdate>,
) {
    while let Some(first) = event_rx.recv().await {
        let mut updates = vec![first];
        while let Ok(more) = event_rx.try_recv() {
            updates.push(more);
        }

        if let Some(outbound) = OUTBOUND_EVENT_MANAGER.lock().await.as_mut() {
            for update in updates {
                dispatch_single_update(outbound, &device_id, update).await;
            }
        }
    }
}

fn normalize_button_image(image: DynamicImage, width: u32, height: u32) -> DynamicImage {
    // CatmullRom keeps icon edges and text noticeably sharper than Triangle,
    // and the cost difference is negligible at key-sized images.
    let resized = image
        .resize(width, height, FilterType::CatmullRom)
        .to_rgb8();
    let mut canvas: RgbImage = ImageBuffer::from_pixel(width, height, Rgb([0, 0, 0]));
    let x = (width.saturating_sub(resized.width())) / 2;
    let y = (height.saturating_sub(resized.height())) / 2;

    let _ = canvas.copy_from(&resized, x, y);
    apply_rounded_corners(&mut canvas, BUTTON_CORNER_RADIUS);

    DynamicImage::ImageRgb8(canvas)
}

fn apply_rounded_corners(image: &mut RgbImage, radius: u32) {
    let width = image.width();
    let height = image.height();
    let radius = radius.min(width / 2).min(height / 2);

    if radius == 0 {
        return;
    }

    // Only iterate the 4 corner regions — edge and center pixels are always inside.
    let corners: [(u32, u32, u32, u32); 4] = [
        (0, 0, radius, radius),
        (width - radius, 0, width, radius),
        (0, height - radius, radius, height),
        (width - radius, height - radius, width, height),
    ];
    for (x0, y0, x1, y1) in corners {
        for y in y0..y1 {
            for x in x0..x1 {
                let coverage = rounded_rect_coverage(x, y, width, height, radius);
                if coverage < 1.0 {
                    // Fade towards black (the bezel colour) for a smooth edge.
                    let pixel = image.get_pixel_mut(x, y);
                    for channel in pixel.0.iter_mut() {
                        *channel = (f32::from(*channel) * coverage).round() as u8;
                    }
                }
            }
        }
    }
}

/// Fraction (0.0–1.0) of the pixel at `(x, y)` that lies inside the rounded
/// rectangle, approximated from the distance between the pixel centre and the
/// corner arc. Values between 0 and 1 anti-alias the curved edge.
fn rounded_rect_coverage(x: u32, y: u32, width: u32, height: u32, radius: u32) -> f32 {
    let r = radius as f32;
    let px = x as f32 + 0.5;
    let py = y as f32 + 0.5;

    let cx = if px < r {
        r
    } else if px > width as f32 - r {
        width as f32 - r
    } else {
        return 1.0;
    };
    let cy = if py < r {
        r
    } else if py > height as f32 - r {
        height as f32 - r
    } else {
        return 1.0;
    };

    let distance = (px - cx).hypot(py - cy);
    (r - distance + 0.5).clamp(0.0, 1.0)
}

fn blank_button_image(width: u32, height: u32) -> DynamicImage {
    let blank: RgbImage = ImageBuffer::from_pixel(width, height, Rgb([0, 0, 0]));
    DynamicImage::ImageRgb8(blank)
}

/// Stages an image in mirajazz's in-memory queue. Nothing reaches the device
/// until [`Device::flush`], which is where the actual USB traffic happens.
async fn queue_button_image_data(
    device: &Device,
    position: u8,
    image_data: &[u8],
) -> Result<(), MirajazzError> {
    device
        .write_image(opendeck_to_device(position), image_data)
        .await
}

fn resolve_device_kind(device: &Device, device_id: &str) -> Result<Kind, MirajazzError> {
    Kind::from_vid_pid(device.vid, device.pid).ok_or_else(|| {
        log::error!(
            "Unable to resolve device kind: vid={:#06x}, pid={:#06x}, device_id={}",
            device.vid,
            device.pid,
            device_id
        );
        MirajazzError::BadData
    })
}

fn hash_image_payload(image: &str) -> u64 {
    let mut hasher = AHasher::default();
    image.hash(&mut hasher);
    hasher.finish()
}

fn image_mode_code(format: ImageFormat) -> u8 {
    match format.mode {
        mirajazz::types::ImageMode::None => 0,
        mirajazz::types::ImageMode::BMP => 1,
        mirajazz::types::ImageMode::JPEG => 2,
    }
}

fn image_rotation_code(format: ImageFormat) -> u8 {
    match format.rotation {
        mirajazz::types::ImageRotation::Rot0 => 0,
        mirajazz::types::ImageRotation::Rot90 => 1,
        mirajazz::types::ImageRotation::Rot180 => 2,
        mirajazz::types::ImageRotation::Rot270 => 3,
    }
}

fn image_mirror_code(format: ImageFormat) -> u8 {
    match format.mirror {
        mirajazz::types::ImageMirroring::None => 0,
        mirajazz::types::ImageMirroring::X => 1,
        mirajazz::types::ImageMirroring::Y => 2,
        mirajazz::types::ImageMirroring::Both => 3,
    }
}

fn converted_image_cache_key(image_hash: u64, format: ImageFormat) -> ConvertedImageCacheKey {
    ConvertedImageCacheKey {
        image_hash,
        width: format.size.0,
        height: format.size.1,
        mode: image_mode_code(format),
        rotation: image_rotation_code(format),
        mirror: image_mirror_code(format),
    }
}

fn validate_button_position(position: u8) -> Result<usize, MirajazzError> {
    let index = position as usize;
    if index < KEY_COUNT {
        Ok(index)
    } else {
        log::warn!(
            "Ignoring out-of-range button position {} (max {})",
            position,
            KEY_COUNT - 1
        );
        Err(MirajazzError::BadData)
    }
}

fn decode_button_image(
    device_id: &str,
    position: u8,
    format: ImageFormat,
    image: &str,
) -> Result<DynamicImage, MirajazzError> {
    let url = match DataUrl::process(image) {
        Ok(url) => url,
        Err(err) => {
            log::error!(
                "Received malformed data URL for device {}, button {}: {}",
                device_id,
                position,
                err
            );
            return Err(MirajazzError::BadData);
        }
    };
    let (body, _fragment) = match url.decode_to_vec() {
        Ok(decoded) => decoded,
        Err(err) => {
            log::error!(
                "Failed to decode data URL for device {}, button {}: {}",
                device_id,
                position,
                err
            );
            return Err(MirajazzError::BadData);
        }
    };

    if url.mime_type().subtype != "jpeg" {
        log::error!("Incorrect mime type: {}", url.mime_type());
        return Err(MirajazzError::BadData);
    }

    let image = load_from_memory_with_format(body.as_slice(), image::ImageFormat::Jpeg)?;
    Ok(normalize_button_image(
        image,
        format.size.0 as u32,
        format.size.1 as u32,
    ))
}

/// Decodes a data-URL payload in a blocking worker and converts it to the
/// device's native image format. The JPEG decode runs on the blocking pool;
/// mirajazz's rotate/encode step runs via `block_in_place`, which hands this
/// worker thread's other tasks off to another thread while it encodes.
async fn decode_and_convert_button_image(
    device_id: &str,
    position: u8,
    format: ImageFormat,
    payload: Arc<str>,
) -> Result<Vec<u8>, MirajazzError> {
    let worker_device_id = device_id.to_string();
    let join_device_id = worker_device_id.clone();

    let decoded =
        spawn_blocking(move || decode_button_image(&worker_device_id, position, format, &payload))
            .await
            .map_err(|err| {
                log::error!(
                    "Image decode task panicked for device {}, button {}: {}",
                    join_device_id,
                    position,
                    err
                );
                MirajazzError::BadData
            })??;

    convert_image_with_format(format, decoded)
        .await
        .map_err(MirajazzError::ImageError)
}

fn should_apply_image(last_image_hash: Option<u64>, image_hash: u64) -> bool {
    last_image_hash != Some(image_hash)
}

fn should_apply_clear(last_image_hash: Option<u64>) -> bool {
    last_image_hash.is_some()
}

fn empty_render_batch() -> RenderBatch {
    array::from_fn(|_| None)
}

fn follow_up_batch_window(command: &RenderCommand) -> Option<Duration> {
    match command {
        RenderCommand::ClearAll => Some(CLEAR_ALL_BATCH_WINDOW),
        RenderCommand::SetImage { .. } => Some(IMAGE_BATCH_WINDOW),
        RenderCommand::ClearButton { .. } => None,
    }
}

fn batch_contains_image_updates(batch: &RenderBatch) -> bool {
    batch
        .iter()
        .any(|op| matches!(op, Some(BatchedRenderOp::Image { .. })))
}

fn apply_render_command_to_batch(
    batch: &mut RenderBatch,
    clear_all: &mut bool,
    command: RenderCommand,
) -> Result<(), MirajazzError> {
    match command {
        RenderCommand::SetImage {
            position,
            image_hash,
            payload,
        } => {
            let index = validate_button_position(position)?;
            batch[index] = Some(BatchedRenderOp::Image {
                image_hash,
                payload,
            });
        }
        RenderCommand::ClearButton { position } => {
            let index = validate_button_position(position)?;
            batch[index] = Some(BatchedRenderOp::Clear);
        }
        RenderCommand::ClearAll => {
            *clear_all = true;
            batch.fill(None);
        }
    }

    Ok(())
}

async fn build_render_assets(kind: &Kind) -> Result<Arc<PreparedRenderAssets>, MirajazzError> {
    if let Some(cached) = RENDER_ASSET_CACHE.lock().await.get(kind).cloned() {
        return Ok(cached);
    }

    let button_formats = (0..KEY_COUNT)
        .map(|position| get_image_format_for_key(kind, position as u8))
        .collect::<Vec<_>>();
    let mut black_frames = Vec::with_capacity(button_formats.len());

    for format in &button_formats {
        let black_frame = blank_button_image(format.size.0 as u32, format.size.1 as u32);
        black_frames.push(Arc::new(
            convert_image_with_format(*format, black_frame).await?,
        ));
    }

    let assets = Arc::new(PreparedRenderAssets {
        button_formats,
        black_frames,
    });

    RENDER_ASSET_CACHE
        .lock()
        .await
        .insert(*kind, assets.clone());

    Ok(assets)
}

async fn clear_all_button_images_with_assets(
    device: &Device,
    device_id: &str,
    black_frames: &[Arc<Vec<u8>>],
) -> Result<(), MirajazzError> {
    if let Err(err) = device.clear_all_button_images().await {
        log::warn!(
            "Failed to clear all button images natively for device {}, falling back to per-key black frames: {}",
            device_id,
            err
        );

        for position in 0..KEY_COUNT as u8 {
            let index = position as usize;
            queue_button_image_data(device, position, black_frames[index].as_slice()).await?;
        }
    }

    Ok(())
}

async fn clear_all_button_images_for_kind(
    device: &Device,
    device_id: &str,
    kind: &Kind,
) -> Result<(), MirajazzError> {
    let assets = build_render_assets(kind).await?;
    clear_all_button_images_with_assets(device, device_id, &assets.black_frames).await
}

async fn remove_renderer_handle(device_id: &str, handle: &Arc<DeviceRenderHandle>) {
    let mut renderers = DEVICE_RENDERERS.lock().await;
    if renderers
        .get(device_id)
        .is_some_and(|existing| Arc::ptr_eq(existing, handle))
    {
        renderers.remove(device_id);
    }
}

async fn get_device_renderer(
    device: &Device,
    device_id: &str,
) -> Result<Arc<DeviceRenderHandle>, MirajazzError> {
    use std::collections::hash_map::Entry;

    let mut renderers = DEVICE_RENDERERS.lock().await;
    match renderers.entry(device_id.to_string()) {
        Entry::Occupied(e) => Ok(e.get().clone()),
        Entry::Vacant(e) => {
            let kind = resolve_device_kind(device, device_id)?;
            let (command_tx, command_rx) = mpsc::channel(RENDER_CHANNEL_CAPACITY);
            let handle = Arc::new(DeviceRenderHandle {
                command_tx,
                shutdown_token: CancellationToken::new(),
            });
            e.insert(handle.clone());
            drop(renderers);
            TRACKER.spawn(render_worker(
                device_id.to_string(),
                kind,
                handle.clone(),
                command_rx,
            ));
            Ok(handle)
        }
    }
}

async fn enqueue_render_command(
    device: &Device,
    device_id: &str,
    command: RenderCommand,
) -> Result<(), MirajazzError> {
    for _ in 0..2 {
        let renderer = get_device_renderer(device, device_id).await?;
        match renderer.command_tx.try_send(command.clone()) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(command)) => {
                // Never drop updates: losing the last frame for a key leaves a
                // stale image on screen. Wait for the worker to drain instead.
                log::debug!(
                    "Render queue is full for device {}, waiting for capacity",
                    device_id
                );
                if renderer.command_tx.send(command).await.is_ok() {
                    return Ok(());
                }
                remove_renderer_handle(device_id, &renderer).await;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                remove_renderer_handle(device_id, &renderer).await;
            }
        }
    }

    log::warn!("Render queue is unavailable for device {}", device_id);
    Err(MirajazzError::BadData)
}

#[allow(clippy::too_many_arguments)]
async fn process_render_batch(
    device: &Device,
    device_id: &str,
    button_formats: &[ImageFormat],
    black_frames: &[Arc<Vec<u8>>],
    converted_image_cache: &mut LruCache<ConvertedImageCacheKey, Arc<Vec<u8>>>,
    last_image_hashes: &mut [Option<u64>; KEY_COUNT],
    clear_all: bool,
    batch: RenderBatch,
) -> Result<(), MirajazzError> {
    let mut writes_to_device = 0usize;
    let queued_ops = batch.iter().filter(|op| op.is_some()).count();
    let has_image_updates = batch_contains_image_updates(&batch);
    // Positions not touched by this batch, captured before `batch` is consumed.
    // A clear-all combined with images blanks these with per-key black frames.
    let untouched: [bool; KEY_COUNT] = array::from_fn(|index| batch[index].is_none());

    // Phase 1: Collect all image decode/convert tasks and resolve cache hits.
    // Cache misses are launched in parallel so all CPU work overlaps.
    struct PendingImage {
        index: usize,
        image_hash: u64,
        image_data: Arc<Vec<u8>>,
    }

    let mut pending_clears: Vec<usize> = Vec::new();
    let mut resolved_images: Vec<PendingImage> = Vec::new();
    let mut decode_tasks: tokio::task::JoinSet<Result<PendingImage, (usize, MirajazzError)>> =
        tokio::task::JoinSet::new();

    for (index, op) in batch.into_iter().enumerate() {
        match op {
            Some(BatchedRenderOp::Clear) => {
                if should_apply_clear(last_image_hashes[index]) {
                    pending_clears.push(index);
                }
            }
            Some(BatchedRenderOp::Image {
                image_hash,
                payload,
            }) => {
                if !should_apply_image(last_image_hashes[index], image_hash) {
                    continue;
                }

                let format = button_formats[index];
                let cache_key = converted_image_cache_key(image_hash, format);

                if let Some(cached) = converted_image_cache.get(&cache_key) {
                    // Cache hit — resolve immediately
                    resolved_images.push(PendingImage {
                        index,
                        image_hash,
                        image_data: cached.clone(),
                    });
                } else {
                    // Cache miss — launch decode+convert in parallel
                    let dev_id = device_id.to_string();
                    decode_tasks.spawn(async move {
                        let data =
                            decode_and_convert_button_image(&dev_id, index as u8, format, payload)
                                .await
                                .map_err(|e| (index, e))?;
                        Ok(PendingImage {
                            index,
                            image_hash,
                            image_data: Arc::new(data),
                        })
                    });
                }
            }
            None => {}
        }
    }

    // Wait for all parallel decode tasks to complete
    while let Some(result) = decode_tasks.join_next().await {
        match result {
            Ok(Ok(pending)) => {
                // Insert into cache (LRU evicts the oldest entry automatically when full)
                let format = button_formats[pending.index];
                let cache_key = converted_image_cache_key(pending.image_hash, format);
                converted_image_cache.put(cache_key, pending.image_data.clone());
                resolved_images.push(pending);
            }
            Ok(Err((_, MirajazzError::BadData | MirajazzError::ImageError(_)))) => {
                // Non-fatal image error, skip this button
            }
            Ok(Err((_, err))) => return Err(err),
            Err(join_err) => {
                log::error!("Image decode task panic: {}", join_err);
            }
        }
    }

    // Sort resolved images by index for deterministic device write order
    resolved_images.sort_by_key(|p| p.index);

    // Phase 2: All device I/O happens under the I/O lock, after the CPU-heavy
    // decoding above, so keepalive/brightness never land inside an upload.
    let io_lock = device_io_lock(device_id);
    let _io = io_lock.lock().await;

    if clear_all {
        if has_image_updates {
            for index in 0..KEY_COUNT {
                if !untouched[index] || !should_apply_clear(last_image_hashes[index]) {
                    continue;
                }

                queue_button_image_data(device, index as u8, black_frames[index].as_slice())
                    .await?;
                last_image_hashes[index] = None;
                writes_to_device += 1;
            }
        } else {
            clear_all_button_images_with_assets(device, device_id, black_frames).await?;
            last_image_hashes.fill(None);
            writes_to_device += 1;
        }
    }

    for index in pending_clears {
        queue_button_image_data(device, index as u8, black_frames[index].as_slice()).await?;
        last_image_hashes[index] = None;
        writes_to_device += 1;
    }

    for pending in resolved_images {
        queue_button_image_data(device, pending.index as u8, pending.image_data.as_slice()).await?;
        last_image_hashes[pending.index] = Some(pending.image_hash);
        writes_to_device += 1;
    }

    log::debug!(
        "Processed render batch: device_id={}, clear_all={}, queued_ops={}, writes={}",
        device_id,
        clear_all,
        queued_ops,
        writes_to_device
    );

    if writes_to_device > 0 {
        match tokio::time::timeout(USB_WRITE_TIMEOUT, device.flush()).await {
            Ok(result) => result?,
            Err(_elapsed) => {
                log::error!("USB flush timed out for device {}", device_id);
                return Err(MirajazzError::InvalidDeviceError);
            }
        }
    }

    Ok(())
}

async fn render_worker(
    device_id: String,
    kind: Kind,
    handle: Arc<DeviceRenderHandle>,
    mut command_rx: mpsc::Receiver<RenderCommand>,
) {
    let assets = match build_render_assets(&kind).await {
        Ok(assets) => assets,
        Err(err) => {
            handle_error(&device_id, err).await;
            remove_renderer_handle(&device_id, &handle).await;
            return;
        }
    };
    let button_formats = assets.button_formats.as_slice();
    let black_frames = assets.black_frames.as_slice();
    let mut converted_image_cache =
        LruCache::new(NonZeroUsize::new(IMAGE_CACHE_LIMIT).expect("cache limit must be non-zero"));
    let mut last_image_hashes = [None; KEY_COUNT];

    loop {
        let recv = tokio::select! {
            recv = command_rx.recv() => recv,
            _ = handle.shutdown_token.cancelled() => None,
        };

        let Some(command) = recv else {
            break;
        };

        let mut batch = empty_render_batch();
        let mut clear_all = false;

        if let Some(window) = follow_up_batch_window(&command) {
            // Page switches may arrive either as `ClearAll + SetImage...` or as a burst of
            // `SetImage` updates only. A short idle window lets us collapse both patterns into
            // a single flush.
            tokio::select! {
                _ = tokio::time::sleep(window) => {},
                _ = handle.shutdown_token.cancelled() => break,
            }
        }

        if let Err(err) = apply_render_command_to_batch(&mut batch, &mut clear_all, command) {
            if !handle_error(&device_id, err).await {
                break;
            }
            continue;
        }

        while let Ok(command) = command_rx.try_recv() {
            if let Err(err) = apply_render_command_to_batch(&mut batch, &mut clear_all, command) {
                if !handle_error(&device_id, err).await {
                    remove_renderer_handle(&device_id, &handle).await;
                    return;
                }
            }
        }

        let Some(device) = DEVICES.read().await.get(&device_id).cloned() else {
            break;
        };

        log::debug!(
            "Rendering batch: device_id={}, clear_all={}, ops={}, image_updates={}",
            device_id,
            clear_all,
            batch.iter().filter(|op| op.is_some()).count(),
            batch_contains_image_updates(&batch)
        );

        let result = process_render_batch(
            device.as_ref(),
            &device_id,
            button_formats,
            black_frames,
            &mut converted_image_cache,
            &mut last_image_hashes,
            clear_all,
            batch,
        )
        .await;

        if let Err(err) = result {
            if !handle_error(&device_id, err).await {
                break;
            }
        }

        // Yield to let the input reader task process any pending HID reports
        // that arrived while the device was busy with image writes.
        tokio::task::yield_now().await;
    }

    remove_renderer_handle(&device_id, &handle).await;
}

/// Handles different combinations of "set image" event, including clearing the specific buttons and whole device
pub async fn handle_set_image(device: &Device, evt: SetImageEvent) -> Result<(), MirajazzError> {
    let device_id = evt.device.clone();

    match (evt.position, evt.image) {
        (Some(position), Some(image)) => {
            validate_button_position(position)?;
            enqueue_render_command(
                device,
                &device_id,
                RenderCommand::SetImage {
                    position,
                    image_hash: hash_image_payload(&image),
                    payload: Arc::from(image),
                },
            )
            .await?;
        }
        (Some(position), None) => {
            validate_button_position(position)?;
            enqueue_render_command(device, &device_id, RenderCommand::ClearButton { position })
                .await?;
        }
        (None, None) => {
            enqueue_render_command(device, &device_id, RenderCommand::ClearAll).await?;
        }
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use image::{DynamicImage, GenericImageView, ImageBuffer, Rgb};
    use mirajazz::types::{ImageMirroring, ImageMode, ImageRotation};

    use crate::mappings::{KEY_COUNT, Kind};

    use super::{
        BUTTON_CORNER_RADIUS, BatchedRenderOp, CLEAR_ALL_BATCH_WINDOW, IMAGE_BATCH_WINDOW,
        RenderCommand, apply_render_command_to_batch, batch_contains_image_updates,
        build_render_assets, converted_image_cache_key, empty_render_batch, follow_up_batch_window,
        normalize_button_image, should_apply_clear, should_apply_image,
    };

    #[test]
    fn duplicate_applied_image_is_skipped() {
        assert!(!should_apply_image(Some(42), 42));
    }

    #[test]
    fn newer_image_is_applied() {
        assert!(should_apply_image(Some(11), 33));
    }

    #[test]
    fn clear_is_skipped_when_button_is_already_empty() {
        assert!(!should_apply_clear(None));
    }

    #[test]
    fn clear_is_applied_when_button_has_content() {
        assert!(should_apply_clear(Some(7)));
    }

    #[test]
    fn latest_command_wins_for_same_button() {
        let mut batch = empty_render_batch();
        let mut clear_all = false;

        apply_render_command_to_batch(
            &mut batch,
            &mut clear_all,
            RenderCommand::SetImage {
                position: 2,
                image_hash: 10,
                payload: "a".into(),
            },
        )
        .unwrap();
        apply_render_command_to_batch(
            &mut batch,
            &mut clear_all,
            RenderCommand::ClearButton { position: 2 },
        )
        .unwrap();

        assert!(!clear_all);
        assert_eq!(batch[2], Some(BatchedRenderOp::Clear));
    }

    #[test]
    fn clear_all_discards_previous_button_batch() {
        let mut batch = empty_render_batch();
        let mut clear_all = false;

        apply_render_command_to_batch(
            &mut batch,
            &mut clear_all,
            RenderCommand::SetImage {
                position: 1,
                image_hash: 10,
                payload: "a".into(),
            },
        )
        .unwrap();
        apply_render_command_to_batch(&mut batch, &mut clear_all, RenderCommand::ClearAll).unwrap();

        assert!(clear_all);
        assert!(batch.iter().all(Option::is_none));
    }

    #[test]
    fn updates_after_clear_all_are_preserved() {
        let mut batch = empty_render_batch();
        let mut clear_all = false;

        apply_render_command_to_batch(&mut batch, &mut clear_all, RenderCommand::ClearAll).unwrap();
        apply_render_command_to_batch(
            &mut batch,
            &mut clear_all,
            RenderCommand::SetImage {
                position: 4,
                image_hash: 99,
                payload: "b".into(),
            },
        )
        .unwrap();

        assert!(clear_all);
        assert_eq!(
            batch[4],
            Some(BatchedRenderOp::Image {
                image_hash: 99,
                payload: "b".into(),
            })
        );
    }

    #[test]
    fn clear_all_waits_for_follow_up_batch() {
        assert_eq!(
            follow_up_batch_window(&RenderCommand::ClearAll),
            Some(CLEAR_ALL_BATCH_WINDOW)
        );
    }

    #[test]
    fn regular_image_update_waits_for_follow_up_batch() {
        assert_eq!(
            follow_up_batch_window(&RenderCommand::SetImage {
                position: 0,
                image_hash: 1,
                payload: "a".into(),
            }),
            Some(IMAGE_BATCH_WINDOW)
        );
    }

    #[test]
    fn clear_button_does_not_wait_for_follow_up_batch() {
        assert_eq!(
            follow_up_batch_window(&RenderCommand::ClearButton { position: 0 }),
            Option::<Duration>::None
        );
    }

    #[test]
    fn converted_image_cache_key_includes_format() {
        let jpeg = mirajazz::types::ImageFormat {
            mode: ImageMode::JPEG,
            size: (100, 100),
            rotation: ImageRotation::Rot180,
            mirror: ImageMirroring::None,
        };
        let bmp = mirajazz::types::ImageFormat {
            mode: ImageMode::BMP,
            size: (100, 100),
            rotation: ImageRotation::Rot180,
            mirror: ImageMirroring::None,
        };

        assert_ne!(
            converted_image_cache_key(7, jpeg),
            converted_image_cache_key(7, bmp)
        );
    }

    #[test]
    fn normalized_image_keeps_requested_dimensions() {
        let source =
            DynamicImage::ImageRgb8(ImageBuffer::from_pixel(100, 100, Rgb([255, 255, 255])));

        let normalized = normalize_button_image(source, 100, 100);

        assert_eq!(normalized.dimensions(), (100, 100));
    }

    #[test]
    fn normalized_image_masks_all_four_corners_to_black() {
        let source =
            DynamicImage::ImageRgb8(ImageBuffer::from_pixel(100, 100, Rgb([255, 255, 255])));

        let normalized = normalize_button_image(source, 100, 100).to_rgb8();

        assert_eq!(normalized.get_pixel(0, 0), &Rgb([0, 0, 0]));
        assert_eq!(normalized.get_pixel(99, 0), &Rgb([0, 0, 0]));
        assert_eq!(normalized.get_pixel(0, 99), &Rgb([0, 0, 0]));
        assert_eq!(normalized.get_pixel(99, 99), &Rgb([0, 0, 0]));
    }

    #[test]
    fn normalized_image_preserves_center_content() {
        let source =
            DynamicImage::ImageRgb8(ImageBuffer::from_pixel(100, 100, Rgb([255, 255, 255])));

        let normalized = normalize_button_image(source, 100, 100).to_rgb8();

        assert_eq!(normalized.get_pixel(50, 50), &Rgb([255, 255, 255]));
        assert_eq!(
            normalized.get_pixel(BUTTON_CORNER_RADIUS, BUTTON_CORNER_RADIUS),
            &Rgb([255, 255, 255])
        );
    }

    #[test]
    fn normalized_image_anti_aliases_corner_edge() {
        let source =
            DynamicImage::ImageRgb8(ImageBuffer::from_pixel(100, 100, Rgb([255, 255, 255])));

        let normalized = normalize_button_image(source, 100, 100).to_rgb8();

        // (4, 4) sits right on the corner arc, so it should be partially blended.
        let value = normalized.get_pixel(4, 4).0[0];
        assert!(
            value > 0 && value < 255,
            "expected blended edge, got {value}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_render_assets_preconverts_black_frames() {
        let assets = build_render_assets(&Kind::AMPGD6).await.unwrap();

        assert_eq!(assets.button_formats.len(), KEY_COUNT);
        assert_eq!(assets.black_frames.len(), KEY_COUNT);
        assert!(
            assets
                .black_frames
                .iter()
                .all(|payload| !payload.is_empty())
        );
    }

    #[test]
    fn clear_all_batch_with_images_avoids_native_clear_path() {
        let mut batch = empty_render_batch();
        let mut clear_all = false;

        apply_render_command_to_batch(&mut batch, &mut clear_all, RenderCommand::ClearAll).unwrap();
        apply_render_command_to_batch(
            &mut batch,
            &mut clear_all,
            RenderCommand::SetImage {
                position: 3,
                image_hash: 55,
                payload: "x".into(),
            },
        )
        .unwrap();

        assert!(clear_all);
        assert!(batch_contains_image_updates(&batch));
    }

    #[test]
    fn clear_only_batch_keeps_native_clear_path_available() {
        let mut batch = empty_render_batch();
        let mut clear_all = false;

        apply_render_command_to_batch(&mut batch, &mut clear_all, RenderCommand::ClearAll).unwrap();
        apply_render_command_to_batch(
            &mut batch,
            &mut clear_all,
            RenderCommand::ClearButton { position: 3 },
        )
        .unwrap();

        assert!(clear_all);
        assert!(!batch_contains_image_updates(&batch));
    }
}
