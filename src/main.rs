use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use eyre::Result;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::event_handler::{Ctx, RawEvent};
use matrix_sdk::room::Receipts;
use matrix_sdk::ruma::api::client::filter::FilterDefinition;
use matrix_sdk::ruma::events::Mentions;
use matrix_sdk::ruma::events::relation::{Reply, Thread};
use matrix_sdk::ruma::events::room::encrypted::OriginalSyncRoomEncryptedEvent;
use matrix_sdk::ruma::events::room::member::{
    MembershipState, StrippedRoomMemberEvent, SyncRoomMemberEvent,
};
use matrix_sdk::ruma::events::room::message::{
    MessageType, OriginalSyncRoomMessageEvent, Relation, RoomMessageEventContent,
    RoomMessageEventContentWithoutRelation, TextMessageEventContent,
};
use matrix_sdk::ruma::{OwnedEventId, OwnedUserId};
use matrix_sdk::{Client, Room, RoomState};
use tokio::select;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, instrument, warn};
use tracing_subscriber::{EnvFilter, prelude::*};

mod config;
mod html_escape;

#[derive(clap::Parser)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    #[clap(about = "Perform initial setup of Matrix account")]
    Setup {
        #[clap(
            long = "config",
            value_name = "PATH",
            help = "Path to the configuration file"
        )]
        config_path: PathBuf,
        #[clap(
            long,
            value_name = "DEVICE_NAME",
            default_value = concat!("matrixbot-ezlogin/", env!("CARGO_BIN_NAME")),
            help = "Device name to use for this session"
        )]
        device_name: String,
    },
    #[clap(about = "Run the bot")]
    Run {
        #[clap(
            long = "config",
            value_name = "PATH",
            help = "Path to the configuration file"
        )]
        config_path: PathBuf,
    },
    #[clap(about = "Log out of the Matrix session, and delete the state database")]
    Logout {
        #[clap(
            long = "config",
            value_name = "PATH",
            help = "Path to the configuration file"
        )]
        config_path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    matrixbot_ezlogin::DuplexLog::init();
    tracing_subscriber::registry()
        .with(tracing_error::ErrorLayer::default())
        .with({
            let mut filter = EnvFilter::new(concat!(
                "warn,",
                env!("CARGO_CRATE_NAME"),
                "=info,matrixbot_ezlogin=info"
            ));
            if let Some(env) = std::env::var_os(EnvFilter::DEFAULT_ENV) {
                for segment in env.to_string_lossy().split(',') {
                    if let Ok(directive) = segment.parse() {
                        filter = filter.add_directive(directive);
                    }
                }
            }
            filter
        })
        .with(
            tracing_subscriber::fmt::layer().with_writer(matrixbot_ezlogin::DuplexLog::get_writer),
        )
        .init();

    let args: Args = clap::Parser::parse();

    match args.command {
        Command::Setup {
            config_path,
            device_name,
        } => {
            let config = config::Config::new(&config_path).await?;
            drop(matrixbot_ezlogin::setup_interactive(&config.data_dir, &device_name).await?);
        }
        Command::Run { config_path } => {
            let config = config::Config::new(&config_path).await?;
            run(config).await?;
        }
        Command::Logout { config_path } => {
            let config = config::Config::new(&config_path).await?;
            matrixbot_ezlogin::logout(&config.data_dir).await?
        }
    };
    Ok(())
}

async fn run(config: Arc<config::Config>) -> Result<()> {
    let (client, sync_helper) = matrixbot_ezlogin::login(&config.data_dir).await?;

    // We don't ignore joining and leaving events happened during downtime.
    client.add_event_handler_context(config);
    client.add_event_handler(on_invite);
    client.add_event_handler(on_leave);

    // Enable room members lazy-loading, it will speed up the initial sync a lot with accounts in lots of rooms.
    // https://spec.matrix.org/v1.6/client-server-api/#lazy-loading-room-members
    let sync_settings =
        SyncSettings::default().filter(FilterDefinition::with_lazy_loading().into());

    info!(
        "Skipping messages since last logout. May take longer depending on the number of rooms joined."
    );
    sync_helper
        .sync_once(&client, sync_settings.clone())
        .await?;

    client.add_event_handler(on_message);
    client.add_event_handler(on_utd);

    // Forget rooms that we already left
    let left_rooms = client.left_rooms();
    tokio::spawn(
        async move {
            for room in left_rooms {
                info!("Forgetting room {}.", room.room_id());
                match room.forget().await {
                    Ok(_) => info!("Forgot room {}.", room.room_id()),
                    Err(err) => error!("Failed to forget room {}: {}", room.room_id(), err),
                }
            }
        }
        .in_current_span(),
    );

    info!("Starting sync.");
    sync_helper.sync(&client, sync_settings).await?;

    Ok(())
}

#[instrument(skip_all)]
async fn set_read_marker(room: Room, event_id: OwnedEventId) {
    if let Err(err) = room
        .send_multiple_receipts(
            Receipts::new()
                .fully_read_marker(event_id.clone())
                .public_read_receipt(event_id.clone()),
        )
        .await
    {
        error!(
            "Failed to set the read marker of room {} to event {}: {}",
            room.room_id(),
            event_id,
            err
        );
    }
}

#[instrument(skip_all)]
fn send_log(
    config: &config::Config,
    client: Client,
    text: String,
    html: Option<String>,
) -> impl Future<Output = ()> + use<> {
    let maybe_reply = config
        .log_room
        .as_ref()
        .and_then(|log_room| {
            client.get_room(&log_room).or_else(|| {
                error!("Cannot find the log room {}.", log_room);
                None
            })
        })
        .map(|room| {
            (
                room,
                match html {
                    Some(html) => RoomMessageEventContent::notice_html(text, html),
                    None => RoomMessageEventContent::notice_plain(text),
                }
                .add_mentions(Mentions::new()),
            )
        });
    async move {
        let Some((room, reply)) = maybe_reply else {
            return;
        };
        if let Err(err) = room.send(reply).await {
            error!("Failed to send log to {}: {}", room.room_id(), err);
        }
    }
    .in_current_span()
}

#[instrument(skip_all)]
fn send_reply(
    room: Room,
    thread_id: Option<OwnedEventId>,
    event_id: OwnedEventId,
    text: String,
    html: Option<String>,
) -> impl Future<Output = ()> + use<> {
    // We should use make_reply_to, but it embeds the original message body, which I don't want
    let relates_to = match thread_id {
        Some(thread) => Some(Relation::Thread(Thread::reply(thread, event_id.clone()))),
        _ => Some(Relation::Reply(Reply::with_event_id(event_id.clone()))),
    };
    let reply = match html {
        Some(html) => RoomMessageEventContentWithoutRelation::notice_html(text, html),
        None => RoomMessageEventContentWithoutRelation::notice_plain(text),
    }
    .add_mentions(Mentions::new())
    .with_relation(relates_to);

    async move {
        if let Err(err) = room.send(reply).await {
            error!("Failed to send a reply to {}: {}", event_id, err);
        }
    }
    .in_current_span()
}

#[instrument(skip_all)]
async fn process_invite(
    client: Client,
    room: Room,
    thread_id: Option<OwnedEventId>,
    event_id: OwnedEventId,
    sender: OwnedUserId,
    passphrase: &str,
    config: Arc<config::Config>,
) {
    let sender_link = sender.matrix_uri(false).to_string();
    let target_room_id = if passphrase.is_empty() {
        None
    } else {
        config.passphrases.get(passphrase).cloned()
    };
    let Some(target_room_id) = target_room_id else {
        tokio::spawn(send_log(
            &config,
            client.clone(),
            format!(
                "<{}> didn‘t pass authentication: Incorrect passphrase.",
                sender
            ),
            Some(format!(
                "<a href=\"{}\">{}</a> didn‘t pass authentication: Incorrect passphrase.",
                html_escape::attr(&sender_link),
                html_escape::text(sender.as_str())
            )),
        ));
        tokio::spawn(send_reply(
            room,
            thread_id,
            event_id,
            "Incorrect passphrase, please try again.".to_owned(),
            None,
        ));
        return;
    };
    let target_room_link = target_room_id.matrix_to_uri().to_string();

    let Some(target_room) = client.get_room(&target_room_id) else {
        error!("Cannot find the target room {}.", target_room_id);
        tokio::spawn(send_log(
            &config,
            client.clone(),
            format!(
                "Failed to invite <{}> to <{}>: Cannot find the target room.",
                sender, target_room_id
            ),
            Some(format!(
                "Failed to invite <a href=\"{}\">{}</a> to <a href=\"{}\">{}</a>: Cannot find the target room.",
                html_escape::attr(&sender_link),
                html_escape::text(sender.as_str()),
                html_escape::attr(&target_room_link),
                html_escape::text(target_room_id.as_str())
            )),
        ));
        tokio::spawn(send_reply(
            room,
            thread_id,
            event_id,
            format!(
                "I’m trying to invite you to <{}>, but something went wrong.",
                target_room_id
            ),
            Some(format!(
                "I’m trying to invite you to <a href=\"{}\">{}</a>, but something went wrong.",
                html_escape::attr(&target_room_link),
                html_escape::text(target_room_id.as_str())
            )),
        ));
        return;
    };

    let target_room_link = Arc::new(RwLock::new(target_room_link));
    let cancel_token = CancellationToken::new();

    // Spawn a task that fires after 5 seconds, informing the user of a potential delay.
    let delay_notification = tokio::spawn({
        let cancel_token = cancel_token.clone();
        let room = room.clone();
        let thread_id = thread_id.clone();
        let event_id = event_id.clone();
        let target_room_id = target_room_id.clone();
        let target_room_link = target_room_link.clone();
        async move {
            select! {
                _ = cancel_token.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(5)) => (),
            }
            // Default retry time taken from https://github.com/matrix-org/matrix-rust-sdk/blob/matrix-sdk-0.12.0/crates/matrix-sdk/src/http_client/native.rs#L50-L54
            send_reply(
                room,
                thread_id,
                event_id,
                format!(
                    "Inviting to <{}>. It make take 0–15 minutes…",
                    target_room_id
                ),
                Some(format!(
                    "Inviting to <a href=\"{}\">{}</a>. It make take 0–15 minutes…",
                    html_escape::attr(&target_room_link.read().await),
                    html_escape::text(target_room_id.as_str())
                )),
            )
            .await;
        }
        .in_current_span()
    });

    tokio::spawn(
        async move {
            info!("Checking the membership of {} in {}.", sender, target_room_id);
            if let Err(err) = room.sync_members().await {
                warn!("Failed to sync members of {}: {}", room.room_id(), err);
            }

            let invite_failure_is_normal = match room.get_member(&sender).await {
                Ok(Some(member)) => {
                    let membership = member.membership();
                    info!(
                        "The membership of {} in room {} is {}.",
                        sender,
                        target_room_id,
                        membership.as_str()
                    );
                    // If the sender is banned, react as if they are joined. Their client will say they are banned.
                    matches!(
                        membership,
                        MembershipState::Ban | MembershipState::Invite | MembershipState::Join
                    )
                }
                Ok(None) => {
                    info!("User {} is not in room {}.", sender, target_room_id);
                    false
                }
                Err(err) => {
                    warn!(
                        "Failed to check if {} is already in room {}: {}",
                        sender, target_room_id, err
                    );
                    false
                }
            };

            let target_room_route = target_room.route().await.unwrap_or_default();
            *target_room_link.write().await = target_room_id.matrix_to_uri_via(target_room_route).to_string();

            info!("Inviting {} to room {}.", sender, target_room_id);
            send_log(
                &config,
                client.clone(),
                format!("Inviting <{}> to room <{}>…", sender, target_room_id),
                Some(format!(
                    "Inviting <a href=\"{}\">{}</a> to room <a href=\"{}\">{}</a>…",
                    html_escape::attr(&sender_link),
                    html_escape::text(sender.as_str()),
                    html_escape::attr(&target_room_link.read().await),
                    html_escape::text(target_room_id.as_str())
                )),
            ).await;

            let invite_is_successful = match target_room.invite_user_by_id(&sender).await {
                Ok(_) => {
                    info!("Invited {} to room {}.", sender, target_room.room_id());
                    tokio::spawn(send_log(
                        &config,
                        client.clone(),
                        format!("Invited <{}> to room <{}>.", sender, target_room_id),
                        Some(format!(
                            "Invited <a href=\"{}\">{}</a> to room <a href=\"{}\">{}</a>.",
                            html_escape::attr(&sender_link),
                            html_escape::text(sender.as_str()),
                            html_escape::attr(&target_room_link.read().await),
                            html_escape::text(target_room_id.as_str())
                        )),
                    ));
                    true
                }
                Err(err) => {
                    error!(
                        "Failed to invite {} to room {}: {}",
                        sender,
                        target_room.room_id(),
                        err
                    );
                    let err_str = err.to_string();
                    tokio::spawn(send_log(
                        &config,
                        client.clone(),
                        format!(
                            "Failed to invite <{}> to room <{}>{}: {}",
                            sender,
                            target_room_id,
                            if invite_failure_is_normal {
                                " (this is normal)"
                            } else {
                                ""
                            },
                            err_str
                        ),
                        Some(format!(
                            "Failed to invite <a href=\"{}\">{}</a> to room <a href=\"{}\">{}</a>{}: <pre>{}</pre>",
                            html_escape::attr(&sender_link),
                            html_escape::text(sender.as_str()),
                            html_escape::attr(&target_room_link.read().await),
                            html_escape::text(target_room_id.as_str()),
                            if invite_failure_is_normal {
                                " (this is normal)"
                            } else {
                                ""
                            },
                            html_escape::text(&err_str)
                        )),
                    ));
                    false
                }
            };

            cancel_token.cancel();
            _ = delay_notification.await;

            if invite_is_successful || invite_failure_is_normal {
                send_reply(
                    room.clone(),
                    thread_id,
                    event_id,
                    format!("Welcome to <{}>!", target_room_id),
                    Some(format!(
                        "Welcome to <a href=\"{}\">{}</a>!",
                        html_escape::attr(&target_room_link.read().await),
                        html_escape::text(target_room_id.as_str())
                    )),
                )
                .await;
            } else {
                send_reply(
                    room.clone(),
                    thread_id,
                    event_id,
                    format!(
                        "I’ve tried to invite you to <{}>, but something went wrong.",
                        target_room_id
                    ),
                    Some(format!(
                        "I’ve tried to invite you to <a href=\"{}\">{}</a>, but something went wrong.",
                        html_escape::attr(&target_room_link.read().await),
                        html_escape::text(target_room_id.as_str())
                    )),
                )
                .await;
            }
        }
        .in_current_span(),
    );
}

// https://spec.matrix.org/v1.14/client-server-api/#mroommessage
#[instrument(skip_all)]
async fn on_message(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    client: Client,
    config: Ctx<Arc<config::Config>>,
) {
    if event.sender == client.user_id().unwrap() {
        // Ignore my own message
        return;
    }
    if !room.is_direct().await.unwrap_or(false) {
        return;
    }
    tokio::spawn(set_read_marker(room.clone(), event.event_id.clone()));
    if room.state() != RoomState::Joined {
        info!(
            "Ignoring room {}: Current room state is {:?}.",
            room.room_id(),
            room.state()
        );
        return;
    }
    if let Some(Relation::Replacement(_)) = event.content.relates_to {
        return;
    }
    let MessageType::Text(TextMessageEventContent {
        body: ref passphrase,
        ..
    }) = event.content.msgtype
    else {
        return;
    };
    let thread_id = match event.content.relates_to {
        Some(Relation::Thread(ref thread)) => Some(thread.event_id.clone()),
        _ => None,
    };
    process_invite(
        client,
        room,
        thread_id,
        event.event_id,
        event.sender,
        passphrase.trim(),
        config.0,
    )
    .await;
}

// https://spec.matrix.org/v1.14/client-server-api/#mroomencrypted
#[instrument(skip_all)]
async fn on_utd(_event: OriginalSyncRoomEncryptedEvent, room: Room, raw_event: RawEvent) {
    error!(
        "Unable to decrypt: room {}, event {}",
        room.room_id(),
        raw_event.get()
    );
}

// https://spec.matrix.org/v1.14/client-server-api/#mroommember
// https://spec.matrix.org/v1.14/client-server-api/#stripped-state
#[instrument(skip_all)]
async fn on_invite(
    event: StrippedRoomMemberEvent,
    room: Room,
    client: Client,
    config: Ctx<Arc<config::Config>>,
) {
    let user_id = client.user_id().unwrap();
    if event.sender == user_id {
        return;
    }
    // The user for which a membership applies is represented by the state_key.
    if event.state_key != user_id {
        info!(
            "Ignoring invitation from {} to room {}: Someone else ({}) was invited.",
            event.sender,
            room.room_id(),
            event.state_key
        );
        return;
    }
    if room.state() != RoomState::Invited {
        info!(
            "Ignoring invitation from {} to room {}: Current room state is {:?}.",
            event.sender,
            room.room_id(),
            room.state()
        );
        return;
    }
    if !room.is_direct().await.unwrap_or(false) {
        info!(
            "Rejecting invitation from {} to room {}: Room is not a direct chat.",
            event.sender,
            room.room_id()
        );
        tokio::spawn(
            async move {
                match room.leave().await {
                    Ok(_) => {
                        info!("Rejected room {}.", room.room_id());
                    }
                    Err(err) => {
                        error!(
                            "Failed to reject room invitation {}: {}",
                            room.room_id(),
                            err
                        );
                    }
                }
            }
            .in_current_span(),
        );
        return;
    }
    info!(
        "Accepting invitation from {} to room {}.",
        event.sender,
        room.room_id()
    );

    let sender_link = event.sender.matrix_uri(false).to_string();
    tokio::spawn(send_log(
        &config,
        client.clone(),
        format!("<{}> invited me to a direct chat.", event.sender),
        Some(format!(
            "<a href=\"{}\">{}</a> invited me to a direct chat.",
            html_escape::attr(&sender_link),
            html_escape::text(event.sender.as_str())
        )),
    ));

    tokio::spawn(
        async move {
            for retry in 0.. {
                info!("Joining room {}.", room.room_id());
                match room.join().await {
                    Ok(_) => {
                        info!("Joined room {}.", room.room_id());
                        tokio::spawn(send_log(
                            &config,
                            client.clone(),
                            format!("Established a direct chat with <{}>.", event.sender),
                            Some(format!(
                                "Established a direct chat with <a href=\"{}\">{}</a>.",
                                html_escape::attr(&sender_link),
                                html_escape::text(event.sender.as_str())
                            )),
                        ));
                        return;
                    }
                    Err(err) => {
                        // https://github.com/matrix-org/synapse/issues/4345
                        if retry >= 16 {
                            error!("Failed to join room {}: {}", room.room_id(), err);
                            error!("Too many retries, giving up after 1 hour.");
                            let err_str = err.to_string();
                            tokio::spawn(send_log(
                                &config,
                                client.clone(),
                                format!(
                                    "Failed to establish a direct chat with <{}>: {}",
                                    event.sender,
                                    err_str
                                ),
                                Some(format!(
                                    "Failed to establish a direct chat with <a href=\"{}\">{}</a>: <pre>{}</pre>",
                                    html_escape::attr(&sender_link),
                                    html_escape::text(event.sender.as_str()),
                                    html_escape::text(&err_str),
                                )),
                            ));
                            return;
                        } else {
                            const BASE: f64 = 1.6180339887498947;
                            let duration = BASE.powi(retry);
                            warn!("Failed to join room {}: {}", room.room_id(), err);
                            warn!("This is common, will retry in {:.1}s.", duration);
                            tokio::time::sleep(Duration::from_secs_f64(duration)).await;
                        }
                    }
                }
            }
        }
        .in_current_span(),
    );
}

// https://spec.matrix.org/v1.14/client-server-api/#mroommember
#[instrument(skip_all)]
async fn on_leave(event: SyncRoomMemberEvent, room: Room) {
    if !matches!(
        event.membership(),
        MembershipState::Leave | MembershipState::Ban
    ) {
        return;
    }

    match room.state() {
        RoomState::Joined => {
            tokio::spawn(
                async move {
                    if let Err(err) = room.sync_members().await {
                        warn!("Failed to sync members of {}: {}", room.room_id(), err);
                    }
                    // Only I remain in the room.
                    if room.joined_members_count() <= 1 {
                        info!("Leaving room {}.", room.room_id());
                        match room.leave().await {
                            Ok(_) => info!("Left room {}.", room.room_id()),
                            Err(err) => {
                                error!("Failed to leave room {}: {}", room.room_id(), err)
                            }
                        }
                    }
                }
                .in_current_span(),
            );
        }
        RoomState::Banned | RoomState::Left => {
            // Either I successfully left the room, or someone kicked me out.
            tokio::spawn(
                async move {
                    info!("Forgetting room {}.", room.room_id());
                    match room.forget().await {
                        Ok(_) => info!("Forgot room {}.", room.room_id()),
                        Err(err) => error!("Failed to forget room {}: {}", room.room_id(), err),
                    }
                }
                .in_current_span(),
            );
        }
        _ => (),
    }
}
