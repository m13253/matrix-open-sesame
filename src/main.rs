use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use color_eyre::eyre::Result;
use futures::future::OptionFuture;
use matrix_sdk::config::SyncSettings;
use matrix_sdk::event_handler::Ctx;
use matrix_sdk::room::Receipts;
use matrix_sdk::ruma::api::client::filter::FilterDefinition;
use matrix_sdk::ruma::events::relation::{InReplyTo, Thread};
use matrix_sdk::ruma::events::room::encrypted::SyncRoomEncryptedEvent;
use matrix_sdk::ruma::events::room::member::{
    MembershipState, StrippedRoomMemberEvent, SyncRoomMemberEvent,
};
use matrix_sdk::ruma::events::room::message::{
    MessageType, OriginalSyncRoomMessageEvent, Relation, RoomMessageEventContent,
    TextMessageEventContent,
};
use matrix_sdk::ruma::events::sticker::OriginalSyncStickerEvent;
use matrix_sdk::ruma::{OwnedEventId, RoomId};
use matrix_sdk::{Client, Room, RoomState};
use tracing::{error, info, instrument, warn};
use tracing_subscriber::{EnvFilter, prelude::*};

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
            long = "data",
            value_name = "PATH",
            help = "Path to store Matrix data between sessions"
        )]
        data_dir: PathBuf,
        #[clap(
            long,
            value_name = "DEVICE_NAME",
            default_value = "matrixbot-ezlogin/open-sesame",
            help = "Device name to use for this session"
        )]
        device_name: String,
    },
    #[clap(about = "Run the bot")]
    Run {
        #[clap(
            long = "data",
            value_name = "PATH",
            help = "Path to an existing Matrix session"
        )]
        data_dir: PathBuf,
        #[clap(
            long = "passbook",
            value_name = "PATH",
            help = "Path to the passphrase file"
        )]
        passbook_path: PathBuf,
    },
    #[clap(about = "Log out of the Matrix session, and delete the state database")]
    Logout {
        #[clap(
            long = "data",
            value_name = "PATH",
            help = "Path to an existing Matrix session"
        )]
        data_dir: PathBuf,
    },
}

type Passbook = HashMap<String, String>;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    matrixbot_ezlogin::DuplexLog::init();
    tracing_subscriber::registry()
        .with(tracing_error::ErrorLayer::default())
        .with({
            let mut filter = EnvFilter::new("warn,open_sesame=debug,matrixbot_ezlogin=info");
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
            data_dir,
            device_name,
        } => drop(matrixbot_ezlogin::setup_interactive(&data_dir, &device_name).await?),
        Command::Run {
            data_dir,
            passbook_path,
        } => run(&data_dir, &passbook_path).await?,
        Command::Logout { data_dir } => matrixbot_ezlogin::logout(&data_dir).await?,
    };
    Ok(())
}

async fn run(data_dir: &Path, passbook_path: &Path) -> Result<()> {
    info!("Loading passphrase file");
    let passbook = toml::from_str::<Passbook>(&tokio::fs::read_to_string(passbook_path).await?)?;

    let (client, sync_helper) = matrixbot_ezlogin::login(data_dir).await?;

    // We don't ignore joining and leaving events happened during downtime.
    client.add_event_handler(on_invite);
    client.add_event_handler(on_leave);

    // Enable room members lazy-loading, it will speed up the initial sync a lot with accounts in lots of rooms.
    // https://spec.matrix.org/v1.6/client-server-api/#lazy-loading-room-members
    let sync_settings =
        SyncSettings::default().filter(FilterDefinition::with_lazy_loading().into());

    info!("Skipping messages since last logout.");
    sync_helper
        .sync_once(&client, sync_settings.clone())
        .await?;

    client.add_event_handler_context(passbook);
    client.add_event_handler(on_message);
    client.add_event_handler(on_sticker);
    client.add_event_handler(on_utd);

    info!("Starting sync.");
    sync_helper.sync(&client, sync_settings).await?;

    Ok(())
}

#[instrument(skip_all)]
fn set_read_marker(room: Room, event_id: OwnedEventId) {
    tokio::spawn(async move {
        if let Err(err) = room
            .send_multiple_receipts(
                Receipts::new()
                    .fully_read_marker(event_id.clone())
                    .public_read_receipt(event_id.clone()),
            )
            .await
        {
            error!(
                "Failed to set the read marker of room {} to event {}: {:?}",
                room.room_id(),
                event_id,
                err
            );
        }
    });
}

#[instrument(skip_all)]
fn send_reply(
    room: Room,
    thread_id: Option<&Thread>,
    event_id: OwnedEventId,
    text: String,
    html: Option<String>,
) -> impl Future<Output = ()> + use<> {
    let mut reply = match html {
        Some(html) => RoomMessageEventContent::notice_html(text, html),
        None => RoomMessageEventContent::notice_plain(text),
    };
    // We should use make_reply_to, but it embeds the original message body, which I don't want
    reply.relates_to = match thread_id {
        Some(thread) => Some(Relation::Thread(Thread::reply(
            thread.event_id.clone(),
            event_id.clone(),
        ))),
        _ => Some(Relation::Reply {
            in_reply_to: InReplyTo::new(event_id.clone()),
        }),
    };

    async move {
        info!("Sending a reply to {}.", event_id);
        match room.send(reply).await {
            Ok(_) => info!("Sent a reply to {}.", event_id),
            Err(err) => error!("Failed to send a reply to {}: {:?}", event_id, err),
        }
    }
}

#[instrument(skip_all)]
async fn matrix_to_matrix_to_permalink_with_fallback(
    room: Option<&Room>,
    fallback_room_id: &str,
) -> String {
    const ESCAPE_SET_1: percent_encoding::AsciiSet = percent_encoding::CONTROLS.add(b' ').add(b'"');
    const ESCAPE_SET_2: percent_encoding::AsciiSet = ESCAPE_SET_1
        .add(b'%')
        .add(b'&')
        .add(b'+')
        .add(b'/')
        .add(b'<')
        .add(b'>')
        .add(b'?');
    OptionFuture::from(room.map(Room::matrix_to_permalink))
        .await
        .and_then(|permalink| {
            Some(
                percent_encoding::utf8_percent_encode(&permalink.ok()?.to_string(), &ESCAPE_SET_1)
                    .to_string(),
            )
        })
        .unwrap_or_else(|| {
            format!(
                "https://matrix.to/#/{}",
                percent_encoding::utf8_percent_encode(fallback_room_id, &ESCAPE_SET_2)
            )
        })
}

// https://spec.matrix.org/v1.14/client-server-api/#mroommessage
#[instrument(skip_all)]
async fn on_message(
    event: OriginalSyncRoomMessageEvent,
    room: Room,
    client: Client,
    passbook: Ctx<Passbook>,
) {
    if event.sender == client.user_id().unwrap() {
        // Ignore my own message
        return;
    }
    info!("room = {}, event = {:?}", room.room_id(), event);
    if !room.is_direct().await.unwrap_or(false) {
        return;
    }
    set_read_marker(room.clone(), event.event_id.clone());
    if room.state() != RoomState::Joined {
        info!("Ignoring: Current room state is {:?}.", room.state());
        return;
    }
    let thread = match event.content.relates_to {
        Some(Relation::Replacement(_)) => return,
        Some(Relation::Thread(ref thread)) => Some(thread),
        _ => None,
    };

    let target_room_id = 'L1: {
        let MessageType::Text(TextMessageEventContent { ref body, .. }) = event.content.msgtype
        else {
            break 'L1 None;
        };
        let passphrase = body.trim();
        if passphrase.is_empty() {
            break 'L1 None;
        }
        passbook.get(passphrase)
    };
    let Some(target_room_id) = target_room_id else {
        tokio::spawn(send_reply(
            room,
            thread,
            event.event_id,
            "Incorrect passphrase, please try again.".to_owned(),
            None,
        ));
        return;
    };
    let target_room = 'L2: {
        let parsed = match RoomId::parse(&target_room_id) {
            Ok(parsed) => parsed,
            Err(err) => {
                error!("Failed to parse room ID {}: {:?}", target_room_id, err);
                break 'L2 None;
            }
        };
        let Some(room) = client.get_room(&parsed) else {
            error!("Failed to get room from ID {}.", target_room_id);
            break 'L2 None;
        };
        Some(room)
    };
    let Some(target_room) = target_room else {
        let link = matrix_to_matrix_to_permalink_with_fallback(None, target_room_id).await;
        tokio::spawn(send_reply(
            room,
            thread,
            event.event_id,
            format!("I’m trying to invite you to <{link}>, but something went wrong."),
            Some(format!(
                "I’m trying to invite you to <a href=\"{link}\">{link}</a>, but something went wrong.",
            )),
        ));
        return;
    };

    let thread = thread.cloned();
    tokio::spawn(async move {
        info!("Generating room link.");
        let link = matrix_to_matrix_to_permalink_with_fallback(
            Some(&target_room),
            target_room.room_id().as_str(),
        )
        .await;
        info!("Room link: {link}");

        info!(
            "Checking the membership of {} in {}.",
            event.sender,
            target_room.room_id()
        );
        if let Err(err) = room.sync_members().await {
            warn!("Failed to sync members of {}: {:?}", room.room_id(), err);
        }
        let invite_failure_is_normal = match room.get_member(&event.sender).await {
            Ok(Some(member)) => {
                let membership = member.membership();
                info!(
                    "The membership of {} in chat {} is {:?}.",
                    event.sender,
                    target_room.room_id(),
                    membership
                );
                match membership {
                    // If the sender is banned, react as if they are joined. Their client will tell them the truth.
                    MembershipState::Ban | MembershipState::Invite | MembershipState::Join => true,
                    _ => false,
                }
            }
            Ok(None) => {
                info!(
                    "User {} is not in chat {}.",
                    event.sender,
                    target_room.room_id()
                );
                false
            }
            Err(err) => {
                warn!(
                    "Failed to check if {} is already in chat {}: {:?}",
                    event.sender,
                    target_room.room_id(),
                    err
                );
                false
            }
        };

        info!(
            "Inviting {} to chat {}.",
            event.sender,
            target_room.room_id()
        );
        send_reply(
            room.clone(),
            thread.as_ref(),
            event.event_id.clone(),
            format!("I’m inviting you to <{link}>, one second please…"),
            Some(format!(
                "I’m inviting you to <a href=\"{link}\">{link}</a>, one second please…",
            )),
        )
        .await;
        let invite_is_successful = match target_room.invite_user_by_id(&event.sender).await {
            Ok(_) => {
                info!(
                    "Invited {} to chat {}.",
                    event.sender,
                    target_room.room_id()
                );
                true
            }
            Err(err) => {
                error!(
                    "Failed to invite {} to chat {}: {:?}",
                    event.sender,
                    target_room.room_id(),
                    err
                );
                false
            }
        };

        if invite_is_successful || invite_failure_is_normal {
            send_reply(
                room.clone(),
                thread.as_ref(),
                event.event_id,
                format!("Welcome to <{link}>!"),
                Some(format!("Welcome to <a href=\"{link}\">{link}</a>!",)),
            )
            .await;
        } else {
            send_reply(
                room.clone(),
                thread.as_ref(),
                event.event_id,
                format!("I’ve tried to invite you to <{link}>, but something went wrong.",),
                Some(format!(
                    "I’ve tried to invite you to <a href=\"{link}\">{link}</a>, but something went wrong.",
                )),
            ).await;
        }
    });
}

// Sticker messages aren't of m.room.message types.
// Basically it means you need to write the logic again with a different type.
// https://spec.matrix.org/v1.14/client-server-api/#sticker-messages
#[instrument(skip_all)]
async fn on_sticker(event: OriginalSyncStickerEvent, room: Room, client: Client) {
    if event.sender == client.user_id().unwrap() {
        // Ignore my own message
        return;
    }
    info!("room = {}, event = {:?}", room.room_id(), event);
    if !room.is_direct().await.unwrap_or(false) {
        return;
    }
    set_read_marker(room.clone(), event.event_id.clone());
    if room.state() != RoomState::Joined {
        info!("Ignoring: Current room state is {:?}.", room.state());
        return;
    }
    let thread = match event.content.relates_to {
        Some(Relation::Replacement(_)) => return,
        Some(Relation::Thread(ref thread)) => Some(thread),
        _ => None,
    };
    tokio::spawn(send_reply(
        room,
        thread,
        event.event_id,
        "Incorrect passphrase, please try again.".to_owned(),
        None,
    ));
}

// The SDK documentation said nothing about how to catch unable-to-decrypt (UTD) events.
// But it seems this handler can capture them.
#[instrument(skip_all)]
async fn on_utd(event: SyncRoomEncryptedEvent, room: Room) {
    info!("room = {}, event = {:?}", room.room_id(), event);
    error!("Unable to decrypt message {}.", event.event_id());
    set_read_marker(room, event.event_id().to_owned());
}

// https://spec.matrix.org/v1.14/client-server-api/#mroommember
// https://spec.matrix.org/v1.14/client-server-api/#stripped-state
#[instrument(skip_all)]
async fn on_invite(event: StrippedRoomMemberEvent, room: Room, client: Client) {
    let user_id = client.user_id().unwrap();
    if event.sender == user_id {
        return;
    }
    info!("room = {}, event = {:?}", room.room_id(), event);
    // The user for which a membership applies is represented by the state_key.
    if event.state_key != user_id {
        info!("Ignoring: Someone else was invited.");
        return;
    }
    if room.state() != RoomState::Invited {
        info!("Ignoring: Current room state is {:?}.", room.state());
        return;
    }

    tokio::spawn(async move {
        for retry in 0.. {
            info!("Joining room {}.", room.room_id());
            match room.join().await {
                Ok(_) => {
                    info!("Joined room {}.", room.room_id());
                    return;
                }
                Err(err) => {
                    // https://github.com/matrix-org/synapse/issues/4345
                    if retry >= 16 {
                        error!("Failed to join room {}: {:?}", room.room_id(), err);
                        error!("Too many retries, giving up after 1 hour.");
                        return;
                    } else {
                        const BASE: f64 = 1.6180339887498947;
                        let duration = BASE.powi(retry);
                        warn!("Failed to join room {}: {:?}", room.room_id(), err);
                        warn!("This is common, will retry in {:.1}s.", duration);
                        tokio::time::sleep(Duration::from_secs_f64(duration)).await;
                    }
                }
            }
        }
    });
}

// https://spec.matrix.org/v1.14/client-server-api/#mroommember
// Each m.room.member event occurs twice in SyncResponse, one as state event, another as timeline event.
// As of matrix_sdk-0.11.0, if our handler matches SyncRoomMemberEvent, the event handler will actually be called twice.
// (Reference: matrix_sdk::Client::call_sync_response_handlers, https://github.com/matrix-org/matrix-rust-sdk/pull/4947)
// Thankfully, leaving a room twice does not return errors.
#[instrument(skip_all)]
async fn on_leave(event: SyncRoomMemberEvent, room: Room) {
    if !matches!(
        event.membership(),
        MembershipState::Leave | MembershipState::Ban
    ) {
        return;
    }
    info!("room = {}, event = {:?}", room.room_id(), event);

    match room.state() {
        RoomState::Joined => {
            tokio::spawn(async move {
                if let Err(err) = room.sync_members().await {
                    warn!("Failed to sync members of {}: {:?}", room.room_id(), err);
                }
                // Only I remain in the room.
                if room.joined_members_count() <= 1 {
                    info!("Leaving room {}.", room.room_id());
                    match room.leave().await {
                        Ok(_) => info!("Left room {}.", room.room_id()),
                        Err(err) => error!("Failed to leave room {}: {:?}", room.room_id(), err),
                    }
                }
            });
        }
        RoomState::Banned | RoomState::Left => {
            // Either I successfully left the room, or someone kicked me out.
            tokio::spawn(async move {
                info!("Forgetting room {}.", room.room_id());
                match room.forget().await {
                    Ok(_) => info!("Forgot room {}.", room.room_id()),
                    Err(err) => error!("Failed to forget room {}: {:?}", room.room_id(), err),
                }
            });
        }
        _ => (),
    }
}
