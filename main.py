import asyncio
import logging
import os
import shelve
import signal
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import json
import threading
from collections import deque
import time

from telegram import Update, Message
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CommandHandler
from telegram.constants import ParseMode
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError
from dotenv import load_dotenv

load_dotenv()

# Configure logging with more detailed format
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('telegram_bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class BotConfig:
    """Configuration class for bot settings"""
    bot_token: str
    target_chat_id: str
    forward_to_chat_id: Optional[str] = None
    append_text: str = "\n\nJoin @YourChannel"
    max_concurrent_tasks: int = 1  # Sequential processing for FIFO
    delay_between_messages: float = 1.0  # Increased delay
    max_retries: int = 3
    retry_delay: float = 2.0
    queue_size: int = 2000
    batch_size: int = 5
    health_check_interval: int = 300
    sequential_processing: bool = True  # Force sequential processing

@dataclass
class QueuedMessage:
    """Message wrapper with metadata for queue processing"""
    update: Update
    context: ContextTypes.DEFAULT_TYPE
    timestamp: datetime
    message_id: int
    chat_id: int
    priority: int = 0  # Lower number = higher priority
    retry_count: int = 0

    def __lt__(self, other):
        """For priority queue sorting - earlier timestamp = higher priority"""
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.timestamp < other.timestamp

class OrderedMessageQueue:
    """Thread-safe FIFO message queue with strict ordering"""

    def __init__(self, max_size: int = 2000):
        self.max_size = max_size
        self._queue = deque()
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self._closed = False

        self.stats = {
            'total_received': 0,
            'total_processed': 0,
            'total_failed': 0,
            'queue_overflows': 0,
            'started_at': datetime.now(),
            'current_processing': None
        }

        # Sequence tracking for strict ordering
        self._sequence_counter = 0
        self._last_processed_sequence = -1

    async def put(self, message: QueuedMessage) -> bool:
        """Add message to queue with strict FIFO ordering"""
        async with self._condition:
            if self._closed:
                return False

            # Assign sequence number for strict ordering
            message.sequence = self._sequence_counter
            self._sequence_counter += 1

            if len(self._queue) >= self.max_size:
                # Remove oldest message to make space
                removed = self._queue.popleft()
                self.stats['queue_overflows'] += 1
                logger.warning(f"Queue overflow: Removed message {removed.message_id} to make space")

            # Add to end of queue (FIFO)
            self._queue.append(message)
            self.stats['total_received'] += 1

            logger.debug(f"Queued message {message.message_id} with sequence {message.sequence}")

            # Notify waiting workers
            self._condition.notify()
            return True

    async def get(self) -> Optional[QueuedMessage]:
        """Get next message in FIFO order"""
        async with self._condition:
            while not self._queue and not self._closed:
                await self._condition.wait()

            if self._closed and not self._queue:
                return None

            if self._queue:
                message = self._queue.popleft()  # FIFO order
                self.stats['current_processing'] = {
                    'message_id': message.message_id,
                    'sequence': message.sequence,
                    'started_at': datetime.now().isoformat()
                }
                return message

            return None

    async def mark_processed(self, message: QueuedMessage, success: bool = True):
        """Mark message as processed and update sequence tracking"""
        async with self._lock:
            if success:
                self.stats['total_processed'] += 1
                self._last_processed_sequence = message.sequence
                logger.debug(f"Marked message {message.message_id} (seq: {message.sequence}) as processed")
            else:
                self.stats['total_failed'] += 1
                logger.debug(f"Marked message {message.message_id} (seq: {message.sequence}) as failed")

            self.stats['current_processing'] = None

    async def requeue_failed(self, message: QueuedMessage):
        """Requeue failed message with higher priority"""
        message.retry_count += 1
        message.priority = -1  # Higher priority for retries
        message.timestamp = datetime.now()  # Update timestamp

        async with self._condition:
            # Add to front for retry priority
            self._queue.appendleft(message)
            logger.info(f"Requeued failed message {message.message_id} (attempt {message.retry_count})")
            self._condition.notify()

    async def close(self):
        """Close the queue gracefully"""
        async with self._condition:
            self._closed = True
            self._condition.notify_all()

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics"""
        runtime = datetime.now() - self.stats['started_at']
        rate_per_minute = 0
        if runtime.total_seconds() > 0:
            rate_per_minute = round(self.stats['total_processed'] / (runtime.total_seconds() / 60), 2)

        return {
            **self.stats,
            'queue_size': len(self._queue),
            'runtime_seconds': int(runtime.total_seconds()),
            'rate_per_minute': rate_per_minute,
            'success_rate': round(
                self.stats['total_processed'] / max(self.stats['total_processed'] + self.stats['total_failed'], 1) * 100, 1
            ),
            'last_processed_sequence': self._last_processed_sequence,
            'next_sequence': self._sequence_counter,
            'sequence_gap': self._sequence_counter - self._last_processed_sequence - 1
        }

class DatabaseManager:
    """Thread-safe database operations with sequence tracking"""

    def __init__(self, db_path: str = "processed_messages"):
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self.sequence_db_path = f"{db_path}_sequences"

    @asynccontextmanager
    async def get_db(self, db_path: str = None):
        """Context manager for safe database access"""
        path = db_path or self.db_path
        async with self._lock:
            db = None
            try:
                db = shelve.open(path, writeback=True)
                yield db
            except Exception as e:
                logger.error(f"Database error for {path}: {e}")
                try:
                    if db:
                        db.close()
                    # Try to recover
                    backup_path = f"{path}_backup_{int(time.time())}"
                    if os.path.exists(f"{path}.db"):
                        os.rename(f"{path}.db", f"{backup_path}.db")
                    db = shelve.open(path, writeback=True)
                    yield db
                except Exception as e2:
                    logger.error(f"Failed to recover database {path}: {e2}")
                    yield {}
            finally:
                if db:
                    try:
                        db.close()
                    except:
                        pass

    async def is_processed(self, message_id: int) -> bool:
        """Check if message was already processed"""
        try:
            async with self.get_db() as db:
                return str(message_id) in db
        except Exception as e:
            logger.error(f"Error checking processed status: {e}")
            return False

    async def mark_as_processed(self, message: QueuedMessage):
        """Mark message as processed with sequence tracking"""
        try:
            async with self.get_db() as db:
                db[str(message.message_id)] = {
                    'processed_at': datetime.now().isoformat(),
                    'sequence': message.sequence,
                    'retry_count': message.retry_count,
                    'status': 'completed'
                }

            # Also track sequence separately
            async with self.get_db(self.sequence_db_path) as seq_db:
                seq_db[str(message.sequence)] = {
                    'message_id': message.message_id,
                    'processed_at': datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"Error marking message as processed: {e}")

    async def get_processing_gaps(self) -> List[int]:
        """Identify gaps in processed sequence numbers"""
        try:
            async with self.get_db(self.sequence_db_path) as seq_db:
                processed_sequences = [int(k) for k in seq_db.keys()]
                if not processed_sequences:
                    return []

                processed_sequences.sort()
                gaps = []
                for i in range(processed_sequences[0], processed_sequences[-1]):
                    if i not in processed_sequences:
                        gaps.append(i)

                return gaps
        except Exception as e:
            logger.error(f"Error checking processing gaps: {e}")
            return []

class TelegramRepostBot:
    def __init__(self, config: BotConfig):
        self.config = config
        self.message_queue = OrderedMessageQueue(config.queue_size)
        self.db_manager = DatabaseManager()
        self.application: Optional[Application] = None
        self.worker_tasks = []
        self.is_running = False
        self.shutdown_event = asyncio.Event()
        self.processing_lock = asyncio.Lock()  # For sequential processing

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        asyncio.create_task(self.shutdown())

    async def shutdown(self):
        """Graceful shutdown with queue completion"""
        logger.info("Starting graceful shutdown...")
        self.is_running = False

        # Close queue to new messages
        await self.message_queue.close()

        # Wait for current processing to complete
        logger.info("Waiting for current message processing to complete...")

        # Wait for workers to finish
        if self.worker_tasks:
            logger.info("Waiting for worker tasks to complete...")
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)

        # Stop the application
        if self.application:
            await self.application.stop()
            await self.application.shutdown()

        self.shutdown_event.set()
        logger.info("Shutdown complete")

    def _escape_html(self, text: str) -> str:
        """Enhanced HTML escaping for Telegram"""
        if not text:
            return ""
        return (text.replace('&', '&amp;')
                   .replace('<', '&lt;')
                   .replace('>', '&gt;')
                   .replace('"', '&quot;')
                   .replace("'", '&#x27;'))

    async def handle_channel_post(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle incoming channel posts by queuing them with timestamp"""
        try:
            message = update.channel_post
            if not message:
                return

            queued_message = QueuedMessage(
                update=update,
                context=context,
                timestamp=datetime.now(),
                message_id=message.message_id,
                chat_id=message.chat.id
            )

            success = await self.message_queue.put(queued_message)
            if not success:
                logger.error(f"Failed to queue message {message.message_id}")
            else:
                logger.info(f"Queued message {message.message_id} at {queued_message.timestamp}")

        except Exception as e:
            logger.error(f"Error handling channel post: {e}")

    async def sequential_message_worker(self):
        """Single worker for sequential FIFO processing"""
        logger.info("Sequential message worker started")

        while self.is_running:
            try:
                # Get next message in FIFO order
                queued_message = await self.message_queue.get()
                if not queued_message:
                    if not self.is_running:
                        break
                    continue

                logger.info(f"Processing message {queued_message.message_id} "
                           f"(seq: {queued_message.sequence}) "
                           f"queued at {queued_message.timestamp}")

                # Process with retry logic
                success = await self.process_message_with_retry(queued_message)

                if success:
                    await self.message_queue.mark_processed(queued_message, True)
                    await self.db_manager.mark_as_processed(queued_message)
                else:
                    await self.message_queue.mark_processed(queued_message, False)

                    # Requeue if retries remaining
                    if queued_message.retry_count < self.config.max_retries:
                        await self.message_queue.requeue_failed(queued_message)
                    else:
                        logger.error(f"Permanently failed message {queued_message.message_id} "
                                   f"after {queued_message.retry_count} retries")

                # Mandatory delay between messages
                await asyncio.sleep(self.config.delay_between_messages)

            except Exception as e:
                logger.error(f"Sequential worker error: {e}")
                await asyncio.sleep(1)

        logger.info("Sequential message worker stopped")

    async def process_message_with_retry(self, queued_message: QueuedMessage) -> bool:
        """Process message with comprehensive error handling"""
        message = queued_message.update.channel_post
        if not message:
            return False

        message_id = message.message_id
        chat_id = message.chat.id

        # Check if already processed (skip duplicates)
        if await self.db_manager.is_processed(message_id):
            logger.debug(f"Skipping already processed message {message_id}")
            return True

        # Sequential processing lock (if configured)
        async with self.processing_lock:
            try:
                success = await self._process_single_message(
                    queued_message.context, message, chat_id, message_id
                )

                if success:
                    logger.info(f"Successfully processed message {message_id} "
                               f"(seq: {queued_message.sequence})")
                    return True

            except RetryAfter as e:
                wait_time = e.retry_after + 1
                logger.warning(f"Rate limited, waiting {wait_time}s for message {message_id}")
                await asyncio.sleep(wait_time)
                return False  # Will be retried

            except (TimedOut, NetworkError) as e:
                logger.warning(f"Network error for message {message_id}: {e}")
                return False  # Will be retried

            except TelegramError as e:
                logger.error(f"Telegram error for message {message_id}: {e}")
                if "message not found" in str(e).lower():
                    # Message was deleted, consider it processed
                    return True
                return False  # Will be retried

            except Exception as e:
                logger.error(f"Unexpected error processing message {message_id}: {e}")
                return False  # Will be retried

            logger.error(f"Failed to process message {message_id}")
            return False

    async def _process_single_message(self, context, message, chat_id, message_id) -> bool:
        """Process a single message based on its type"""
        try:
            if message.photo:
                return await self._handle_photo(context, message, chat_id, message_id)
            elif message.video:
                return await self._handle_video(context, message, chat_id, message_id)
            elif message.document:
                return await self._handle_document(context, message, chat_id, message_id)
            elif message.text:
                return await self._handle_text(context, message, chat_id, message_id)
            else:
                logger.info(f"Unsupported message type from {chat_id}")
                return True  # Consider unsupported types as "processed"

        except Exception as e:
            logger.error(f"Error in _process_single_message: {e}")
            return False

    async def _handle_photo(self, context, message, original_chat_id, original_message_id) -> bool:
        """Handle photo messages"""
        try:
            photo = message.photo[-1]
            caption = (message.caption or "") + self.config.append_text
            bold_caption = f"<b>{self._escape_html(caption)}</b>"

            sent_message = await context.bot.send_photo(
                chat_id=self.config.target_chat_id,
                photo=photo.file_id,
                caption=bold_caption[:1024],
                parse_mode=ParseMode.HTML
            )

            if self.config.forward_to_chat_id:
                await self._forward_message(context, sent_message)

            await self._delete_original_message(context, original_chat_id, original_message_id)
            return True

        except Exception as e:
            logger.error(f"Error handling photo {original_message_id}: {e}")
            return False

    async def _handle_video(self, context, message, original_chat_id, original_message_id) -> bool:
        """Handle video messages"""
        try:
            caption = (message.caption or "") + self.config.append_text
            bold_caption = f"<b>{self._escape_html(caption)}</b>"

            sent_message = await context.bot.send_video(
                chat_id=self.config.target_chat_id,
                video=message.video.file_id,
                caption=bold_caption[:1024],
                parse_mode=ParseMode.HTML
            )

            if self.config.forward_to_chat_id:
                await self._forward_message(context, sent_message)

            await self._delete_original_message(context, original_chat_id, original_message_id)
            return True

        except Exception as e:
            logger.error(f"Error handling video {original_message_id}: {e}")
            return False

    async def _handle_document(self, context, message, original_chat_id, original_message_id) -> bool:
        """Handle document messages"""
        try:
            caption = (message.caption or "") + self.config.append_text
            bold_caption = f"<b>{self._escape_html(caption)}</b>"

            sent_message = await context.bot.send_document(
                chat_id=self.config.target_chat_id,
                document=message.document.file_id,
                caption=bold_caption[:1024],
                parse_mode=ParseMode.HTML
            )

            if self.config.forward_to_chat_id:
                await self._forward_message(context, sent_message)

            await self._delete_original_message(context, original_chat_id, original_message_id)
            return True

        except Exception as e:
            logger.error(f"Error handling document {original_message_id}: {e}")
            return False

    async def _handle_text(self, context, message, original_chat_id, original_message_id) -> bool:
        """Handle text messages"""
        try:
            text = message.text + self.config.append_text
            bold_text = f"<b>{self._escape_html(text)}</b>"

            sent_message = await context.bot.send_message(
                chat_id=self.config.target_chat_id,
                text=bold_text[:4096],
                parse_mode=ParseMode.HTML
            )

            if self.config.forward_to_chat_id:
                await self._forward_message(context, sent_message)

            await self._delete_original_message(context, original_chat_id, original_message_id)
            return True

        except Exception as e:
            logger.error(f"Error handling text {original_message_id}: {e}")
            return False

    async def _forward_message(self, context: ContextTypes.DEFAULT_TYPE, message_to_forward: Message):
        """Forward a message with error handling"""
        try:
            await context.bot.forward_message(
                chat_id=self.config.forward_to_chat_id,
                from_chat_id=message_to_forward.chat.id,
                message_id=message_to_forward.message_id
            )
            logger.debug(f"Forwarded message {message_to_forward.message_id}")
        except Exception as e:
            logger.warning(f"Error forwarding message: {e}")

    async def _delete_original_message(self, context, chat_id, message_id):
        """Delete the original message with error handling"""
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.debug(f"Deleted original message {message_id}")
        except Exception as e:
            logger.debug(f"Could not delete message {message_id}: {e}")

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Health check command with statistics"""
        stats = self.message_queue.get_stats()
        response = (
            "🤖 Repost bot is running (FIFO mode)!\n\n"
            f"📊 Statistics:\n"
            f"• Processed: {stats['total_processed']}\n"
            f"• Failed: {stats['total_failed']}\n"
            f"• Queue size: {stats['queue_size']}\n"
            f"• Success rate: {stats['success_rate']}%\n"
            f"• Rate: {stats['rate_per_minute']}/min\n"
            f"• Runtime: {stats['runtime_seconds']}s\n"
            f"• Sequence gap: {stats['sequence_gap']}"
        )
        await update.message.reply_text(response)
        logger.info("Responded to /start command")

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Detailed statistics command"""
        stats = self.message_queue.get_stats()
        gaps = await self.db_manager.get_processing_gaps()

        response = (
            "📈 Detailed Queue Statistics:\n\n"
            f"Queue Status:\n"
            f"• Current queue size: {stats['queue_size']}\n"
            f"• Max queue size: {self.config.queue_size}\n"
            f"• Queue overflows: {stats['queue_overflows']}\n\n"
            f"Processing Stats:\n"
            f"• Total received: {stats['total_received']}\n"
            f"• Total processed: {stats['total_processed']}\n"
            f"• Total failed: {stats['total_failed']}\n"
            f"• Success rate: {stats['success_rate']}%\n"
            f"• Rate: {stats['rate_per_minute']} messages/minute\n\n"
            f"Sequence Tracking:\n"
            f"• Last processed: {stats['last_processed_sequence']}\n"
            f"• Next sequence: {stats['next_sequence']}\n"
            f"• Processing gaps: {len(gaps)}\n\n"
            f"Runtime: {stats['runtime_seconds']} seconds\n"
            f"Mode: Sequential FIFO processing"
        )

        if stats['current_processing']:
            current = stats['current_processing']
            response += f"\n\n🔄 Currently processing:\n"
            response += f"• Message ID: {current['message_id']}\n"
            response += f"• Sequence: {current['sequence']}\n"
            response += f"• Started: {current['started_at']}"

        await update.message.reply_text(response)

    async def queue_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show current queue status"""
        stats = self.message_queue.get_stats()

        response = (
            "📋 Queue Status:\n\n"
            f"• Queue size: {stats['queue_size']}/{self.config.queue_size}\n"
            f"• Processing mode: Sequential FIFO\n"
            f"• Delay between messages: {self.config.delay_between_messages}s\n"
            f"• Max retries: {self.config.max_retries}\n\n"
            f"Recent activity:\n"
            f"• Messages received: {stats['total_received']}\n"
            f"• Successfully processed: {stats['total_processed']}\n"
            f"• Failed: {stats['total_failed']}\n"
            f"• Queue overflows: {stats['queue_overflows']}"
        )

        await update.message.reply_text(response)

    async def health_monitor(self):
        """Background task to monitor bot health and queue status"""
        while self.is_running:
            try:
                stats = self.message_queue.get_stats()
                logger.info(f"Health check - Queue: {stats['queue_size']}, "
                           f"Processed: {stats['total_processed']}, "
                           f"Failed: {stats['total_failed']}, "
                           f"Rate: {stats['rate_per_minute']}/min, "
                           f"Seq gap: {stats['sequence_gap']}")

                # Alert on high queue size
                if stats['queue_size'] > self.config.queue_size * 0.8:
                    logger.warning(f"Queue is {round(stats['queue_size'] / self.config.queue_size * 100)}% full")

                # Alert on large sequence gaps
                if stats['sequence_gap'] > 50:
                    logger.warning(f"Large sequence gap detected: {stats['sequence_gap']} messages")

                await asyncio.sleep(self.config.health_check_interval)

            except Exception as e:
                logger.error(f"Health monitor error: {e}")
                await asyncio.sleep(60)

    def run(self):
        """Start the bot with sequential processing"""
        try:
            # Create application
            self.application = Application.builder().token(self.config.bot_token).build()

            # Add handlers
            self.application.add_handler(CommandHandler("start", self.start_command))
            self.application.add_handler(CommandHandler("stats", self.stats_command))
            self.application.add_handler(CommandHandler("queue", self.queue_command))
            self.application.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST, self.handle_channel_post))

            logger.info("Starting Telegram repost bot with FIFO queue processing...")

            # Start the event loop
            asyncio.run(self._run_async())

        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            sys.exit(1)

    async def _run_async(self):
        """Async runner for the bot"""
        try:
            self.is_running = True

            # Start single sequential worker
            self.worker_tasks = [
                asyncio.create_task(self.sequential_message_worker())
            ]

            # Start health monitor
            health_task = asyncio.create_task(self.health_monitor())

            # Start the bot
            await self.application.initialize()
            await self.application.start()

            logger.info("Bot started with sequential FIFO processing")

            # Run polling
            polling_task = asyncio.create_task(
                self.application.updater.start_polling(allowed_updates=["channel_post", "message"])
            )

            # Wait for shutdown signal
            await self.shutdown_event.wait()

            # Cleanup
            polling_task.cancel()
            health_task.cancel()

            try:
                await asyncio.gather(polling_task, health_task, return_exceptions=True)
            except:
                pass

        except Exception as e:
            logger.error(f"Error in async runner: {e}")
        finally:
            await self.shutdown()

def create_config() -> BotConfig:
    """Create configuration from environment variables"""
    bot_token = os.getenv("BOT_TOKEN")
    target_chat_id = os.getenv("TARGET_CHAT_ID")
    forward_to_chat_id = os.getenv("FORWARD_TO_CHAT_ID")
    append_text = os.getenv("APPEND_TEXT", "\n\nJoin @YourChannel")

    if not bot_token or not target_chat_id:
        raise ValueError("Missing required environment variables: BOT_TOKEN and TARGET_CHAT_ID")

    return BotConfig(
        bot_token=bot_token,
        target_chat_id=target_chat_id,
        forward_to_chat_id=forward_to_chat_id,
        append_text=append_text,
        max_concurrent_tasks=1,  # Sequential processing
        delay_between_messages=float(os.getenv("DELAY_BETWEEN_MESSAGES", "1.0")),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
        queue_size=int(os.getenv("QUEUE_SIZE", "2000")),
        sequential_processing=True
    )

if __name__ == "__main__":
    try:
        config = create_config()
        bot = TelegramRepostBot(config)
        bot.run()
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        sys.exit(1)
