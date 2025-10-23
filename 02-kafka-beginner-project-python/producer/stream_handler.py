"""
Stream handler for Binance WebSocket and Wikimedia SSE streams
"""
import json
import time
import threading
import logging
import requests
from websocket import WebSocketApp
import sseclient

logger = logging.getLogger('stream-handler')


class StreamEventHandler:
    """Handles Binance or Wikimedia stream and forwards to Kafka"""

    def __init__(self, producer, topic, url, source):
        self.producer = producer
        self.topic = topic
        self.url = url
        self.source = source
        self.msg_count = 0
        self.ws = None
        self.running = False
        self.error_count = 0
        self.last_flush_time = time.time()

    def on_open(self, ws):
        """WebSocket connection opened"""
        logger.info('✅ WebSocket connection opened')

    def on_close(self, ws, close_status_code, close_msg):
        """WebSocket connection closed"""
        logger.info(f'🔴 WebSocket closed: {close_status_code} - {close_msg}')

    def on_error(self, ws, error):
        """WebSocket error occurred"""
        self.error_count += 1
        logger.warning(f'⚠️ WebSocket error: {error}')

    def on_message(self, ws, message):
        """Handle Binance stream messages"""
        try:
            data = json.loads(message)
            self.producer.send(self.topic, data)
            self.msg_count += 1
            
            # Log every 100 messages
            if self.msg_count % 100 == 0:
                logger.info(f"📊 Processed {self.msg_count} Binance events")
                self.producer.flush()
                
        except json.JSONDecodeError as e:
            logger.warning(f"⚠️ Invalid JSON in Binance message: {e}")
        except Exception as e:
            logger.error(f"❌ Error handling Binance message: {e}")

    def _run_binance_stream(self):
        """Start Binance WebSocket stream"""
        logger.info("🚀 Connecting to Binance WebSocket...")
        
        self.ws = WebSocketApp(
            url=self.url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_close=self.on_close,
            on_error=self.on_error
        )
        
        # Run with automatic reconnection
        self.ws.run_forever(
            ping_interval=30,
            ping_timeout=10,
            reconnect=5  # Reconnect after 5 seconds
        )

    def _run_wikimedia_stream(self, duration_seconds):
        """Handle Wikimedia Server-Sent Events (SSE) stream"""
        logger.info("🌐 Connecting to Wikimedia EventStream...")
        
        headers = {
            "Accept": "text/event-stream",
            "User-Agent": "KafkaStreamProducer/1.0 (Learning Project; Python)"
        }
        
        start_time = time.time()
        
        try:
            response = requests.get(
                self.url,
                stream=True,
                headers=headers,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"❌ Wikimedia returned status code {response.status_code}")
                return
            
            logger.info("✅ Connected to Wikimedia stream")
            client = sseclient.SSEClient(response)
            
            for event in client.events():
                # Check duration
                if time.time() - start_time > duration_seconds:
                    logger.info(f"⏱️ Duration limit reached ({duration_seconds}s)")
                    break
                
                # Skip empty events
                if not event.data or not event.data.strip():
                    continue
                
                try:
                    data = json.loads(event.data)
                    self.producer.send(self.topic, data)
                    self.msg_count += 1
                    
                    # Flush every 50 messages or every 5 seconds
                    current_time = time.time()
                    if (self.msg_count % 50 == 0 or 
                        current_time - self.last_flush_time > 5):
                        self.producer.flush()
                        self.last_flush_time = current_time
                    
                    # Log every 50 messages
                    if self.msg_count % 50 == 0:
                        logger.info(f"📊 Processed {self.msg_count} Wikimedia events")
                        
                except json.JSONDecodeError:
                    logger.debug("⚠️ Skipping invalid JSON event")
                    continue
                except Exception as e:
                    logger.error(f"❌ Error processing Wikimedia event: {e}")
                    
        except requests.exceptions.Timeout:
            logger.error("❌ Connection to Wikimedia timed out")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error connecting to Wikimedia: {e}")
        except Exception as e:
            logger.exception(f"❌ Unexpected error in Wikimedia stream: {e}")

    def start(self, duration_seconds):
        """Start the chosen stream"""
        logger.info(f"🚀 Starting {self.source} stream for {duration_seconds}s...")
        self.running = True
        self.msg_count = 0
        self.error_count = 0

        try:
            if self.source == "binance":
                # Run WebSocket in background thread
                ws_thread = threading.Thread(
                    target=self._run_binance_stream,
                    daemon=True
                )
                ws_thread.start()
                
                # Wait for duration
                time.sleep(duration_seconds)
                logger.info(f"⏱️ Duration limit reached ({duration_seconds}s)")
                
            elif self.source == "wikimedia":
                self._run_wikimedia_stream(duration_seconds)
                
        except Exception as e:
            logger.exception(f"❌ Error in stream: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the stream and cleanup"""
        if not self.running:
            return
            
        logger.info("🧹 Stopping stream and cleaning up...")
        self.running = False
        
        # Final flush
        try:
            self.producer.flush()
            logger.info(f"📊 Final count: {self.msg_count} messages processed")
            if self.error_count > 0:
                logger.warning(f"⚠️ Encountered {self.error_count} errors")
        except Exception as e:
            logger.warning(f"⚠️ Error during flush: {e}")
        
        # Close producer
        try:
            self.producer.close()
        except Exception as e:
            logger.warning(f"⚠️ Error closing producer: {e}")
        
        # Close WebSocket if exists
        if self.ws:
            try:
                self.ws.close()
                logger.info("✅ WebSocket closed")
            except Exception as e:
                logger.warning(f"⚠️ Error closing WebSocket: {e}")
        
        logger.info("✅ Cleanup complete")
