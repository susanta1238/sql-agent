import os
import redis
import json
import logging
from app.services.database import db_connector # Import our existing SQL connector

logger = logging.getLogger(__name__)

# --- Redis Connection for Short-Term Memory ---
try:
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=0,
        decode_responses=True # Important: decodes responses from bytes to strings
    )
    redis_client.ping() # Check connection
    logger.info("Successfully connected to Redis for short-term memory.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis: {e}. Short-term memory will not work.")
    redis_client = None

# --- Short-Term Memory Functions ---
def get_short_term_memory(session_id: str, k: int = 10) -> list:
    """Gets the last k messages from the current conversation session."""
    if not redis_client:
        return []
    
    memory_key = f"session_memory:{session_id}"
    history = redis_client.lrange(memory_key, 0, -1) # Get all items
    # The history from Redis needs to be parsed from JSON strings
    return [json.loads(item) for item in history][-k:] # Return the last k items

def update_short_term_memory(session_id: str, user_message: str, agent_response: str):
    """Updates the conversation history in Redis."""
    if not redis_client:
        return

    memory_key = f"session_memory:{session_id}"
    # Add user message
    redis_client.rpush(memory_key, json.dumps({"role": "user", "content": user_message}))
    # Add agent response
    redis_client.rpush(memory_key, json.dumps({"role": "assistant", "content": agent_response}))
    
    # Set an expiration time for the session memory (e.g., 24 hours)
    redis_client.expire(memory_key, 86400)

# --- Long-Term Memory Functions ---
def log_significant_action(
    user_id: str,
    session_id: str,
    action_type: str,
    user_query: str,
    generated_sql: str = None,
    tool_output_summary: str = None,
    agent_response: str = None
):
    """Logs a significant agent action to the SQL database for permanent memory."""
    sql = """
    INSERT INTO dbo.AgentActivityLog (UserID, SessionID, ActionType, UserQuery, GeneratedSQL, ToolOutputSummary, AgentResponse)
    VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    params = (
        user_id,
        session_id,
        action_type,
        user_query,
        generated_sql,
        tool_output_summary,
        agent_response
    )
    try:
        db_connector.execute_query(sql, params)
        logger.info(f"Logged significant action '{action_type}' for user '{user_id}'.")
    except Exception as e:
        logger.error(f"Failed to log significant action to SQL: {e}")