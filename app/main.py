# app/main.py

import uuid
from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse # Import JSONResponse
from pydantic import BaseModel
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
import logging 
import logging.config 

from app.agent.core import run_agent_interaction

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "filename": "agent_activity.log", # The name of your log file
            "maxBytes": 10485760,  # 10 MB
            "backupCount": 5,
            "encoding": "utf8",
        },
    },
    "root": {
        "level": "INFO", # The minimum level of logs to capture
        "handlers": ["console", "file"], # Send logs to both console and file
    },
}

# Apply the logging configuration
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__) # Get a logger for this file

logger.info("Logging configured successfully. Logs will be written to console and file.")

app = FastAPI(
    title="Leadnova Assistant API",
    description="Backend service for the AI Marketing Agent with Memory."
)

# CORS Middleware (should already be here)
origins = ["*"] # For simplicity in testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Session-ID"]
)

class ChatRequest(BaseModel):
    message: str

# --- MODIFIED ENDPOINT ---
@app.post("/chat") # Renamed from /chat/stream
async def chat_endpoint(
    request: ChatRequest,
    x_session_id: Optional[str] = Header(None, alias="X-Session-ID"),
    x_user_id: Optional[str] = Header("default-user", alias="X-User-ID")
):
    """
    The API endpoint that now returns a structured JSON response from the agent.
    """
    session_id = x_session_id or str(uuid.uuid4())

    # The agent interaction now returns a complete dictionary.
    # No more async for loop needed here.
    response_data = await run_agent_interaction(request.message, session_id, x_user_id)
    
    # Create a JSON response and add the session ID to the headers
    response = JSONResponse(content=response_data)
    response.headers["X-Session-ID"] = session_id
    response.headers["Access-Control-Expose-Headers"] = "X-Session-ID" # Ensure frontend can read it
    return response

@app.get("/")
def read_root():
    return {"status": "Leadnova Assistant API is running"}