import uuid
from fastapi import FastAPI, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from app.agent.core import run_agent_interaction

app = FastAPI(
    title="Leadnova Assistant API",
    description="Backend service for the AI Marketing Agent with Memory."
)

class ChatRequest(BaseModel):
    message: str

@app.post("/chat/stream")
async def chat_endpoint(
    request: ChatRequest,
    # Use headers to get user and session IDs.
    # If a session_id is not provided, we create a new one.
    x_session_id: Optional[str] = Header(None, alias="X-Session-ID"),
    x_user_id: Optional[str] = Header("default-user", alias="X-User-ID") # Default user for testing
):
    """
    The API endpoint that streams the response from the agent.
    It now uses session and user IDs to manage memory.
    """
    # Generate a new session ID if one isn't provided by the client
    session_id = x_session_id or str(uuid.uuid4())
    
    async def stream_wrapper():
        # The agent interaction now returns the full response as a single string
        # so we can log it after it's complete.
        full_response = ""
        async for chunk in run_agent_interaction(request.message, session_id, x_user_id):
            full_response += chunk
            yield chunk
        # After streaming is complete, the full_response is available for logging if needed
        # (Though we'll log inside the agent core for more context)

    return StreamingResponse(
        stream_wrapper(),
        media_type="text/event-stream",
        headers={"X-Session-ID": session_id} # Return the session ID to the client
    )

@app.get("/")
def read_root():
    return {"status": "Leadnova Assistant API is running"}