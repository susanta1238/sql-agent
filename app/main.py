from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any

from app.agent.core import run_agent_interaction

app = FastAPI(
    title="Leadnova Assistant API",
    description="Backend service for the AI Marketing Agent."
)

class ChatRequest(BaseModel):
    message: str
    # The history will be a list of dictionaries, e.g. [{"role": "user", "content": "Hi"}]
    history: List[Dict[str, Any]] = Field(default_factory=list)

@app.post("/chat/stream")
async def chat_endpoint(request: ChatRequest):
    """
    The API endpoint that streams the response back from the agent.
    """
    return StreamingResponse(
        run_agent_interaction(request.message, request.history),
        media_type="text/event-stream"
    )

@app.get("/")
def read_root():
    return {"status": "Leadnova Assistant API is running"}