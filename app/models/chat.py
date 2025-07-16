from pydantic import BaseModel 
from typing import Optional 
from datetime import datetime 
 
class ChatRequest(BaseModel): 
    """Request model for chat endpoint.""" 
    message: str 
    user_id: Optional[str] = None 
    session_id: Optional[str] = None 
 
class ChatResponse(BaseModel): 
    """Response model for chat endpoint.""" 
    response: str 
    timestamp: datetime = datetime.now() 
    status: str = "success" 
