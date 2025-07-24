# app/agent/core.py
import json
import logging
from openai import OpenAI
from app.services.memory import get_short_term_memory, update_short_term_memory, log_significant_action
from app.services.database import db_connector

# Initialize clients and logger
client = OpenAI()
logger = logging.getLogger(__name__)


# --- THE FINAL, DYNAMIC QUERY BUILDER TOOL ---
def build_and_run_search_query(filters: list = None, columns_to_select: list = None):
    """
    Builds and executes a safe query from a dynamic list of filters and can select custom columns.
    This is the heart of the agent's data access, providing both flexibility and security.
    """
    # Define a default set of columns for the initial view if none are specified
    if not columns_to_select:
        columns_to_select = [
            "person_full_name", "job_title", "organization_name", 
            "person_location_country", "organization_email", "person_email"
        ]
    
    # Security Layer 1: Whitelist of all columns the AI is allowed to select or filter on.
    allowed_columns = [
        "ProfileId", "person_full_name", "job_title", "job_title_role", "organization_name",
        "organization_industries", "person_location_city", "person_location_state", "person_location_country",
        "organization_email", "person_email", "person_mobile", "person_phone", "person_linkedin_url",
        "person_twitter_url", "person_github_url", "person_skills", 
        "organization_email_status", "person_linkedin_connections" 
    ]
    
    # Filter the requested columns against the allowlist to ensure safety
    safe_columns = [col for col in columns_to_select if col in allowed_columns]
    if not safe_columns: # Fallback if AI provides no valid columns
        safe_columns = ["person_full_name", "job_title", "organization_name"]

    select_clause = ", ".join(f"[{col}]" for col in safe_columns) # Add brackets for safety
    query = f"SELECT TOP 10 {select_clause} FROM dbo.ProfileData"
    
    conditions, params = [], []
    allowed_operators = ["LIKE", "=", "IS NOT NULL", "IS NULL", ">", "<"]

    if filters:
        for f in filters:
            # Validate the filter object received from the AI
            if not isinstance(f, dict) or not all(k in f for k in ["column", "operator", "value"]):
                logger.warning(f"Skipping malformed filter from AI: {f}")
                continue

            column, operator, value = f["column"], f["operator"].upper(), f["value"]

            # Security Layer 2: Validate the column and operator against allowlists
            if column not in allowed_columns or operator not in allowed_operators:
                logger.warning(f"Skipping filter with disallowed column or operator: {f}")
                continue

            if operator in ["IS NOT NULL", "IS NULL"]:
                conditions.append(f"[{column}] {operator}")
            else:
                conditions.append(f"[{column}] {operator} ?")
                params.append(value if operator != "LIKE" else f"%{value}%")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY person_full_name ASC;"
    return db_connector.execute_query(query, tuple(params))


# --- FINAL TOOL SCHEMA ---
dynamic_query_schema = {
    "type": "function",
    "function": {
        "name": "run_dynamic_query",
        "description": "Searches for contacts using a list of filters and specifies which columns to return.",
        "parameters": {
            "type": "object",
            "properties": {
                "filters": {
                    "type": "array",
                    "description": "List of filter objects: {'column': str, 'operator': str, 'value': str}",
                    "items": { "type": "object", "properties": {
                        "column": {"type": "string"}, "operator": {"type": "string"}, "value": {"type": "string"}
                    }}
                },
                "columns_to_select": {
                    "type": "array",
                    "description": "List of exact column names to show in the result.",
                    "items": {"type": "string"}
                }
            }
        }
    }
}

available_tools = {"run_dynamic_query": build_and_run_search_query}

db_schema_ddl = """
CREATE TABLE [dbo].[ProfileData](
	[ProfileId] [bigint] IDENTITY(1,1) NOT NULL,
	[created_on] [datetime] NULL,
	[job_title] [varchar](255) NULL,
	[job_title_role] [varchar](255) NULL,
	[organization_name] [varchar](255) NULL,
	[organization_email] [varchar](255) NULL,
	[organization_email_status] [varchar](50) NULL,
	[organization_industries] [varchar](1000) NULL,
	[organization_linkedin_url] [varchar](255) NULL,
	[organization_size] [varchar](255) NULL,
	[person_email] [varchar](500) NULL,
	[person_full_name] [varchar](255) NULL,
	[person_github_url] [varchar](255) NULL,
	[person_linkedin_url] [varchar](255) NULL,
    [person_linkedin_connections] [int] NULL,
	[person_location_city] [varchar](255) NULL,
	[person_location_country] [varchar](255) NULL,
	[person_location_state] [varchar](255) NULL,
	[person_mobile] [varchar](255) NULL,
	[person_phone] [varchar](255) NULL,
	[person_skills] [nvarchar](1000) NULL,
	[person_twitter_url] [varchar](255) NULL
);
""" # NOTE: This is a curated, shorter version for prompt efficiency. You can use your full DDL.


# --- THE MAIN AGENT INTERACTION FUNCTION ---
async def run_agent_interaction(message: str, session_id: str, user_id: str) -> dict:
    history = get_short_term_memory(session_id)
    
    system_prompt = f"""
    You are Leadnova Assistant, an expert data analyst. Your job is to translate user requests into parameters for the `run_dynamic_query` tool. You operate in two modes: Broad Search and Narrow Refinement.
    never told user you are data analyst work as a data analyst but tell to find lead

    **MODE 1: BROAD SEARCH (for initial industry requests)**
    When the user first asks for a broad industry category, your most important task is to perform **semantic expansion**. Convert their broad request into a list of specific, related sub-industries to provide a comprehensive initial result.

    *Example of Broad Search:*
    *User:* "give me data for businesses in the 'Health & Wellness' industry in the usa"
    *Assistant's Action (Tool Call):*
    ```json
    {{
      "filters": [
        {{ "column": "person_location_country", "operator": "LIKE", "value": "united states" }}
      ],
      "organization_industries": [
        "hospital & health care", "mental health care", "pharmaceuticals", "health, wellness and fitness", "medical devices"
      ]
    }}
    ```

    **MODE 2: NARROW REFINEMENT (for follow-up requests)**
    After providing an initial list, any follow-up from the user should be treated **literally**. You MUST retain all previous filters and add the new, literal filter or specify new columns.

    *Example of Narrow Refinement:*
    *History:* The user just received a list of software engineers at Google.
    *User:* "great, now show me their phone number and linkedin profile"
    *Assistant's Action (Tool Call):*
    ```json
    {{
      "filters": [
        {{"column": "job_title", "operator": "LIKE", "value": "software engineer"}},
        {{"column": "organization_name", "operator": "LIKE", "value": "Google"}}
      ],
      "columns_to_select": [ "person_full_name", "job_title", "person_mobile", "person_linkedin_url" ]
    }}
    ```

    **CRITICAL RULES:**
    - Always analyze the full conversation history to determine your mode and retain context.
    - NEVER show your thought process or the JSON tool call to the user.
    - If the user's request is too vague, ask a clarifying question.

    **Database Schema for finding column names:**
    ```sql
    {db_schema_ddl}
    ```
    """
    
    messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": message}]
    
    try:
        response = client.chat.completions.create(model="gpt-4o", messages=messages, tools=[dynamic_query_schema], tool_choice="auto")
        response_message = response.choices[0].message
        
        final_response_obj = {}

        if response_message.tool_calls:
            tool_call = response_message.tool_calls[0]
            function_name = tool_call.function.name
            function_to_call = available_tools.get(function_name)
            
            if function_to_call:
                function_args = json.loads(tool_call.function.arguments)
                log_significant_action(user_id=user_id, session_id=session_id, action_type=f"attempt_{function_name}", user_query=message, generated_sql=str(function_args))
                tool_output = function_to_call(**function_args)

                if isinstance(tool_output, dict) and 'error' in tool_output:
                    final_response_obj = {"type": "error_response", "content": f"Database error: {tool_output['error']}"}
                elif not tool_output:
                    final_response_obj = {"type": "text_response", "content": "I couldn't find any contacts matching that refined search. Please try removing a filter or using different keywords."}
                else:
                    summary_text = f"I've updated the list and found {len(tool_output)} contacts. Here are the details:"
                    final_response_obj = {"type": "data_response", "content": {"summary": summary_text, "data": tool_output}}

                output_summary = f"Found {len(tool_output)} rows." if isinstance(tool_output, list) else str(tool_output)
                log_significant_action(user_id=user_id, session_id=session_id, action_type=f"success_{function_name}", user_query=message, generated_sql=str(function_args), tool_output_summary=output_summary, agent_response=json.dumps(final_response_obj))
            else:
                final_response_obj = {"type": "error_response", "content": "Internal error: AI tried an unknown tool."}
        else:
            full_agent_response = response_message.content or "I'm not sure how to respond to that."
            final_response_obj = {"type": "text_response", "content": full_agent_response}
        
        response_for_history = ""
        response_type = final_response_obj.get("type")
        if response_type == "data_response":
            response_for_history = final_response_obj.get("content", {}).get("summary", "I have found some data for you.")
        elif response_type in ["text_response", "error_response"]:
            response_for_history = final_response_obj.get("content", "An unspecified error occurred.")
        
        if response_for_history:
             update_short_term_memory(session_id, message, response_for_history)
        
        return final_response_obj

    except Exception as e:
        logger.error(f"An error occurred in agent interaction: {e}", exc_info=True)
        return {"type": "error_response", "content": "I'm sorry, an unexpected error occurred."}