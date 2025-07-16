# app/agent/core.py

import json
import logging
from openai import OpenAI
from app.services.database import db_connector

# Initialize the OpenAI client
client = OpenAI()

# Initialize a logger
logger = logging.getLogger(__name__)

# --- Tool Definition ---
def run_sql_query(query: str):
    """
    A tool that runs a read-only SQL query against the database.
    The AI will generate the SQL query string itself.
    """
    if not query.strip().upper().startswith("SELECT"):
        logger.warning(f"Blocked non-SELECT query: {query}")
        return {"error": "For security, I can only run SELECT queries."}
    return db_connector.execute_query(query)

# --- Agent Core Logic ---
available_tools = { "run_sql_query": run_sql_query }

# --- NEW: Raw DDL for the "Vanna" approach ---
# Providing the raw CREATE TABLE statement is often more effective for the LLM.
db_schema_ddl = """
CREATE TABLE [dbo].[ProfileData](
	[ProfileId] [bigint] IDENTITY(1,1) NOT NULL,
	[created_by] [varchar](100) NULL,
	[created_on] [datetime] NULL,
	[data_source] [varchar](100) NULL,
	[person_profile_headline] [varchar](8000) NULL,
	[job_last_updated] [nvarchar](255) NULL,
	[job_start_date] [nvarchar](255) NULL,
	[job_summary] [nvarchar](max) NULL,
	[job_title] [varchar](255) NULL,
	[job_title_role] [varchar](255) NULL,
	[job_title_sub_role] [varchar](255) NULL,
	[job_title_levels] [varchar](255) NULL,
	[organization_domain] [varchar](255) NULL,
	[organization_email] [varchar](255) NULL,
	[organization_email_status] [varchar](50) NULL,
	[organization_email_validation_source] [varchar](100) NULL,
	[organization_email_validation_date] [date] NULL,
	[organization_email_last_opened_date] [datetime] NULL,
	[organization_email_last_clicked_date] [datetime] NULL,
	[organization_facebook_url] [varchar](255) NULL,
	[organization_founded_year] [varchar](255) NULL,
	[organization_location_address_line_2] [varchar](255) NULL,
	[organization_location_city] [varchar](255) NULL,
	[organization_location_city_state_country] [varchar](255) NULL,
	[organization_location_continent] [varchar](255) NULL,
	[organization_location_country] [varchar](255) NULL,
	[organization_location_geo_code] [varchar](255) NULL,
	[organization_location_postal_code] [varchar](255) NULL,
	[organization_location_region] [varchar](255) NULL,
	[organization_location_state_country] [varchar](255) NULL,
	[organization_location_state_address] [varchar](255) NULL,
	[organization_industries] [varchar](1000) NULL,
	[organization_linkedin_id] [varchar](255) NULL,
	[organization_linkedin_url] [varchar](255) NULL,
	[organization_name] [varchar](255) NULL,
	[organization_phone] [varchar](255) NULL,
	[organization_phone_status] [varchar](50) NULL,
	[organization_phone_validation_source] [varchar](100) NULL,
	[organization_phone_validation_date] [datetime] NULL,
	[organization_phone_last_communicated_date] [datetime] NULL,
	[organization_size] [varchar](255) NULL,
	[organization_twitter_url] [varchar](255) NULL,
	[person_birth_date] [varchar](255) NULL,
	[person_birth_year] [varchar](255) NULL,
	[person_email] [varchar](500) NULL,
	[person_email_status] [varchar](50) NULL,
	[person_email_validation_source] [varchar](100) NULL,
	[person_email_validation_date] [datetime] NULL,
	[person_email_last_opened_date] [datetime] NULL,
	[person_email_last_clicked_date] [datetime] NULL,
	[person_facebook_id] [varchar](255) NULL,
	[person_facebook_url] [varchar](255) NULL,
	[person_facebook_url_status] [varchar](50) NULL,
	[person_facebook_url_validation_source] [varchar](100) NULL,
	[person_facebook_url_validation_date] [datetime] NULL,
	[person_facebook_username] [varchar](255) NULL,
	[person_first_name] [varchar](255) NULL,
	[person_gender] [varchar](255) NULL,
	[person_github_url] [varchar](255) NULL,
	[person_github_username] [varchar](255) NULL,
	[person_industries] [varchar](1000) NULL,
	[person_inferred_salary] [varchar](255) NULL,
	[person_inferred_years_experience] [varchar](255) NULL,
	[person_interest] [varchar](1000) NULL,
	[person_last_name] [varchar](255) NULL,
	[person_linkedin_connections] [varchar](255) NULL,
	[person_linkedin_id] [varchar](255) NULL,
	[person_linkedin_url] [varchar](255) NULL,
	[person_linkedin_url_status] [varchar](50) NULL,
	[person_linkedin_url_validation_source] [varchar](100) NULL,
	[person_linkedin_url_validation_date] [datetime] NULL,
	[person_linkedin_url_last_communicated_date] [datetime] NULL,
	[person_linkedin_username] [varchar](255) NULL,
	[person_location_address_line_2] [varchar](255) NULL,
	[person_location_city] [varchar](255) NULL,
	[person_location_city_status_country] [varchar](255) NULL,
	[person_location_continent] [varchar](255) NULL,
	[person_location_country] [varchar](255) NULL,
	[person_location_geo_code] [varchar](255) NULL,
	[person_location_last_updated] [varchar](255) NULL,
	[person_location_state_country] [varchar](255) NULL,
	[person_location_postal_code] [varchar](255) NULL,
	[person_location_state] [varchar](255) NULL,
	[person_location_street_address] [varchar](255) NULL,
	[person_middle_initial] [varchar](255) NULL,
	[person_middle_name] [varchar](255) NULL,
	[person_mobile] [varchar](255) NULL,
	[person_mobile_status] [varchar](50) NULL,
	[person_mobile_validation_source] [varchar](100) NULL,
	[person_mobile_validation_date] [datetime] NULL,
	[person_mobile_last_communicated_date] [datetime] NULL,
	[person_full_name] [varchar](255) NULL,
	[person_phone] [varchar](255) NULL,
	[person_phone_status] [varchar](50) NULL,
	[person_phone_validation_source] [varchar](100) NULL,
	[person_phone_validation_date] [datetime] NULL,
	[person_phone_last_communicated_date] [datetime] NULL,
	[person_raw_number] [varchar](255) NULL,
	[person_skills] [nvarchar](1000) NULL,
	[person_twitter_url] [varchar](255) NULL,
	[person_twitter_url_status] [varchar](50) NULL,
	[person_twitter_url_validation_source] [varchar](100) NULL,
	[person_twitter_url_validation_date] [datetime] NULL,
	[person_twitter_username] [varchar](255) NULL,
	[search_tags] [varchar](500) NULL,
	[updated_by] [varchar](100) NULL,
	[updated_on] [datetime] NULL,
	[person_photo_url] [nvarchar](1000) NULL,
	[organization_phone_sanitized] [varchar](255) NULL,
	[ConfidenceScore] [int] NULL,
	[PersonEmailScore] [int] NULL,
	[OrganisationEmailScore] [int] NULL,
	[PersonPhoneScore] [int] NULL,
	[PersonMobileScore] [int] NULL,
	[OrganisationPhoneScore] [int] NULL,
	[PersonLinkedInScore] [int] NULL,
	[DataBatch] [varchar](100) NULL,
	[person_email_opened] [bit] NULL,
	[person_email_clicked] [bit] NULL,
	[IsUnsubscribed] [bit] NULL,
	[Unsubscribed_on] [datetime] NULL,
	[person_twitter_followers] [int] NULL,
	[person_twitter_createdon] [date] NULL,
	[organization_email_clicked] [bit] NULL,
	[organization_email_opened] [bit] NULL,
);
"""
# Note: I have curated the DDL to include only the most relevant columns to keep the prompt size manageable.
# You can add more columns here if they become important for queries.

def run_agent_interaction(message: str, history: list):
    """
    The main agent loop.
    """
    
    # --- FIX 1: Robust History Formatting ---
    def format_history(raw_history: list) -> list:
        formatted = []
        for item in raw_history:
            if isinstance(item, dict) and 'role' in item and 'content' in item:
                formatted.append(item)
            else:
                logger.warning(f"Skipping malformed history item: {item}")
        return formatted

    clean_history = format_history(history)
    
    # --- FIX 2: Overhauled System Prompt ---
    # This new prompt is more directive and handles all your requirements.
    system_prompt = f"""
    You are Leadnova Assistant.
    
    **Your Identity & Persona:**
    - When asked "who are you?", respond concisely: "I am Leadnova Assistant, your AI marketing specialist. I can help you find validated leads for your business."
    - You are helpful, professional, and an expert in T-SQL.
    - NEVER mention that you are using a database or running queries. Frame your actions as "searching our network" or "finding contacts".
    - When presenting data, state confidently that "Our data is continuously updated and validated by our team to ensure quality." DO NOT reveal the source.

    **Core Task: SQL Query Generation**
    1.  Your primary function is to convert user requests into precise T-SQL (SQL Server) queries to run against the `ProfileData` table.
    2.  You MUST use the `run_sql_query` tool to get data.
    3.  **IMPORTANT**: Analyze the ENTIRE conversation history to understand the full context. If a user first asks for "CEOs" and then says "only with a valid email and phone", you must COMBINE these criteria into a new, single query. Do not forget previous constraints.
    4.  **Data Quality Filters (Apply these by default unless told otherwise):**
        - `(IsUnsubscribed = 0 OR IsUnsubscribed IS NULL)`
        - `ConfidenceScore > 50`
        - For emails, add `organization_email_status = 'valid'`.
    5.  For user requests like "with email and phone", interpret this as `organization_email IS NOT NULL` and `person_mobile IS NOT NULL`.
    6.  Default to `TOP 10` records unless the user specifies a different number.
    7.  Pay close attention to the `CREATE TABLE` schema provided below to understand available columns and their data types.

    **Output Formatting:**
    - If data is found, present it FIRST in a Markdown table. Then, you may add a brief suggestion.
    - If no data is found, do not just say "no results". Suggest a less restrictive search. Example: "I couldn't find contacts for that specific request. Would you like to try searching without the location filter?"

    **Database Schema (DDL):**
    ```sql
    {db_schema_ddl}
    ```
    """
    
    messages = [{"role": "system", "content": system_prompt}] + clean_history + [{"role": "user", "content": message}]
    
    try:
        # This is now a single, unified call. The AI will decide if a tool is needed.
        # If it is, it will respond with tool_calls. If not, it will respond with content.
        # This is more robust for follow-up conversations.
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=[{"type": "function", "function": available_tools["run_sql_query"].__dict__}], # Simplified tool definition
            tool_choice="auto",
        )
        
        response_message = response.choices[0].message
        
        # --- FIX 3: Robust Follow-up and Tool-Use Logic ---
        # This loop handles cases where the AI might ask clarifying questions OR call a tool.
        while response_message.tool_calls:
            tool_call = response_message.tool_calls[0]
            function_name = tool_call.function.name
            
            if function_name not in available_tools:
                yield "Error: The AI tried to call a non-existent tool."
                return

            function_to_call = available_tools[function_name]
            try:
                function_args = json.loads(tool_call.function.arguments)
                tool_output = function_to_call(**function_args)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON arguments from AI: {tool_call.function.arguments}")
                yield "Error: I received malformed arguments from the AI."
                return

            # Append the tool call and its output to the message history
            messages.append(response_message)
            messages.append({
                "tool_call_id": tool_call.id,
                "role": "tool",
                "name": function_name,
                "content": json.dumps(tool_output),
            })
            
            # Make a subsequent call to the model with the tool's output
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                stream=True,
            )
            
            # Stream the final response
            for chunk in response:
                content = chunk.choices[0].delta.content
                if content:
                    yield content
            return # Exit after streaming the final response

        # If the loop was never entered, it means no tool was called. Stream the direct response.
        content = response_message.content
        yield content

    except Exception as e:
        logger.error(f"An error occurred in the agent interaction: {e}", exc_info=True)
        logger.error(f"Failing payload messages: {messages}")
        yield "I'm sorry, I encountered an unexpected error. Please try again."

# Simplified function definition for tool use
run_sql_query.__dict__ = {
    "name": "run_sql_query",
    "description": "Executes a read-only T-SQL query on the database to fetch data.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "The T-SQL SELECT query to execute."},
        },
        "required": ["query"],
    },
}