# app/agent/core.py

import json
import logging
from openai import OpenAI
from app.services.memory import get_short_term_memory, update_short_term_memory, log_significant_action
from app.services.database import db_connector

client = OpenAI()
logger = logging.getLogger(__name__)

def run_sql_query(query: str):
    """A tool that runs a read-only SQL query against the database."""
    if not query.strip().upper().startswith("SELECT"):
        logger.warning(f"Blocked non-SELECT query: {query}")
        return {"error": "For security reasons, I can only run SELECT queries."}
    return db_connector.execute_query(query)

available_tools = {"run_sql_query": run_sql_query}

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
	[organization_email_opened] [bit] NULL
);
""" # Your full DDL from before goes here. I'm omitting it for brevity.

async def run_agent_interaction(message: str, session_id: str, user_id: str):
    """The main agent loop, now with short-term and long-term memory."""
    history = get_short_term_memory(session_id)
    
    system_prompt = f"""
You are Leadnova Assistant, a powerful and fast data retrieval assistant. Your goal is to find contacts for the user with maximum speed and inclusiveness, and then allow them to refine the results.

**Core Philosophy: Find First, Filter Later**
Your primary directive is to find ANY and ALL records that match the user's core request (like job title and location). DO NOT apply any data quality filters by default. You will let the user add filters later.

**Core Dialogue & Query Rules:**
1.  **If the user provides a job title and location, act immediately.** Do not ask for more details to narrow it down.
2.  **If the user's request is very vague** (e.g., "find people"), you MUST ask for a job title or industry to get started.
3.  **Intelligent and Flexible Searching (VERY IMPORTANT):**
    - You MUST use the `LIKE` operator with wildcards (`%`) for all text searches (`job_title`, `organization_industries`, `person_location_country`).
    - Be smart about it. If the user asks for "developers", your query should be `job_title LIKE '%developer%'` to also match "software developer". If they ask for "sales", query `job_title_role LIKE '%sales%'`.
4.  **No Default Filtering:**
    - **DO NOT** filter by `ConfidenceScore` by default.
    - **DO NOT** filter by `IsUnsubscribed` by default.
    - **DO NOT** filter by `organization_email_status` by default.
    - Your job is to show what exists first.
5.  **Handle Follow-up Refinements:** After showing the initial list, the user might ask to filter it. You must then take the previous context and add the new filter.
    - *User:* "Find developers in california" -> *You run:* `SELECT ... WHERE job_title LIKE '%developer%' AND person_location_country LIKE 'california'`
    - *User:* "ok now only show the ones with a verified email" -> *You run a NEW query:* `SELECT ... WHERE job_title LIKE '%developer%' AND person_location_country LIKE 'california' AND organization_email_status = 'Verified'`
6.  **SQL Dialect:** You MUST use `TOP N` to limit results. The `LIMIT` keyword is INVALID. Default to `TOP 10` unless the user specifies otherwise.

**Persona & Communication Rules:**
- Be concise. Your job is to provide data quickly.
- When asked "who are you?", say: "I am Leadnova Assistant. I find business contacts for you."
- NEVER mention "SQL" or "database". Frame actions as "searching" or "finding".
- Do not make claims about data quality unless the user specifically filters for it.

**Output Formatting:**
- Present results in a Markdown table. For the initial search, show a few key columns: `person_full_name`, `job_title`, `organization_name`, `person_location_city`, `person_location_country`.
- After the table, simply ask: "Would you like to add more details or refine this list?"

**Database Schema for Query Generation (T-SQL):**
```sql
{db_schema_ddl}
    ```
    """
    
    messages = [{"role": "system", "content": system_prompt}] + history + [{"role": "user", "content": message}]
    full_agent_response = ""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=[{"type": "function", "function": run_sql_query.__dict__}],
            tool_choice="auto",
        )
        response_message = response.choices[0].message
        
        if response_message.tool_calls:
            tool_call = response_message.tool_calls[0]
            function_name = tool_call.function.name
            
            try:
                function_args = json.loads(tool_call.function.arguments)
                log_significant_action(
                    user_id=user_id, session_id=session_id, action_type=f"attempt_{function_name}",
                    user_query=message, generated_sql=function_args.get("query")
                )
                tool_output = available_tools[function_name](**function_args)
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Failed to execute tool call: {e}")
                yield "I had trouble understanding the tool's instructions. Please try rephrasing your request."
                return

            messages.append(response_message)
            messages.append({"tool_call_id": tool_call.id, "role": "tool", "name": function_name, "content": json.dumps(tool_output)})
            
            final_response_stream = client.chat.completions.create(model="gpt-4o", messages=messages, stream=True)
            
            # --- FIX APPLIED HERE ---
            for chunk in final_response_stream:
                content = chunk.choices[0].delta.content
                if content:
                    full_agent_response += content
                    yield content
            
            output_summary = f"Found {len(tool_output)} rows." if isinstance(tool_output, list) and 'error' not in tool_output else str(tool_output)
            log_significant_action(
                user_id=user_id, session_id=session_id, action_type=f"success_{function_name}",
                user_query=message, generated_sql=function_args.get("query"),
                tool_output_summary=output_summary, agent_response=full_agent_response
            )
        else:
            full_agent_response = response_message.content or ""
            yield full_agent_response
        
        if full_agent_response:
            update_short_term_memory(session_id, message, full_agent_response)

    except Exception as e:
        logger.error(f"An error occurred in the agent interaction: {e}", exc_info=True)
        error_message = "I'm sorry, I encountered an unexpected error. My technical team has been notified."
        update_short_term_memory(session_id, message, error_message)
        yield error_message

run_sql_query.__dict__ = {
    "name": "run_sql_query",
    "description": "Executes a read-only T-SQL query on the database to fetch data for the user.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "A complete and valid T-SQL SELECT query."},
        },
        "required": ["query"],
    },
}