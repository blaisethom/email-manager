from __future__ import annotations

import json
import sqlite3
import subprocess

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

from email_manager.ai.base import LLMBackend
from email_manager.agent.context import ConversationContext
from email_manager.agent.tools import TOOL_DEFINITIONS, execute_tool
from email_manager.db import fetchone


SYSTEM_PROMPT = """You are an email analysis assistant. You help the user understand their email history, manage projects, explore contacts, and refine how their emails are organised.

You have access to a database of the user's emails that have been synced and categorised. Use the available tools to answer questions and make changes.

Guidelines:
- Use tools to look up data before answering — don't guess.
- When the user asks about projects, contacts, or emails, query the data first.
- You can merge, rename, and reorganise projects based on the user's preferences.
- When discussing the data model (projects, departments, workstreams), help the user think through a structure that makes sense for their work.
- Be concise but thorough. Show relevant data to support your answers.
- If the user wants to change how things are categorised, use the available tools to make those changes.

Database schema (key tables):
- emails: id, message_id, thread_id, subject, from_address, from_name, to_addresses, cc_addresses, date, body_text, folder
- contacts: email, name, company, email_count, sent_count, received_count, first_seen, last_seen
- projects: name, description, department, workstream
- email_projects: email_id, project_id, confidence, assigned_by
- threads: thread_id, subject, email_count, first_date, last_date, summary
- entities: email_id, entity_type, value, context, confidence"""


def run_repl(conn: sqlite3.Connection, backend: LLMBackend, console: Console) -> None:
    """Run the interactive agent REPL using Claude API with tool use."""
    from email_manager.ai.claude_backend import ClaudeBackend
    from email_manager.ai.claude_cli_backend import ClaudeCLIBackend

    if isinstance(backend, ClaudeBackend):
        _run_claude_api_repl(conn, backend, console)
    elif isinstance(backend, ClaudeCLIBackend):
        _run_claude_cli_repl(conn, backend, console)
    else:
        _run_generic_repl(conn, backend, console)


def _run_claude_api_repl(conn: sqlite3.Connection, backend, console: Console) -> None:
    """REPL using Claude API with native tool use."""
    import anthropic

    client = backend._client
    model = backend._model
    ctx = ConversationContext()

    # Get quick stats for context
    stats = fetchone(conn, "SELECT COUNT(*) as cnt FROM emails")
    projects = fetchone(conn, "SELECT COUNT(*) as cnt FROM projects")

    console.print(Panel(
        f"[bold]Email Agent[/bold] — {stats['cnt']} emails, {projects['cnt']} projects\n"
        f"Model: {model}\n"
        "Type your questions or commands. Type 'quit' to exit.",
        title="Chat",
    ))

    while True:
        try:
            user_input = console.input("\n[bold blue]You:[/bold blue] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nGoodbye!")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            console.print("Goodbye!")
            break

        ctx.add_user(user_input)

        # Run the agent loop (tool use may require multiple turns)
        while True:
            try:
                response = client.messages.create(
                    model=model,
                    max_tokens=4096,
                    system=SYSTEM_PROMPT,
                    tools=TOOL_DEFINITIONS,
                    messages=ctx.messages,
                )
            except Exception as e:
                console.print(f"[red]API error: {e}[/red]")
                break

            # Process response content blocks
            assistant_content = response.content
            ctx.add_assistant(assistant_content)

            has_tool_use = False
            text_parts = []

            for block in assistant_content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    has_tool_use = True
                    tool_name = block.name
                    tool_args = block.input
                    tool_id = block.id

                    console.print(f"  [dim]Using {tool_name}...[/dim]")
                    result = execute_tool(conn, tool_name, tool_args)
                    ctx.add_tool_result(tool_id, result)

            if text_parts:
                console.print()
                for text in text_parts:
                    console.print(Markdown(text))

            if not has_tool_use:
                break  # No more tool calls, done with this turn


def _run_claude_cli_repl(conn: sqlite3.Connection, backend, console: Console) -> None:
    """REPL using Claude CLI with tool use via subprocess conversation."""
    ctx = ConversationContext()

    stats = fetchone(conn, "SELECT COUNT(*) as cnt FROM emails")
    projects = fetchone(conn, "SELECT COUNT(*) as cnt FROM projects")

    console.print(Panel(
        f"[bold]Email Agent[/bold] — {stats['cnt']} emails, {projects['cnt']} projects\n"
        f"Model: {backend.model_name} (via Claude CLI)\n"
        "Type your questions or commands. Type 'quit' to exit.",
        title="Chat",
    ))

    # Build tool descriptions for the system prompt
    tools_desc = "\n\nAvailable tools:\n"
    for t in TOOL_DEFINITIONS:
        props = t["input_schema"].get("properties", {})
        params = ", ".join(f"{k}: {v.get('type', 'string')}" for k, v in props.items())
        tools_desc += f"\n- {t['name']}({params}): {t['description']}"

    tools_desc += """

To use a tool, output a JSON block:
```tool
{"tool": "tool_name", "args": {"param": "value"}}
```

You can use multiple tools. After each tool call, I'll provide the result. Then continue your response."""

    system = SYSTEM_PROMPT + tools_desc

    while True:
        try:
            user_input = console.input("\n[bold blue]You:[/bold blue] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nGoodbye!")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            console.print("Goodbye!")
            break

        # Multi-turn loop for tool use
        current_input = user_input
        max_turns = 5

        for _turn in range(max_turns):
            raw = backend._run_claude(system, current_input)

            # Check for tool calls
            tool_results = []
            text_parts = []
            in_tool = False
            tool_block = []

            for line in raw.split("\n"):
                if line.strip() == "```tool":
                    in_tool = True
                    tool_block = []
                elif line.strip() == "```" and in_tool:
                    in_tool = False
                    try:
                        call = json.loads("\n".join(tool_block))
                        tool_name = call.get("tool", "")
                        tool_args = call.get("args", {})
                        console.print(f"  [dim]Using {tool_name}...[/dim]")
                        result = execute_tool(conn, tool_name, tool_args)
                        tool_results.append(f"Tool '{tool_name}' result:\n{result}")
                    except (json.JSONDecodeError, KeyError) as e:
                        tool_results.append(f"Tool call error: {e}")
                elif in_tool:
                    tool_block.append(line)
                else:
                    text_parts.append(line)

            text_output = "\n".join(text_parts).strip()
            if text_output:
                console.print()
                console.print(Markdown(text_output))

            if not tool_results:
                break  # No tool calls, done

            # Feed tool results back
            current_input = "Tool results:\n" + "\n\n".join(tool_results) + "\n\nContinue your response based on these results."


def _run_generic_repl(conn: sqlite3.Connection, backend: LLMBackend, console: Console) -> None:
    """Simple REPL for non-Claude backends (Ollama etc.) using ReAct-style prompting."""
    stats = fetchone(conn, "SELECT COUNT(*) as cnt FROM emails")
    projects = fetchone(conn, "SELECT COUNT(*) as cnt FROM projects")

    console.print(Panel(
        f"[bold]Email Agent[/bold] — {stats['cnt']} emails, {projects['cnt']} projects\n"
        f"Model: {backend.model_name}\n"
        "Type your questions or commands. Type 'quit' to exit.",
        title="Chat",
    ))

    tools_desc = "\n\nAvailable tools:\n"
    for t in TOOL_DEFINITIONS:
        props = t["input_schema"].get("properties", {})
        params = ", ".join(f"{k}: {v.get('type', 'string')}" for k, v in props.items())
        tools_desc += f"\n- {t['name']}({params}): {t['description']}"

    tools_desc += """

To use a tool, output exactly:
TOOL: tool_name
ARGS: {"param": "value"}

After receiving the result, continue your answer. When done, just output your final answer without any TOOL: prefix."""

    system = SYSTEM_PROMPT + tools_desc

    while True:
        try:
            user_input = console.input("\n[bold blue]You:[/bold blue] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nGoodbye!")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            console.print("Goodbye!")
            break

        current_input = user_input
        max_turns = 5

        for _turn in range(max_turns):
            raw = backend.complete(system, current_input)

            # Parse tool calls (TOOL: / ARGS: pattern)
            lines = raw.split("\n")
            text_parts = []
            tool_name = None
            tool_args_str = None
            tool_results = []

            for line in lines:
                if line.startswith("TOOL:"):
                    tool_name = line[5:].strip()
                elif line.startswith("ARGS:"):
                    tool_args_str = line[5:].strip()
                    if tool_name and tool_args_str:
                        try:
                            args = json.loads(tool_args_str)
                        except json.JSONDecodeError:
                            args = {}
                        console.print(f"  [dim]Using {tool_name}...[/dim]")
                        result = execute_tool(conn, tool_name, args)
                        tool_results.append(f"Result of {tool_name}:\n{result}")
                        tool_name = None
                        tool_args_str = None
                else:
                    text_parts.append(line)

            text_output = "\n".join(text_parts).strip()
            if text_output:
                console.print()
                console.print(Markdown(text_output))

            if not tool_results:
                break

            current_input = "Tool results:\n" + "\n\n".join(tool_results) + "\n\nContinue your response."
