"""
title: Nextcloud R2R Search Pipe (Local Model)
author: GAGPT
version: 1.0
license: MIT
"""

import json
import requests
import uuid
import re
from typing import Optional, Tuple, List, Dict
from pydantic import BaseModel, Field
from fastapi import Request

# Import OpenWebUI internal functions
from open_webui.models.users import Users
from open_webui.utils.chat import generate_chat_completion

try:
    import ldap3

    LDAP_AVAILABLE = True
except ImportError:
    LDAP_AVAILABLE = False


def _truncate(s: str, n: int) -> str:
    """Truncate string to n characters, adding ellipsis if truncated."""
    s = s or ""
    return s if len(s) <= n else s[: n - 3] + "..."


def _get_citation_identifier(meta: dict) -> str:
    """Extract citation identifier from metadata using priority: title | source | filename."""
    if not isinstance(meta, dict):
        return "Unknown"

    for key in ("title", "source", "filename"):
        value = meta.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for key in ("file_name", "name", "document_id"):
        value = meta.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return "Unknown"


def _extract_nextcloud_file_id(metadata: dict) -> Optional[str]:
    """Extract Nextcloud file ID from metadata filename field."""
    if not isinstance(metadata, dict):
        return None

    filename = metadata.get("filename", "")
    if not filename:
        return None

    if "files__default:" in filename:
        try:
            file_id = filename.split("files__default:")[-1].strip()
            if file_id.isdigit():
                return file_id
        except:
            pass

    if filename.startswith("files_") and filename[6:].isdigit():
        return filename[6:]

    if filename.isdigit():
        return filename

    return None


def _ldap_lookup_user_guid(email: str, ldap_config: dict) -> Optional[str]:
    """Lookup user GUID from Active Directory via LDAP using email address."""
    if not LDAP_AVAILABLE:
        raise ImportError(
            "ldap3 library is required for AD authentication. Install with: pip install ldap3"
        )

    if not email or not email.strip():
        return None

    try:
        from ldap3 import Server, Connection, ALL, SUBTREE

        server = Server(ldap_config["server_uri"], get_info=ALL)
        conn = Connection(
            server,
            user=ldap_config["bind_user"],
            password=ldap_config["bind_password"],
            auto_bind=True,
            receive_timeout=ldap_config.get("timeout", 10),
        )

        if not conn.bound:
            return None

        search_filter = ldap_config["user_filter"].format(email=email)
        conn.search(
            search_base=ldap_config["search_base"],
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=[
                ldap_config["guid_attribute"],
                "mail",
                "userPrincipalName",
                "cn",
            ],
        )

        if not conn.entries:
            return None

        entry = conn.entries[0]
        guid_attr = ldap_config["guid_attribute"]

        if hasattr(entry, guid_attr):
            guid_value = getattr(entry, guid_attr).value

            if isinstance(guid_value, bytes):
                guid_uuid = uuid.UUID(bytes=guid_value)
                return str(guid_uuid).upper()
            elif isinstance(guid_value, str):
                guid_clean = guid_value.strip("{}").strip()
                try:
                    guid_uuid = uuid.UUID(guid_clean)
                    return str(guid_uuid).upper()
                except ValueError:
                    return None
            elif isinstance(guid_value, list) and len(guid_value) > 0:
                first_guid = guid_value[0]
                if isinstance(first_guid, str):
                    guid_clean = first_guid.strip("{}").strip()
                    try:
                        guid_uuid = uuid.UUID(guid_clean)
                        return str(guid_uuid).upper()
                    except ValueError:
                        return None
                elif isinstance(first_guid, bytes):
                    guid_uuid = uuid.UUID(bytes=first_guid)
                    return str(guid_uuid).upper()

        return None

    except Exception as e:
        print(f"LDAP lookup failed for {email}: {e}")
        return None
    finally:
        try:
            if "conn" in locals() and conn.bound:
                conn.unbind()
        except:
            pass


def _get_collection_id_from_guid(user_guid: str, r2r_config: dict) -> Optional[str]:
    """Get R2R collection ID from user GUID by calling collections API."""
    if not user_guid:
        return None

    try:
        url = f"{r2r_config['collections_api_url']}/{user_guid}"
        params = {"owner_id": r2r_config["default_owner_id"]}
        headers = {"Authorization": f"Bearer {r2r_config['bearer_token']}"}

        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("results", {}).get("id")
        return None
    except Exception as e:
        print(f"Collection lookup failed for GUID {user_guid}: {e}")
        return None


def _format_metadata_info(meta: dict) -> list:
    """Extract and format useful metadata information."""
    if not isinstance(meta, dict):
        return []

    info_parts = []

    for key in ["document_id", "document_type", "filename", "file_name"]:
        if key in meta and meta[key]:
            info_parts.append(f"{key}: {meta[key]}")

    for key in ["chunk_index", "page", "page_number", "section", "chapter"]:
        if key in meta and meta[key] is not None:
            info_parts.append(f"{key}: {meta[key]}")

    for key in ["created_at", "updated_at", "date", "year"]:
        if key in meta and meta[key]:
            info_parts.append(f"{key}: {meta[key]}")

    for key in ["size_in_bytes", "total_tokens"]:
        if key in meta and meta[key] is not None:
            info_parts.append(f"{key}: {meta[key]}")

    return info_parts


class Pipe:
    class Valves(BaseModel):
        # === Model Configuration ===
        LOCAL_MODEL: str = Field(
            default="ga3/qwen3:30b-a3b",
            description="Local model to use (e.g., qwen2.5:30b-instruct-q4_0, llama3.2:70b, etc.)",
        )

        # === R2R Connection / Auth ===
        api_url: str = Field(
            default="http://your-r2r-server:7272/v3/retrieval/search",
            description="R2R search endpoint URL (must be accessible from OpenWebUI server).",
        )
        nextcloud_base_url: str = Field(
            default="https://your-nextcloud.domain.com",
            description="Base Nextcloud URL to prefix filenames for creating links.",
        )
        bearer_token: str = Field(
            default="",
            description="Authorization Bearer token. Required for authenticated R2R instances.",
        )

        # === Active Directory LDAP Configuration ===
        ldap_server_uri: str = Field(
            default="ldap://your-ad-server.domain.com:389",
            description="Active Directory LDAP server URI (e.g., ldap://server:389 or ldaps://server:636).",
        )
        ldap_bind_user: str = Field(
            default="",
            description="LDAP bind username (e.g., service-account@domain.com).",
        )
        ldap_bind_password: str = Field(
            default="",
            description="LDAP bind password for the service account.",
        )
        ldap_search_base: str = Field(
            default="DC=domain,DC=com",
            description="LDAP search base DN (e.g., DC=company,DC=com).",
        )
        ldap_user_filter: str = Field(
            default="(mail={email})",
            description="LDAP filter to find user by email. Use {email} placeholder.",
        )
        ldap_guid_attribute: str = Field(
            default="objectGUID",
            description="LDAP attribute containing user GUID (usually 'objectGUID' for AD).",
        )

        # === R2R Collections API ===
        collections_api_url: str = Field(
            default="http://your-r2r-server:7272/v3/collections/name",
            description="R2R collections API base URL for collection lookups.",
        )
        default_owner_id: str = Field(
            default="00000000-0000-0000-0000-000000000000",
            description="Default owner ID for R2R collection lookups.",
        )

        # === Permission Settings ===
        enforce_permissions: bool = Field(
            default=True,
            description="Enable user permission filtering via AD GUID and R2R collections.",
        )
        ldap_timeout: int = Field(
            default=10,
            description="LDAP connection timeout in seconds.",
        )

        # === Search Configuration ===
        use_hybrid_search: bool = Field(
            default=True,
            description="Enable hybrid (semantic + keyword) search for better results.",
        )
        search_limit: int = Field(
            default=10,
            description="Maximum number of chunks to retrieve from R2R (1-100).",
            ge=1,
            le=100,
        )

        # === Response Filtering ===
        max_chunks_in_context: int = Field(
            default=8,
            description="Maximum chunks to include in LLM context (1-20).",
            ge=1,
            le=20,
        )
        max_chars_per_chunk: int = Field(
            default=1200,
            description="Truncate each chunk to this character limit.",
            ge=100,
            le=5000,
        )
        min_relevance_score: float = Field(
            default=0.0,
            description="Minimum relevance score to include chunk (0.0-1.0).",
            ge=0.0,
            le=1.0,
        )

        # === Technical Settings ===
        request_timeout: int = Field(
            default=30, description="HTTP request timeout in seconds.", ge=5, le=300
        )

        include_metadata: bool = Field(
            default=True,
            description="Include metadata information in context for better citations.",
        )

    def __init__(self):
        self.valves = self.Valves()

    def pipes(self) -> List[Dict[str, str]]:
        """Define the models available through this pipe."""
        return [
            {
                "id": "r2r-search",
                "name": f"NC R2R ({self.valves.LOCAL_MODEL})",
            }
        ]

    def _parse_user_input(self, input_text: str) -> Tuple[str, Optional[str]]:
        """Parse user input to extract search query and optional LLM instructions."""
        input_text = (input_text or "").strip()
        if not input_text:
            return "", None

        # Check for structured format
        if input_text.startswith("---") and "---" in input_text[3:]:
            try:
                yaml_end = input_text.find("---", 3)
                yaml_content = input_text[3:yaml_end].strip()

                search_query = None
                llm_instructions = None

                for line in yaml_content.split("\n"):
                    line = line.strip()
                    if line.startswith("search:"):
                        search_query = line[7:].strip().strip("\"'")
                    elif line.startswith("instructions:"):
                        llm_instructions = line[12:].strip().strip("\"'")

                if search_query:
                    return search_query, llm_instructions
            except Exception:
                pass

        # Check for simple delimiter format
        if " | " in input_text:
            parts = input_text.split(" | ", 1)
            search_query = parts[0].strip()
            llm_instructions = parts[1].strip() if len(parts) > 1 else None
            return search_query, llm_instructions

        # Default: entire input as search query
        return input_text, None

    def _perform_r2r_search(
        self, query: str, user_collection_id: Optional[str] = None
    ) -> Dict:
        """Execute the R2R search with permission filtering."""
        search_payload = {
            "query": query,
            "search_settings": {
                "use_hybrid_search": self.valves.use_hybrid_search,
                "limit": min(self.valves.search_limit, 100),
            },
        }

        # Add collection-based filtering if permissions are enforced
        if user_collection_id:
            search_payload["search_settings"]["filters"] = {
                "collection_ids": {"$in": [user_collection_id]}
            }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.valves.bearer_token.strip()}",
            "User-Agent": "OpenWebUI-R2R-Pipe/1.0",
        }

        try:
            response = requests.post(
                self.valves.api_url.rstrip("/"),
                headers=headers,
                json=search_payload,
                timeout=min(self.valves.request_timeout, 300),
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"R2R search failed: {str(e)}")

    def _get_user_collection_id(self, user_email: str) -> Optional[str]:
        """Get user's collection ID through LDAP and R2R APIs."""
        if not self.valves.enforce_permissions:
            return None

        # LDAP lookup
        ldap_config = {
            "server_uri": self.valves.ldap_server_uri,
            "bind_user": self.valves.ldap_bind_user,
            "bind_password": self.valves.ldap_bind_password,
            "search_base": self.valves.ldap_search_base,
            "user_filter": self.valves.ldap_user_filter,
            "guid_attribute": self.valves.ldap_guid_attribute,
            "timeout": self.valves.ldap_timeout,
        }

        user_guid = _ldap_lookup_user_guid(user_email, ldap_config)
        if not user_guid:
            return None

        # R2R collection lookup
        r2r_config = {
            "collections_api_url": self.valves.collections_api_url,
            "bearer_token": self.valves.bearer_token.strip(),
            "default_owner_id": self.valves.default_owner_id,
        }

        return _get_collection_id_from_guid(user_guid, r2r_config)

    def _build_context_from_results(
        self,
        search_query: str,
        chunks: List[Dict],
        custom_instructions: Optional[str] = None,
    ) -> str:
        """Build the context string from R2R search results."""
        # Filter and sort chunks
        filtered_chunks = [
            chunk
            for chunk in chunks
            if chunk.get("score", 0.0) >= self.valves.min_relevance_score
        ]

        if not filtered_chunks:
            return f"No relevant documents found for query: '{search_query}'"

        # Select top chunks for context
        top_chunks = filtered_chunks[: self.valves.max_chunks_in_context]

        # Build context parts
        context_parts = [
            "=== SEARCH RESULTS ===",
            f"Search Query: {search_query}",
            f"Found {len(filtered_chunks)} relevant results (showing top {len(top_chunks)}):",
            "",
        ]

        for idx, chunk in enumerate(top_chunks, 1):
            text = chunk.get("text", "").strip()
            metadata = chunk.get("metadata", {})
            score = chunk.get("score")
            document_id = chunk.get("document_id", "")

            citation_id = _get_citation_identifier(metadata)

            # Extract Nextcloud file ID and create link
            nextcloud_link = None
            nextcloud_file_id = _extract_nextcloud_file_id(metadata)

            if nextcloud_file_id:
                nextcloud_link = f"{self.valves.nextcloud_base_url.rstrip('/')}/f/{nextcloud_file_id}"

            # Truncate text for context
            text = _truncate(text, self.valves.max_chars_per_chunk)

            # Format chunk entry
            header = f"[{idx}] Citation ID: {citation_id}"
            if nextcloud_file_id:
                header += f" | Nextcloud File ID: {nextcloud_file_id}"
            if nextcloud_link:
                header += f" | Link: {nextcloud_link}"
            if score is not None:
                header += f" | Relevance: {score:.3f}"

            context_parts.append(header)

            # Include enhanced metadata for context
            if self.valves.include_metadata:
                meta_info = []
                filename = metadata.get("filename", "")
                if filename:
                    meta_info.append(f"filename: {filename}")

                other_meta = _format_metadata_info(metadata)
                if document_id:
                    other_meta.insert(0, f"document_id: {document_id}")
                meta_info.extend(other_meta[:5])

                if meta_info:
                    context_parts.append(f"Metadata: {', '.join(meta_info)}")

            context_parts.append("Content:")
            context_parts.append(text)
            context_parts.append("-" * 50)

        # Build system prompt
        base_system_prompt = (
            "## CRITICAL CITATION RULES - MUST FOLLOW EXACTLY:\n"
            "You MUST cite using the exact Citation ID shown for each result, NOT numbers.\n"
            "WRONG: [1], [2], [3], [4] - NEVER use numbers in citations\n"
            "CORRECT: Use the exact Citation ID after 'Citation ID:' in each result\n\n"
            "## HYPERLINK GENERATION:\n"
            "After each citation, provide a hyperlink to the Nextcloud document:\n"
            "1. Find the 'filename' field in the metadata (e.g., 'filename: files__default:8060008')\n"
            "2. Extract the number after 'files__default:' (e.g., '8060008')\n"
            f"3. Create link as: [{self.valves.nextcloud_base_url.rstrip('/')}/f/NUMBER]\n"
            f"Example: If metadata shows 'filename: files__default:8060008', create link [{self.valves.nextcloud_base_url.rstrip('/')}/f/8060008]\n\n"
            "## CITATION FORMAT:\n"
            f"Each citation should be: [Citation_ID][{self.valves.nextcloud_base_url.rstrip('/')}/f/FILE_ID]\n"
            f"Example: [Document Title][{self.valves.nextcloud_base_url.rstrip('/')}/f/8060008]\n\n"
            "## Task: Answer the query strictly using the provided Context. If the Context does not support an answer, say you don't know.\n"
            "## Grounding: Use only the provided Context. If the Context does not support an answer, reply exactly: I don't know.\n"
            "## Citations: After every sentence that uses the Context, append citations with hyperlinks as shown above.\n"
            "## Formatting: Begin directly with the answer text. No preface, no 'Response:', no lists unless the query demands it. Keep prose concise and factual."
        )

        if custom_instructions:
            enhanced_prompt = f"{base_system_prompt}\n\n## ADDITIONAL USER INSTRUCTIONS:\n{custom_instructions}\n"
        else:
            enhanced_prompt = base_system_prompt

        return "\n".join(
            [
                "CRITICAL: You must use the exact Citation ID from each result, NOT numbers like [1], [2], [3].",
                "Look at each result and find the line that says 'Citation ID: ...' - use that exact text in brackets.",
                "",
                enhanced_prompt,
                "",
                f"Context: {chr(10).join(context_parts)}",
                "",
                "REMINDER: Use [Citation_ID] format with the exact Citation ID from each result. Never use numbers.",
                "Instructions: Follow the rules above strictly. Output only the answer text with inline citations using Citation IDs.",
            ]
        )

    async def pipe(self, body: dict, __user__: dict, __request__: Request):
        """Main pipe function that handles R2R search and routes to local model."""
        try:
            # Extract the user's query
            messages = body.get("messages", [])
            if not messages:
                # Return simple error message for non-streaming
                return "Error: No messages provided"

            last_message = messages[-1]
            if last_message.get("role") != "user":
                return "Error: Last message must be from user"

            user_query = last_message.get("content", "").strip()
            if not user_query:
                return "Error: Empty user query"

            # Parse search query and custom instructions
            search_query, custom_instructions = self._parse_user_input(user_query)

            if not search_query or len(search_query.strip()) < 3:
                return f"Error: Search query too short: '{search_query}'"

            # Validate authentication
            if not self.valves.bearer_token.strip():
                return "Error: Missing R2R authentication token. Please configure the 'bearer_token' in pipe settings."

            # Handle user permissions
            user_collection_id = None
            if self.valves.enforce_permissions:
                user_email = __user__.get("email") if __user__ else None

                if not user_email or "@" not in user_email:
                    return (
                        "❌ **Access Denied**\n\n"
                        "Unable to identify user for permission filtering. This search requires valid "
                        "user authentication to ensure you only see documents you have access to."
                    )

                try:
                    user_collection_id = self._get_user_collection_id(user_email)
                    if not user_collection_id:
                        return (
                            "❌ **Access Denied**\n\n"
                            f"No document collection found for user: {user_email}\n"
                            "Please contact your system administrator to request access."
                        )
                except Exception as e:
                    return (
                        "❌ **Permission Check Failed**\n\n"
                        "Unable to verify your document access permissions due to a system error. "
                        "Please try again later or contact your system administrator."
                    )

            # Execute R2R search
            try:
                response_data = self._perform_r2r_search(
                    search_query, user_collection_id
                )
            except Exception as e:
                return f"Error: {str(e)}"

            # Process results
            results = response_data.get("results", {})
            if isinstance(results, dict):
                chunks = results.get("chunk_search_results", [])
            else:
                chunks = results if isinstance(results, list) else []

            if not chunks:
                return (
                    f"No relevant documents found for query: '{search_query}'\n\n"
                    "The search returned no results. This could mean:\n"
                    "• No documents match your search terms\n"
                    "• You don't have access to documents containing this information\n"
                    "• Try rephrasing your question or using different keywords"
                )

            # Build context for the local model
            enhanced_context = self._build_context_from_results(
                search_query, chunks, custom_instructions
            )

            # Get user object for generate_chat_completion
            user_obj = Users.get_user_by_id(__user__["id"])

            # Prepare the request for the local model
            enhanced_body = body.copy()
            enhanced_body["model"] = self.valves.LOCAL_MODEL

            # Replace the user's message with the enhanced context
            enhanced_messages = messages[:-1] + [
                {"role": "user", "content": enhanced_context}
            ]
            enhanced_body["messages"] = enhanced_messages

            # Call the local model using OpenWebUI's internal function
            return await generate_chat_completion(__request__, enhanced_body, user_obj)

        except Exception as e:
            error_msg = f"❌ **System Error**: {str(e)}"
            print(error_msg)
            return error_msg
