"""
title: Nextcloud R2R Search → LLM (Valve Version)
author: GAGPT
version: 1.0
license: MIT
"""
import json
import requests
import base64
import uuid
import re
from typing import Optional, Tuple
from pydantic import BaseModel, Field

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
    
    # Priority order as specified: title | source | filename
    for key in ("title", "source", "filename"):
        value = meta.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    
    # Additional fallbacks
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
    
    # Pattern: files__default:XXXXXX
    if "files__default:" in filename:
        try:
            file_id = filename.split("files__default:")[-1].strip()
            if file_id.isdigit():
                return file_id
        except:
            pass
    
    # Additional patterns if your system uses different formats
    # Pattern: files_XXXXXX
    if filename.startswith("files_") and filename[6:].isdigit():
        return filename[6:]
    
    # Pattern: direct numeric ID
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
        
        # Create LDAP server connection
        server = Server(ldap_config["server_uri"], get_info=ALL)
        
        # Bind with service account
        conn = Connection(
            server,
            user=ldap_config["bind_user"],
            password=ldap_config["bind_password"],
            auto_bind=True,
            receive_timeout=ldap_config.get("timeout", 10),
        )
        
        # Search for user by email
        search_filter = ldap_config["user_filter"].format(email=email)
        conn.search(
            search_base=ldap_config["search_base"],
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=[ldap_config["guid_attribute"]],
        )
        
        if not conn.entries:
            return None
            
        # Get the GUID from the first matching entry
        entry = conn.entries[0]
        guid_attr = ldap_config["guid_attribute"]
        if hasattr(entry, guid_attr):
            guid_bytes = getattr(entry, guid_attr).value
            if isinstance(guid_bytes, bytes):
                # Convert bytes to UUID string
                guid_uuid = uuid.UUID(bytes=guid_bytes)
                return str(guid_uuid).upper()
                
        return None
        
    except Exception as e:
        print(f"LDAP lookup failed for {email}: {e}")
        return None
    finally:
        try:
            if "conn" in locals():
                conn.unbind()
        except:
            pass


def _get_collection_id_from_guid(user_guid: str, r2r_config: dict) -> Optional[str]:
    """Get R2R collection ID from user GUID by calling collections API."""
    if not user_guid:
        return None
    
    try:
        # Build collection lookup URL
        url = f"{r2r_config['collections_api_url']}/{user_guid}"
        params = {"owner_id": r2r_config["default_owner_id"]}
        headers = {"Authorization": f"Bearer {r2r_config['bearer_token']}"}
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            collection_id = data.get("results", {}).get("id")
            return collection_id
        return None
    except Exception as e:
        print(f"Collection lookup failed for GUID {user_guid}: {e}")
        return None


def _format_metadata_info(meta: dict) -> list:
    """Extract and format useful metadata information."""
    if not isinstance(meta, dict):
        return []
    
    info_parts = []
    
    # Document/file information
    for key in ["document_id", "document_type", "filename", "file_name"]:
        if key in meta and meta[key]:
            info_parts.append(f"{key}: {meta[key]}")
    
    # Location information
    for key in ["chunk_index", "page", "page_number", "section", "chapter"]:
        if key in meta and meta[key] is not None:
            info_parts.append(f"{key}: {meta[key]}")
    
    # Date information
    for key in ["created_at", "updated_at", "date", "year"]:
        if key in meta and meta[key]:
            info_parts.append(f"{key}: {meta[key]}")
    
    # Size information
    for key in ["size_in_bytes", "total_tokens"]:
        if key in meta and meta[key] is not None:
            info_parts.append(f"{key}: {meta[key]}")
    
    return info_parts


class Tools:
    class Valves(BaseModel):
        # === Connection / Auth ===
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
            description="LDAP bind username (e.g., service-account@domain.com or CN=user,OU=Users,DC=domain,DC=com).",
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
        
        # === LLM Instructions ===
        system_prompt: str = Field(
            default=(
                "## CRITICAL CITATION RULES - MUST FOLLOW EXACTLY:\n"
                "You MUST cite using the exact Citation ID shown for each result, NOT numbers.\n"
                "WRONG: [1], [2], [3], [4] - NEVER use numbers in citations\n"
                "CORRECT: Use the exact Citation ID after 'Citation ID:' in each result\n\n"
                "## HYPERLINK GENERATION:\n"
                "After each citation, provide a hyperlink to the Nextcloud document:\n"
                "1. Find the 'filename' field in the metadata (e.g., 'filename: files__default:8060008')\n"
                "2. Extract the number after 'files__default:' (e.g., '8060008')\n"
                "3. Create link as: [https://your-nextcloud.domain.com/f/NUMBER]\n"
                "Example: If metadata shows 'filename: files__default:8060008', create link [https://your-nextcloud.domain.com/f/8060008]\n\n"
                "## CITATION FORMAT:\n"
                "Each citation should be: [Citation_ID][https://your-nextcloud.domain.com/f/FILE_ID]\n"
                "Example: [Document Title][https://your-nextcloud.domain.com/f/8060008]\n\n"
                "## Task: Answer the query strictly using the provided Context. If the Context does not support an answer, say you dont know.\n"
                "## Grounding: Use only the provided Context. If the Context does not support an answer, reply exactly: I don't know.\n"
                "## Citations: After every sentence that uses the Context, append citations with hyperlinks as shown above.\n"
                "## Formatting: Begin directly with the answer text. No preface, no 'Response:', no lists unless the query demands it. Keep prose concise and factual."
            ),
            description="System prompt with citation and hyperlink generation requirements for Nextcloud integration.",
        )
        
        # === Technical Settings ===
        request_timeout: int = Field(
            default=30, description="HTTP request timeout in seconds.", ge=5, le=300
        )
        include_metadata: bool = Field(
            default=True,
            description="Include metadata information in context for better citations.",
        )
        enable_source_emission: bool = Field(
            default=True,
            description="Enable emission of sources to OpenWebUI frontend for display in sources panel.",
        )

    def __init__(self):
        """Initialize the R2R tool."""
        self.valves = self.Valves()
        
        # Force tool registration
        self._tool_name = "r2r_search_context"
        self._tool_description = "Search R2R knowledge base"
        
        # Add debugging
        print(f"R2R Tool initialized: {self._tool_name}")
        
        # Validate critical configuration
        if not self.valves.bearer_token:
            print("WARNING: No bearer token configured")
        if not self.valves.api_url:
            print("WARNING: No API URL configured")

    def _parse_user_input(self, input_text: str) -> Tuple[str, Optional[str]]:
        """
        Parse user input to extract search query and optional LLM instructions.
        
        Supports structured format:
        ---
        search: "your search query here"
        instructions: "additional instructions for the LLM"
        ---
        
        Or simple format where entire input is treated as search query.
        
        Returns:
            tuple: (search_query, llm_instructions)
        """
        input_text = (input_text or "").strip()
        if not input_text:
            return "", None
            
        # Check for YAML-style structured input
        if input_text.startswith("---") and "---" in input_text[3:]:
            try:
                # Extract YAML block
                yaml_end = input_text.find("---", 3)
                yaml_content = input_text[3:yaml_end].strip()
                
                # Simple YAML parsing for our specific fields
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
                    
            except Exception as e:
                print(f"Failed to parse structured input: {e}")
                # Fall through to treat as simple query
        
        # Check for simple delimiter format
        if " | " in input_text:
            parts = input_text.split(" | ", 1)
            search_query = parts[0].strip()
            llm_instructions = parts[1].strip() if len(parts) > 1 else None
            return search_query, llm_instructions
        
        # Check for natural language patterns
        return self._parse_user_input_natural(input_text)

    def _parse_user_input_natural(self, input_text: str) -> Tuple[str, Optional[str]]:
        """Parse natural language input with patterns."""
        input_text = (input_text or "").strip()
        if not input_text:
            return "", None
        
        # Pattern 1: "Query: ... Instructions: ..."
        if "Query:" in input_text and "Instructions:" in input_text:
            query_start = input_text.find("Query:") + 6
            instructions_start = input_text.find("Instructions:")
            
            search_query = input_text[query_start:instructions_start].strip()
            llm_instructions = input_text[instructions_start + 12:].strip()
            return search_query, llm_instructions
        
        # Pattern 2: "Search for X. Please Y" or "Find X. Format as Y"
        search_patterns = [
            r"(?:Search for|Find|Look for)\s+(.+?)\.\s*(?:Please|Format|Present|Show)\s+(.+)",
            r"(.+?)\.\s*(?:Please|Format|Present|Show|Make sure to)\s+(.+)",
        ]
        
        for pattern in search_patterns:
            match = re.search(pattern, input_text, re.IGNORECASE | re.DOTALL)
            if match:
                search_query = match.group(1).strip()
                llm_instructions = match.group(2).strip()
                return search_query, llm_instructions
        
        # Default: entire input as search query
        return input_text, None

    def _do_ldap_lookup(self, user_email: str) -> Optional[str]:
        """Perform LDAP lookup and return user GUID."""
        try:
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
            print(f"Debug: LDAP lookup result for {user_email}: {user_guid}")
            return user_guid
            
        except Exception as e:
            print(f"Debug: LDAP lookup failed: {e}")
            return None

    def _get_collection_from_guid(self, user_guid: str) -> Optional[str]:
        """Get R2R collection ID from user GUID."""
        try:
            r2r_config = {
                "collections_api_url": self.valves.collections_api_url,
                "bearer_token": self.valves.bearer_token.strip(),
                "default_owner_id": self.valves.default_owner_id,
            }
            
            collection_id = _get_collection_id_from_guid(user_guid, r2r_config)
            print(f"Debug: Collection ID lookup result: {collection_id}")
            return collection_id
            
        except Exception as e:
            print(f"Debug: Collection lookup failed: {e}")
            return None

    def _get_user_email_alternative(self) -> Optional[str]:
        """Alternative methods to get user email when standard parameter fails."""
        try:
            # Method 1: Check for OpenWebUI session context
            import inspect
            frame = inspect.currentframe()
            try:
                # Look through multiple frames for user context
                current_frame = frame
                for _ in range(5):  # Check up to 5 frames up
                    if current_frame and current_frame.f_locals:
                        locals_dict = current_frame.f_locals
                        
                        # Check for various user-related variables
                        for key in [
                            "__user__",
                            "user_data",
                            "session_user",
                            "current_user",
                        ]:
                            if key in locals_dict:
                                user_data = locals_dict[key]
                                if isinstance(user_data, dict):
                                    email = user_data.get("email") or user_data.get("mail")
                                    if email and "@" in str(email):
                                        return str(email).strip().lower()
                    
                    current_frame = current_frame.f_back
                    if not current_frame:
                        break
            finally:
                del frame
            
            # Method 2: Check environment variables (if OpenWebUI sets them)
            import os
            env_email = os.environ.get("OPENWEBUI_USER_EMAIL") or os.environ.get("WEBUI_USER")
            if env_email and "@" in env_email:
                return env_email.strip().lower()
            
            return None
            
        except Exception as e:
            print(f"Alternative user detection failed: {e}")
            return None

    def health_check(self) -> str:
        """Simple health check for the R2R tool."""
        try:
            # Test basic functionality
            if not self.valves.bearer_token.strip():
                return "❌ No bearer token configured"
            
            if not self.valves.api_url.strip():
                return "❌ No API URL configured"
            
            # Test a simple connection (optional)
            import requests
            test_url = self.valves.api_url.replace('/search', '/health')
            try:
                response = requests.get(test_url, timeout=5)
                api_status = f"API: {response.status_code}"
            except:
                api_status = "API: Cannot connect"
            
            return f"✅ R2R Tool Ready | {api_status} | Token: {'✓' if self.valves.bearer_token else '✗'}"
            
        except Exception as e:
            return f"❌ Health check failed: {str(e)}"

    def r2r_search_context(
        self,
        query: str = Field(
            ...,
            description="Search query or 'search terms | additional instructions'",
        ),
        user: str = Field(
            default="",
            description="User context (automatically provided)",
        ),
    ) -> str:
        """Search your private R2R knowledge base and return formatted context for the LLM."""
        
        # Add validation logging
        print(f"Tool called with query: '{query}', user: '{user}'")
        
        try:
            # Validate inputs immediately
            if not query or not isinstance(query, str):
                return "Error: Invalid or empty query parameter"
                
            # Parse the input to extract search query and optional LLM instructions
            search_query, custom_instructions = self._parse_user_input(query)
            
            if not search_query or len(search_query.strip()) < 3:
                return f"Error: Search query too short: '{search_query}'"
            
            print(f"Proceeding with search: '{search_query}'")
            if custom_instructions:
                print(f"Debug: Custom LLM instructions: '{custom_instructions}'")

            # Authentication check
            if not self.valves.bearer_token.strip():
                return "Error: Missing authentication token. Please configure the 'bearer_token' in tool settings."

            # User permission filtering
            user_collection_id = None
            if self.valves.enforce_permissions:
                try:
                    # Parse user parameter (comes as JSON string from OpenWebUI)
                    user_obj = None
                    if user:
                        try:
                            user_obj = json.loads(user) if isinstance(user, str) else user
                        except json.JSONDecodeError:
                            print(f"Debug: Failed to parse user JSON: {user}")
                            user_obj = {}
                    
                    print(f"Debug: Parsed user object: {user_obj}")
                    
                    # Extract user email from parsed object
                    user_email = None
                    if user_obj and isinstance(user_obj, dict):
                        user_email = (
                            user_obj.get("email")
                            or user_obj.get("mail")
                            or user_obj.get("username")
                        )
                    
                    # If still no email, try alternative approaches
                    if not user_email:
                        user_email = self._get_user_email_alternative()
                    
                    if not user_email:
                        print("Debug: No user email found, disabling permission filtering")
                        user_collection_id = None
                    else:
                        print(f"Debug: Found user email: {user_email}")
                        # Continue with LDAP lookup...
                        user_guid = self._do_ldap_lookup(user_email)
                        if user_guid:
                            user_collection_id = self._get_collection_from_guid(user_guid)
                            
                except Exception as e:
                    print(f"Debug: Permission lookup exception: {e}")
                    user_collection_id = None

            # Prepare search payload
            search_payload = {
                "query": search_query,  # Use parsed query, not original input
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

            # Prepare headers
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.valves.bearer_token.strip()}",
                "User-Agent": "OpenWebUI-R2R-Tool/1.0",
            }

            # Execute search request
            try:
                print(f"Sending request to: {self.valves.api_url}")
                response = requests.post(
                    self.valves.api_url.rstrip("/"),
                    headers=headers,
                    json=search_payload,
                    timeout=min(self.valves.request_timeout, 25),  # Cap at 25s for OpenWebUI
                )
                print(f"Response status: {response.status_code}")
                
            except requests.exceptions.Timeout:
                return "Error: Request timed out. Try a simpler query."
            except requests.exceptions.ConnectionError:
                return f"Error: Could not connect to R2R at {self.valves.api_url}"
            except requests.exceptions.RequestException as e:
                return f"Error: Request failed - {str(e)}"

            # Handle HTTP errors
            if response.status_code == 401:
                return "Error: Authentication failed. Check your bearer token."
            elif response.status_code == 403:
                return "Error: Access forbidden. Check your permissions."
            elif response.status_code == 404:
                return "Error: R2R endpoint not found. Check the API URL."
            elif response.status_code >= 400:
                try:
                    error_detail = response.json()
                    return f"Error: R2R API returned {response.status_code}: {json.dumps(error_detail)}"
                except:
                    return f"Error: R2R API returned {response.status_code}: {response.text[:500]}"

            # Parse response
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                return f"Error: Invalid JSON response from R2R: {response.text[:500]}"

            # Extract search results
            results = response_data.get("results", {})
            if isinstance(results, dict):
                chunks = results.get("chunk_search_results", [])
            else:
                chunks = results if isinstance(results, list) else []

            if not chunks:
                return (
                    f"No relevant documents found for query: '{search_query}'\n\n"
                    "The search returned no results. Try rephrasing your question or using different keywords."
                )

            # Filter and sort chunks
            filtered_chunks = []
            for chunk in chunks:
                score = chunk.get("score", 0.0)
                if score >= self.valves.min_relevance_score:
                    filtered_chunks.append(chunk)

            if not filtered_chunks:
                return (
                    f"No sufficiently relevant documents found for query: '{search_query}'\n\n"
                    f"Found {len(chunks)} results, but none met the minimum relevance threshold of {self.valves.min_relevance_score}."
                )

            # Select top chunks for context
            top_chunks = filtered_chunks[: self.valves.max_chunks_in_context]

            # Format context and collect sources for emission
            context_parts = []
            sources_for_emission = []
            
            context_parts.append("=== SEARCH RESULTS ===")
            context_parts.append(f"Original Query: {query}")
            context_parts.append(f"Search Query Used: {search_query}")
            context_parts.append(
                f"Found {len(filtered_chunks)} relevant results (showing top {len(top_chunks)}):"
            )
            context_parts.append("")

            for idx, chunk in enumerate(top_chunks, 1):
                # Extract chunk information
                text = chunk.get("text", "").strip()
                metadata = chunk.get("metadata", {})
                score = chunk.get("score")
                document_id = chunk.get("document_id", "")

                # Get citation identifier (for LLM to use in citations)
                citation_id = _get_citation_identifier(metadata)

                # Extract Nextcloud file ID and create link
                nextcloud_link = None
                nextcloud_file_id = None

                # Look for filename with pattern 'files__default:XXXXXX'
                filename = metadata.get("filename", "")
                if filename and "files__default:" in filename:
                    try:
                        # Extract the numeric ID after 'files__default:'
                        file_id = filename.split("files__default:")[-1].strip()
                        if file_id.isdigit():
                            nextcloud_file_id = file_id
                            nextcloud_link = f"{self.valves.nextcloud_base_url.rstrip('/')}/f/{file_id}"
                    except:
                        pass

                # Fallback: try other filename patterns if needed
                if not nextcloud_link and citation_id:
                    if ("/" in citation_id or "." in citation_id) and not citation_id.startswith("http"):
                        base = self.valves.nextcloud_base_url.rstrip("/")
                        nextcloud_link = f"{base}/f/{citation_id.lstrip('/')}"

                # Prepare source for OpenWebUI emission
                source_entry = {
                    "name": citation_id,
                    "url": nextcloud_link if nextcloud_link else "",
                    "content": _truncate(text, 300),
                }
                sources_for_emission.append(source_entry)

                # Truncate text for context
                text = _truncate(text, self.valves.max_chars_per_chunk)

                # Format chunk entry - show the citation identifier and file ID clearly
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
                    
                    # Always show filename first if available
                    if filename:
                        meta_info.append(f"filename: {filename}")
                        
                    # Add other metadata
                    other_meta = _format_metadata_info(metadata)
                    if document_id:
                        other_meta.insert(0, f"document_id: {document_id}")
                    
                    meta_info.extend(other_meta[:5])  # Limit total metadata items
                    
                    if meta_info:
                        context_parts.append(f"Metadata: {', '.join(meta_info)}")

                context_parts.append("Content:")
                context_parts.append(text)
                context_parts.append("-" * 50)

            # Build final context with custom instructions
            base_system_prompt = self.valves.system_prompt
            if custom_instructions:
                enhanced_prompt = f"{base_system_prompt}\n\n##ADDITIONAL USER INSTRUCTIONS:\n{custom_instructions}\n"
            else:
                enhanced_prompt = base_system_prompt

            final_context = "\n".join([
                "CRITICAL: You must use the exact Citation ID from each result, NOT numbers like [1], [2], [3].",
                "Look at each result and find the line that says 'Citation ID: ...' - use that exact text in brackets.",
                "",
                enhanced_prompt,
                "",
                f"Context: {chr(10).join(context_parts)}",
                "",
                "REMINDER: Use [Citation_ID] format with the exact Citation ID from each result. Never use numbers.",
                "Instructions: Follow the rules above strictly. Output only the answer text with inline citations using Citation IDs.",
                "Response:",
            ])

            # Emit sources to OpenWebUI frontend
            if sources_for_emission and self.valves.enable_source_emission:
                try:
                    formatted_sources = []
                    for source in sources_for_emission:
                        formatted_sources.append({
                            "name": source["name"],
                            "url": source.get("url", ""),
                            "content": source.get("content", ""),
                        })
                    
                    return f"__SOURCES__: {json.dumps(formatted_sources)}\n\n{final_context}"
                except Exception:
                    return final_context

            return final_context
            
        except Exception as e:
            error_msg = f"Tool execution failed: {str(e)}"
            print(error_msg)
            return error_msg