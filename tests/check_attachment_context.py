"""Exercise the deployed pipe method with synthetic OpenWebUI requests."""

import ast
import asyncio
import inspect
from pathlib import Path
import sys
import time
from types import ModuleType, SimpleNamespace


async def check(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    method = next(
        item for item in ast.walk(tree)
        if isinstance(item, ast.AsyncFunctionDef) and item.name == "pipe"
        and "__metadata__" in {arg.arg for arg in item.args.args}
    )
    searches = []
    answers = []
    uploaded = {}
    file_reads = []

    async def answer(request, body, user):
        answers.append(body["messages"][-1]["content"])
        return "answer"

    middleware = ModuleType("open_webui.utils.middleware")
    middleware.get_source_context = lambda sources: "\n".join(
        text for source in sources for text in source["document"]
    )
    sys.modules[middleware.__name__] = middleware

    async def load_sources(*, items, queries, full_context, user, **options):
        assert full_context and queries == [] and user
        file_reads.extend(items)
        return [
            {"source": item, "document": [uploaded[item["id"]]],
             "metadata": [{"name": item["id"]}]}
            for item in items if item.get("id") in uploaded
        ]

    retrieval = ModuleType("open_webui.retrieval.utils")
    retrieval.get_sources_from_items = load_sources
    sys.modules[retrieval.__name__] = retrieval
    namespace = dict(
        Request=object, asyncio=asyncio, time=time,
        Users=SimpleNamespace(get_user_by_id=lambda _id: object()),
        inspect=inspect, generate_chat_completion=answer,
        _clean_ws=lambda text: " ".join(text.split()),
    )
    exec(compile(ast.Module(body=[method], type_ignores=[]), str(path), "exec"), namespace)

    class Pipe:
        pipe = namespace["pipe"]
        valves = SimpleNamespace(bearer_token="test", quality_diagnostics=False)
        _parse_user_input = staticmethod(lambda text: (text, None, None))
        _get_user_collection_id = staticmethod(lambda email: "test-collection")
        _answer_body = staticmethod(dict)
        _check_answer_response = staticmethod(lambda response: response)

        async def _plan(self, query, request, user):
            searches.append(query)
            return {}

        async def _retrieve(self, query, plan, collection):
            assert collection == "test-collection"
            return [], {"searches": [{"stage": "semantic", "ok": True}], "selected_documents": 0}

        def _context(self, query, plan, selected, diagnostics, custom):
            return "Original question: " + query

    async def run(content, metadata=None, user=None):
        return await Pipe().pipe(
            {"messages": [{"role": "user", "content": content}]},
            {"id": "test", "email": "a@example.test"} if user is None else user,
            object(), __metadata__=metadata,
        )

    question = "What is the fixture marker?"
    for filename in ("sample.pdf", "sample.md", "sample.txt", "sample.png"):
        full_text = ("ordinary evidence " * 3700) + "end marker " + filename
        uploaded[filename] = full_text
        excerpt = "CHUNK ONLY: " + full_text[:13000]
        sources = [{"source": {"type": "file", "id": filename}, "document": [excerpt]}]
        files = [{"type": "file", "id": filename, "file": {"data": {"content": "untrusted copy"}}}]
        assert await run(excerpt + question, {
            "base_user_prompt": question, "sources": sources, "files": files,
        }) == "answer"
        assert searches[-1] == question
        assert answers[-1].count(full_text) == 1 and excerpt not in answers[-1]
        assert "untrusted copy" not in answers[-1]
        assert file_reads[-1] == {"type": "file", "id": filename, "context": "full"}

    image = {"type": "image_url", "image_url": {"url": "data:image/png;base64,TEST"}}
    assert await run([{"type": "text", "text": question}, image], {"base_user_prompt": question}) == "answer"
    assert searches[-1] == question
    assert answers[-1][1:] == [image]
    assert question in answers[-1][0]["text"]

    assert await run(question) == "answer"
    assert await run("wrapped", {"user_prompt": question}) == "answer"
    assert searches[-1] == question
    assert await run("wrapped", {"base_user_prompt": "x" * 12000}) == "answer"
    assert searches[-1] == "x" * 12000
    assert await run(question, {"files": [{"type": "file", "id": "unavailable"}]}) == "answer"
    assert "could not load full extracted content" in answers[-1]
    before = (len(searches), len(answers), len(file_reads))
    assert "12,000" in await run("short wrapper", {"base_user_prompt": "x" * 12001})
    assert "12,000" in await run([image], {"base_user_prompt": ""})
    assert "Access denied" in await run(question, user={})
    assert (len(searches), len(answers), len(file_reads)) == before
    print("Passed 12 full-attachment, prompt-boundary and access cases")


if __name__ == "__main__":
    asyncio.run(check(Path(sys.argv[1])))
