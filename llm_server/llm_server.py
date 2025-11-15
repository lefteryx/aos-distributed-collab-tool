import asyncio
import grpc
import re
import collab_pb2, collab_pb2_grpc
import logging

from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("llm_server")

MODEL_ID = "google/flan-t5-small"
MAX_NEW_TOKENS = 128
TIMEOUT_SEC = 20

log.info(f"LLM server starting with model {MODEL_ID}...")

gen = None
is_seq2seq = False

def try_load_text2text(model_id):
    try:
        tok = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
        model = AutoModelForSeq2SeqLM.from_pretrained(model_id, trust_remote_code=True, device_map="cpu")
        pipe = pipeline("text2text-generation", model=model, tokenizer=tok)
        return pipe
    except Exception as e:
        log.error(f"text2text load failed for {model_id}: {e}")
        return None

gen = try_load_text2text(MODEL_ID)
if gen:
    is_seq2seq = True
    log.info(f"Loaded seq2seq model: {MODEL_ID}")
else:
    log.error(f"Failed to load model {MODEL_ID}. The server will run but return mock responses.")

EDIT_BY_RE = re.compile(r"Edit by [^\n]* at \d{6,}", re.IGNORECASE)
EPOCH_NUM_RE = re.compile(r"\b\d{9,}\b")

def sanitize_input_text(text: str) -> str:
    if not text:
        return text
    cleaned = EDIT_BY_RE.sub("", text)
    cleaned = EPOCH_NUM_RE.sub("", cleaned)
    cleaned = "\n".join([ln.strip() for ln in cleaned.splitlines() if ln.strip() != ""])
    return cleaned.strip()

def build_prompt(mode: str, text: str) -> str:
    text = sanitize_input_text(text)
    if mode == "grammar":
        return f"Correct and polish the grammar of the following text. Also correct spelling mistakes. Do NOT add timestamps or author names. Keep meaning unchanged.\n\nText:\n{text}\n\nCorrected:"
    if mode == "summarize":
        return f"Summarize the following text into 3 concise bullet points. Do NOT invent timestamps or author names.\n\n{text}\n\nSummary:"
    if mode == "rewrite":
        return f"Rewrite the following text to be more formal and coherent. Do NOT add timestamps or author names. Preserve the meaning. ADD the word 'EDITED' to the end after you make the changes.\n\n{text}\n\nRewritten:"
    if mode == "qa":
        return text
    return f"Rewrite the following text:\n\n{text}\n\nRewritten:"

class LLMServicer(collab_pb2_grpc.LLMServiceServicer):
    async def GetLLMAnswer(self, request, context):
        query = request.query or request.context or ""
        mode = (request.mode or "rewrite").lower()

        def blocking_infer(prompt, mode):
            if gen is None:
                return f"[Mock-{mode}] {prompt[:400]}"

            try:
                gen_kwargs = {
                    "max_new_tokens": MAX_NEW_TOKENS,
                    "do_sample": False,
                    "num_beams": 4,
                    "early_stopping": True,
                    "no_repeat_ngram_size": 3,
                }
                out = gen(prompt, **gen_kwargs)
                text = out[0].get("generated_text", "")
                lines = text.splitlines()
                cleaned = []
                prev = None
                for ln in lines:
                    ln_stripped = ln.strip()
                    if ln_stripped == prev:
                        continue
                    cleaned.append(ln.rstrip())
                    prev = ln_stripped
                text = "\n".join(cleaned).strip()
                if len(text) > 2000:
                    text = text[:2000] + "..."
                return text
            except Exception as e:
                return f"[LLM inference error: {e}]"

        prompt = build_prompt(mode, query)
        loop = asyncio.get_running_loop()
        try:
            answer = await loop.run_in_executor(None, blocking_infer, prompt, mode)
        except Exception as e:
            answer = f"[LLM error: {e}]"
        return collab_pb2.LLMResponse(request_id=request.request_id, answer=answer)

async def serve():
    server = grpc.aio.server()
    collab_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(), server)
    server.add_insecure_port("[::]:50061")
    log.info("LLM server listening at 0.0.0.0:50061")
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())

