# 📄 PDF Research Analyzer

A production-ready system for uploading research PDFs, detecting sections, and chatting with documents using semantic search (RAG) and LLMs.

---

## Features

- **PDF Upload** — validates file type, size, and PDF header
- **Text Extraction** — full text via PyMuPDF with page-by-page cleaning
- **Section Detection** — Abstract, Introduction, Methods, Results, Discussion, Conclusion, References
- **Intelligent Chunking** — section-aware sliding window with overlap
- **Embeddings** — sentence-transformers (`all-MiniLM-L6-v2`), runs locally
- **Vector Store** — FAISS per-document index, persisted to disk
- **Semantic Search** — cosine similarity over document chunks
- **Chat with PDF** — streaming RAG-powered chat with conversation history
- **LLM Routing** — OpenRouter (primary) → HuggingFace (automatic fallback)
- **Retry Logic** — exponential backoff on all API calls
- **Structured Logging** — colored console + daily log files

---

## Project Structure

```
pdf_analyzer/
├── app/
│   ├── main.py                  ← Streamlit UI
│   ├── config.py                ← All settings (env vars)
│   ├── models/
│   │   └── schemas.py           ← Pydantic data models
│   ├── services/
│   │   ├── pdf_service.py       ← Upload, storage, state
│   │   ├── extraction_service.py← Text extraction + chunking
│   │   ├── embedding_service.py ← sentence-transformers
│   │   ├── rag_service.py       ← FAISS vector store + search
│   │   ├── analysis_service.py  ← Pipeline orchestrator
│   │   └── ai_router.py         ← OpenRouter + HuggingFace + streaming
│   └── utils/
│       ├── logger.py            ← Structured logging
│       └── retry.py             ← Retry + exponential backoff
├── data/
│   ├── uploads/                 ← Saved PDFs
│   ├── processed/               ← Document state JSON files
│   └── vectorstore/             ← FAISS indexes per document
├── logs/                        ← Daily log files (auto-created)
├── .env                         ← Environment variables
├── requirements.txt
├── run.py                       ← Entry point
└── README.md
```

---

## Setup

### 1. Clone and create a virtual environment

```bash
git clone <your-repo>
cd pdf_analyzer
python -m venv venv

# Activate
source venv/bin/activate        # macOS/Linux
venv\Scripts\activate           # Windows
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

> **Note:** First run downloads the `all-MiniLM-L6-v2` model (~90MB). This is cached locally after the first download.

### 3. Configure environment variables

```bash
cp .env .env.local   # optional — edit .env directly
```

Open `.env` and fill in your API keys:

```env
OPENROUTER_API_KEY=your_openrouter_key_here
HUGGINGFACE_API_KEY=your_huggingface_key_here
```

- **OpenRouter key** → https://openrouter.ai/keys (free tier available)
- **HuggingFace key** → https://huggingface.co/settings/tokens (free)

The app works with just one key — the other is the fallback.

### 4. Run

```bash
python run.py
```

Then open **http://localhost:8501** in your browser.

---

## Usage

1. **Upload** a PDF using the sidebar uploader
2. The system automatically:
   - Extracts text from all pages
   - Detects sections (Abstract, Methods, Results etc.)
   - Chunks text with overlap
   - Generates embeddings (local, no API cost)
   - Builds and saves a FAISS vector index
3. **Chat** — ask questions in the Chat tab, get streaming answers with source context
4. **Sections** — browse detected sections directly
5. **Search** — run semantic search and see matching chunks with similarity scores
6. **Info** — view metadata, section stats, and index details

---

## Configuration Reference

All values are set in `.env`. Key settings:

| Variable | Default | Description |
|---|---|---|
| `OPENROUTER_MODEL` | `mistralai/mistral-7b-instruct` | Primary LLM |
| `HUGGINGFACE_MODEL` | `mistralai/Mistral-7B-Instruct-v0.1` | Fallback LLM |
| `EMBEDDING_MODEL` | `all-MiniLM-L6-v2` | Local embedding model |
| `CHUNK_SIZE` | `500` | Words per chunk |
| `CHUNK_OVERLAP` | `50` | Overlap between chunks |
| `TOP_K_RESULTS` | `5` | Chunks retrieved per query |
| `SIMILARITY_THRESHOLD` | `0.3` | Min similarity score (0–1) |
| `MAX_TOKENS` | `1024` | Max LLM output tokens |
| `MAX_FILE_SIZE_MB` | `50` | Upload size limit |

---

## Switching LLM Models

Edit `OPENROUTER_MODEL` in `.env` to use any model available on OpenRouter:

```env
# Fast and cheap
OPENROUTER_MODEL=mistralai/mistral-7b-instruct

# More capable
OPENROUTER_MODEL=anthropic/claude-3-haiku
OPENROUTER_MODEL=openai/gpt-4o-mini
OPENROUTER_MODEL=google/gemini-flash-1.5
```

---

## Requirements

- Python 3.10+
- 2GB RAM minimum (for embedding model)
- Internet connection (for LLM API calls)
- No GPU required — CPU inference only

---

## Logs

Daily log files are written to `logs/app_YYYY-MM-DD.log`.
Set `LOG_LEVEL=DEBUG` in `.env` for verbose output.