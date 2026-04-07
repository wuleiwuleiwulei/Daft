from __future__ import annotations

import os
import pymupdf
import torch
import ray
from langchain_text_splitters import RecursiveCharacterTextSplitter
import time
import ray

import daft
from daft import col

start = time.perf_counter()
# 需要自行下载模型，并修改路径为自己的模型路径，避免网络影响
EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
# 由于下方模型UDF处gpus设置为0，所以此时该参数用于设置一次起多少个基于CPU的解析任务，由于mock了模型可以设置的大些
NUM_GPU_NODES = 32
# 数据输入路径，需要修改成本地路径
INPUT_PATH = "/home/data/document_benchmark/parquet"
# 输出路径，需要修改成本地路径
OUTPUT_PATH = "/home/data/document_benchmark/document_embedding_results"
MAX_PDF_PAGES = 100
CHUNK_SIZE = 2048
CHUNK_OVERLAP = 200
EMBEDDING_BATCH_SIZE = 10

daft.set_runner_ray()

# Wait for Ray cluster to be ready
@ray.remote
def warmup():
    pass
ray.get([warmup.remote() for _ in range(64)])


def extract_text_from_parsed_pdf(pdf_bytes):
    try:
        doc = pymupdf.Document(stream=pdf_bytes, filetype="pdf")
        if len(doc) > MAX_PDF_PAGES:
            print(f"Skipping PDF because it has {len(doc)} pages")
            return None
        page_texts = [{"text": page.get_text(), "page_number": page.number} for page in doc]
        return page_texts
    except Exception as e:
        print(f"Error extracting text from PDF {e}")
        return None


def chunk(text):
    splitter = RecursiveCharacterTextSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
    chunk_iter = splitter.split_text(text)
    chunks = []
    for chunk_index, text in enumerate(chunk_iter):
        chunks.append(
            {
                "text": text,
                "chunk_id": chunk_index,
            }
        )
    return chunks

# 取消掉GPU设置
@daft.cls(max_concurrency=NUM_GPU_NODES, gpus=0)
class Embedder:
    def __init__(self):
        # 注释掉模型加载，避免下载权重和占用内存
        self.model = None  # 打桩：不需要实际模型
        print("Model stubbed: Inference will be replaced by random vectors.")

    @daft.method.batch(
        return_dtype=daft.DataType.fixed_size_list(daft.DataType.float32(), EMBEDDING_DIM),
        batch_size=EMBEDDING_BATCH_SIZE,
    )
    def __call__(self, text_col):
        # 注释模型处理
        batch_size = len(text_col)
        
        if batch_size == 0:
            return []
        
        import numpy as np
        # 生成形状为 (batch_size, 384) 的随机向量
        mock_embeddings = np.random.rand(batch_size, EMBEDDING_DIM).astype(np.float32)
        
        return mock_embeddings

# 本地 IO，取消掉s3配置，避免报错
daft.set_planning_config(default_io_config=daft.io.IOConfig())

start_time = time.time()
df = daft.read_parquet(INPUT_PATH)
df = df.where(daft.col("file_name").endswith(".pdf"))
df = df.with_column("pdf_bytes", df["uploaded_pdf_path"].download())
df = df.with_column(
    "pages",
    df["pdf_bytes"].apply(
        extract_text_from_parsed_pdf,
        return_dtype=daft.DataType.list(daft.DataType.struct({"text": daft.DataType.string(), "page_number": daft.DataType.int64()})),
    ),
)
df = df.explode("pages")
df = df.with_columns({"page_text": col("pages")["text"], "page_number": col("pages")["page_number"]})
df = df.where(daft.col("page_text").not_null())
df = df.with_column(
    "chunks",
    df["page_text"].apply(chunk, return_dtype=daft.DataType.list(daft.DataType.struct({"text": daft.DataType.string(), "chunk_id": daft.DataType.int64()})),
    ),
)
df = df.explode("chunks")
df = df.with_columns({"chunk": col("chunks")["text"], "chunk_id": col("chunks")["chunk_id"]})
df = df.where(daft.col("chunk").not_null())
df = df.with_column("embedding", Embedder()(df["chunk"]))
df = df.select("uploaded_pdf_path", "page_number", "chunk_id", "chunk", "embedding")
df.write_parquet(OUTPUT_PATH)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
end = time.perf_counter() - start
print(f"ALL PYTHON Time taken: {end} seconds")
